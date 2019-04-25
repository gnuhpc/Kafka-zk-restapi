package org.gnuhpc.bigdata.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import kafka.zk.KafkaZkClient;
import lombok.extern.log4j.Log4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.requests.DescribeLogDirsResponse.LogDirInfo;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.gnuhpc.bigdata.config.KafkaConfig;
import org.gnuhpc.bigdata.config.ZookeeperConfig;
import org.gnuhpc.bigdata.constant.ConsumerType;
import org.gnuhpc.bigdata.constant.GeneralResponseState;
import org.gnuhpc.bigdata.constant.ReassignmentState;
import org.gnuhpc.bigdata.model.AddPartition;
import org.gnuhpc.bigdata.model.BrokerInfo;
import org.gnuhpc.bigdata.model.ClusterInfo;
import org.gnuhpc.bigdata.model.ConsumerGroupDesc;
import org.gnuhpc.bigdata.model.ConsumerGroupMeta;
import org.gnuhpc.bigdata.model.CustomConfigEntry;
import org.gnuhpc.bigdata.model.CustomTopicPartitionInfo;
import org.gnuhpc.bigdata.model.GeneralResponse;
import org.gnuhpc.bigdata.model.HealthCheckResult;
import org.gnuhpc.bigdata.model.MemberDescription;
import org.gnuhpc.bigdata.model.PartitionAssignmentState;
import org.gnuhpc.bigdata.model.ReassignModel;
import org.gnuhpc.bigdata.model.ReassignStatus;
import org.gnuhpc.bigdata.model.ReassignWrapper;
import org.gnuhpc.bigdata.model.Record;
import org.gnuhpc.bigdata.model.TopicBrief;
import org.gnuhpc.bigdata.model.TopicDetail;
import org.gnuhpc.bigdata.model.TopicMeta;
import org.gnuhpc.bigdata.model.TopicPartitionReplicaAssignment;
import org.gnuhpc.bigdata.service.avro.User;
import org.gnuhpc.bigdata.utils.KafkaUtils;
import org.gnuhpc.bigdata.utils.ZookeeperUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;

@FixMethodOrder(MethodSorters.JVM)
@Log4j
public class KafkaAdminServiceTest {

  @Mock
  private static KafkaConfig mockKafkaConfig;
  @Mock
  private static ZookeeperConfig mockZookeeperConfig;
  @Spy
  private ZookeeperUtils mockZookeeperUtils = new ZookeeperUtils();
  @Spy
  private KafkaUtils mockKafkaUtils = new KafkaUtils();

  @InjectMocks
  private KafkaAdminService kafkaAdminServiceUnderTest;

  private static final String TEST_KAFKA_BOOTSTRAP_SERVERS =
      "localhost:19092,localhost:19094,localhost:19096";
  private static final List<Integer> TEST_KAFKA_BOOTSTRAP_SERVERS_ID = Arrays.asList(10, 11, 12);
  private static final int KAFKA_NODES_COUNT = TEST_KAFKA_BOOTSTRAP_SERVERS_ID.size();
  private static final String TEST_ZK = "127.0.0.1:2181";
  private static final int TEST_CONTROLLER_ID = 10;
//  private static final List<String> TEST_KAFKA_LOG_DIRS =
//      Arrays.asList(
//          "/home/xiangli/bigdata/kafka_2.11-1.1.1/kafka-logs,/home/xiangli/bigdata/kafka_2.11-1.1.1/kafka111_2-logs,/home/xiangli/bigdata/kafka_2.11-1.1.1/kafka111_3-logs",
//          "/home/xiangli/bigdata/kafka_2.11-1.1.1/kafka113-logs,/home/xiangli/bigdata/kafka_2.11-1.1.1/kafka113_2-logs,/home/xiangli/bigdata/kafka_2.11-1.1.1/kafka113_3-logs",
//          "/home/xiangli/bigdata/kafka_2.11-1.1.1/kafka115-logs,/home/xiangli/bigdata/kafka_2.11-1.1.1/kafka115_2-logs,/home/xiangli/bigdata/kafka_2.11-1.1.1/kafka115_3-logs");

  private static final List<String> TEST_KAFKA_LOG_DIRS =
      Arrays.asList(
          "/Users/wenqiao/work/bigdata/kafka_2.11-1.1.1/data",
          "/Users/wenqiao/work/bigdata/kafka_2.11-1.1.1_2/data",
          "/Users/wenqiao/work/bigdata/kafka_2.11-1.1.1_3/data");


  private static final String FIRST_TOPIC_TO_TEST = "first";
  private static final String SECOND_TOPIC_TO_TEST = "second";
  private static final String NON_EXIST_TOPIC_TO_TEST = "nontopic";
  private static final String HEALTH_CHECK_TOPIC_TO_TEST = "healthtest";
  private static final int TEST_TOPIC_PARTITION_COUNT = 2;
  private static final String FIRST_CONSUMER_GROUP_TO_TEST = "testConsumerGroup1";
  private static final String SECOND_CONSUMER_GROUP_TO_TEST = "testConsumerGroup2";
  private static final String FIRST_CONSUMER_CLIENT_TO_TEST = "testConsumerClient1";
  private static final String SECOND_CONSUMER_CLIENT_TO_TEST = "testConsumerClient2";

  public static final String GROUP_METADATA_TOPIC_NAME = "__consumer_offsets";
  public static final int GROUP_METADATA_TOPIC_PARTITION_COUNT = 50;

  private Set<String> allTopicsInClusterBeforeTest;
  private static final KafkaZkClient kafkaZkClient =
      KafkaZkClient.apply(
          TEST_ZK,
          false,
          5000,
          5000,
          Integer.MAX_VALUE,
          Time.SYSTEM,
          "kafka.zk.rest",
          "rest");

  private static final RetryPolicy retryPolicy =
      new ExponentialBackoffRetry(1000, 5, 60000);

  private static CuratorFramework curatorClient = null;

  @BeforeClass
  public static void start() {
    RetryPolicy retryPolicy =
        new ExponentialBackoffRetry(5000, 5, 60000);

    // 2.初始化客户端
    curatorClient =
        CuratorFrameworkFactory.builder()
            .connectString(TEST_ZK)
            .sessionTimeoutMs(5000)
            .connectionTimeoutMs(5000)
            .retryPolicy(retryPolicy)
            //                .namespace("kafka-rest")        //命名空间隔离
            .build();
    curatorClient.start();
    try {
      curatorClient.blockUntilConnected(5000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException interruptedException) {
      log.warn("Curator Client connect is interrupted." + interruptedException);
    }
  }

  @Before
  public void setUp() throws InterruptedException {
    initMocks(this);
    when(mockKafkaConfig.getBrokers()).thenReturn(TEST_KAFKA_BOOTSTRAP_SERVERS);
    when(mockKafkaConfig.getHealthCheckTopic()).thenReturn(HEALTH_CHECK_TOPIC_TO_TEST);
    when(mockZookeeperConfig.getUris()).thenReturn(TEST_ZK);

    mockKafkaUtils.setKafkaConfig(mockKafkaConfig);
    mockZookeeperUtils.setZookeeperConfig(mockZookeeperConfig);
    mockZookeeperUtils.setKafkaZkClient(kafkaZkClient);
    mockZookeeperUtils.setCuratorClient(curatorClient);

    clean();
    allTopicsInClusterBeforeTest = kafkaAdminServiceUnderTest.getAllTopics();
  }

  private void clean() throws InterruptedException {
    // Delete test topics
    kafkaAdminServiceUnderTest.deleteTopicList(
        Arrays.asList(FIRST_TOPIC_TO_TEST, SECOND_TOPIC_TO_TEST, HEALTH_CHECK_TOPIC_TO_TEST));

    // Delete test consumers
    GeneralResponse deleteConsumer1Response =
        kafkaAdminServiceUnderTest.deleteConsumerGroup(
            FIRST_CONSUMER_GROUP_TO_TEST, ConsumerType.NEW);
    GeneralResponse deleteConsumer2Response =
        kafkaAdminServiceUnderTest.deleteConsumerGroup(
            SECOND_CONSUMER_GROUP_TO_TEST, ConsumerType.NEW);
  }

  public static HashMap<Integer, Integer> getBrokerIdAndPort() {
    HashMap<Integer, Integer> brokerIdAndPortMap = new HashMap<>();

    String[] brokerInfos = TEST_KAFKA_BOOTSTRAP_SERVERS.split(",");
    for (int i = 0; i < TEST_KAFKA_BOOTSTRAP_SERVERS_ID.size(); i++) {
      int brokerId = TEST_KAFKA_BOOTSTRAP_SERVERS_ID.get(i);
      String[] hostAndPort = brokerInfos[i].split(":");
      int port = Integer.parseInt(hostAndPort[1]);
      brokerIdAndPortMap.put(brokerId, port);
    }

    return brokerIdAndPortMap;
  }

  @After
  public void clearDirtyData() throws InterruptedException {
    clean();

//    Thread.sleep(10000);
  }

  private TopicDetail generateTopicDetail(
      String topicName, int partitionCount, int replicationFactor) {
    return TopicDetail.builder()
        .name(topicName)
        .partitions(partitionCount)
        .factor(replicationFactor)
        .build();
  }

  private TopicDetail generateTopicDetailByReplicaAssignment(
      String topicName, HashMap<Integer, List<Integer>> replicaAssignments) {
    return TopicDetail.builder().name(topicName).replicasAssignments(replicaAssignments).build();
  }

  @Test
  public void testHealthCheckButHealthTopicNonExist() throws InterruptedException {
    // Run the test
    final HealthCheckResult healthCheckResult = kafkaAdminServiceUnderTest.healthCheck();

    // Verify the results
    assertEquals("unknown", healthCheckResult.getStatus());
    assertTrue(healthCheckResult.msg.contains("Non-Exist"));
  }

  @Test
  public void testHealthCheck() {
    // Set up
    // Create health check topic
    createOneTopic(HEALTH_CHECK_TOPIC_TO_TEST, 1, 1);

    // Run the test
    final HealthCheckResult healthCheckResult = kafkaAdminServiceUnderTest.healthCheck();

    // Verify the results
    assertEquals("ok", healthCheckResult.getStatus());
  }

  @Test
  public void testDescribeCluster() {
    // Run the test
    final ClusterInfo clusterInfo = kafkaAdminServiceUnderTest.describeCluster();

    // Verify the results
    assertEquals(KAFKA_NODES_COUNT, clusterInfo.getNodes().size());
  }

  @Test
  public void testGetControllerId() {
    // Setup
    final int expectedControllerId = TEST_CONTROLLER_ID;

    // Run the test
    final Node controller = kafkaAdminServiceUnderTest.getController();

    // Verify the results
    assertEquals(expectedControllerId, controller.id());
  }

  @Test
  public void testListBrokers() {
    // Setup
    final HashMap<Integer, Integer> expectedBrokerIdAndPort = getBrokerIdAndPort();

    // Run the test
    final List<BrokerInfo> brokerInfoList = kafkaAdminServiceUnderTest.listBrokers();

    // Verify the results
    for (BrokerInfo brokerInfo : brokerInfoList) {
      int brokerId = brokerInfo.getId();
      int brokerPort = brokerInfo.getPort();
      assertTrue(expectedBrokerIdAndPort.containsKey(brokerId));
      if (expectedBrokerIdAndPort.containsKey(brokerId)) {
        int expectedBrokerPort = expectedBrokerIdAndPort.get(brokerId);
        assertEquals(expectedBrokerPort, brokerPort);
      }
    }
  }

  @Test
  public void testGetBrokerConf() {
    final HashMap<Integer, Integer> expectedBrokerIdAndPort = getBrokerIdAndPort();

    for (int i = 0; i < TEST_KAFKA_BOOTSTRAP_SERVERS_ID.size(); i++) {
      // Setup
      final int expectedBrokerId = TEST_KAFKA_BOOTSTRAP_SERVERS_ID.get(i);
      final int expectedBrokerPort = expectedBrokerIdAndPort.get(expectedBrokerId);

      // Run the test
      final Collection<CustomConfigEntry> brokerConf =
          kafkaAdminServiceUnderTest.getBrokerConf(expectedBrokerId);

      for (CustomConfigEntry customConfigEntry : brokerConf) {
        // Verify the results
        // Just verify broker.id, listeners, log dirs
        if (customConfigEntry.getName().equals("broker.id")) {
          assertEquals(expectedBrokerId, Integer.parseInt(customConfigEntry.getValue()));
        }
        if (customConfigEntry.getName().equals("listeners")) {
          String listeners = customConfigEntry.getValue();
          int brokerPort = Integer.parseInt(listeners.substring(listeners.lastIndexOf(":") + 1));
          assertEquals(expectedBrokerPort, brokerPort);
        }
        if (customConfigEntry.getName().equals("log.dirs")) {
          String logDirs = customConfigEntry.getValue();
          assertEquals(TEST_KAFKA_LOG_DIRS.get(i), logDirs);
        }
      }
    }
  }

  @Test
  public void testListLogDirsByBroker() {
    // Setup
    final List<Integer> brokerList = TEST_KAFKA_BOOTSTRAP_SERVERS_ID;
    final Map<Integer, List<String>> expectedBrokerLogDirMap = new HashMap<>();

    for (int i = 0; i < brokerList.size(); i++) {
      int brokerId = brokerList.get(i);
      String dirs = TEST_KAFKA_LOG_DIRS.get(i);
      expectedBrokerLogDirMap.put(brokerId, Arrays.asList(dirs.split(",")));
    }

    // Run the test
    final Map<Integer, List<String>> logDirs =
        kafkaAdminServiceUnderTest.listLogDirsByBroker(brokerList);

    // Verify the results
    assertEquals(expectedBrokerLogDirMap, logDirs);
  }

  @Test
  public void testCreateTopic() {
    List<TopicDetail> topicList = new ArrayList<>();
    // Create first topic by topic name, partition count, replication factor
    int partitionCount = 3;
    int replicationFactor = 1;
    final TopicDetail topic = generateTopicDetail(FIRST_TOPIC_TO_TEST, 3, 1);

    // Create second topic by replica assignment
    HashMap<Integer, List<Integer>> replicaAssignments = new HashMap<>();
    replicaAssignments.put(0, TEST_KAFKA_BOOTSTRAP_SERVERS_ID.subList(0, 1));
    TopicDetail topic2 =
        generateTopicDetailByReplicaAssignment(SECOND_TOPIC_TO_TEST, replicaAssignments);

    topicList.add(topic);
    topicList.add(topic2);

    // Run the test
    final Map<String, GeneralResponse> result = kafkaAdminServiceUnderTest.createTopic(topicList);

    // Verify the first topic result
    TopicMeta firstTopicMeta = (TopicMeta) result.get(FIRST_TOPIC_TO_TEST).getData();
    assertEquals(GeneralResponseState.success, result.get(FIRST_TOPIC_TO_TEST).getState());
    assertEquals(FIRST_TOPIC_TO_TEST, firstTopicMeta.getTopicName());
    assertEquals(partitionCount, firstTopicMeta.getPartitionCount());
    assertEquals(replicationFactor, firstTopicMeta.getReplicationFactor());

    // Verify the second topic result
    TopicMeta secondTopicMeta = (TopicMeta) result.get(SECOND_TOPIC_TO_TEST).getData();
    assertEquals(GeneralResponseState.success, result.get(SECOND_TOPIC_TO_TEST).getState());
    assertEquals(SECOND_TOPIC_TO_TEST, secondTopicMeta.getTopicName());
    assertEquals(1, secondTopicMeta.getPartitionCount());
    assertEquals(replicationFactor, secondTopicMeta.getReplicationFactor());
    assertEquals(
        TEST_KAFKA_BOOTSTRAP_SERVERS_ID.get(0).intValue(),
        secondTopicMeta
            .getTopicPartitionInfos()
            .get(0)
            .getTopicPartitionInfo()
            .replicas()
            .get(0)
            .id());

    // Verify topics exist in cluster
    assertTrue(kafkaAdminServiceUnderTest.existTopic(FIRST_TOPIC_TO_TEST));
    assertTrue(kafkaAdminServiceUnderTest.existTopic(SECOND_TOPIC_TO_TEST));

    // Verify all topics in cluster
    Set<String> expectedAllTopics = new HashSet<>();
    expectedAllTopics.addAll(allTopicsInClusterBeforeTest);
    expectedAllTopics.add(FIRST_TOPIC_TO_TEST);
    expectedAllTopics.add(SECOND_TOPIC_TO_TEST);
    assertEquals(expectedAllTopics, kafkaAdminServiceUnderTest.getAllTopics());
  }

  @Test
  public void testCreateTopicAlreadyExist() {
    List<TopicDetail> topicListToCreate = new ArrayList<>();

    int paritionCount = 3;
    int replicationFactor = 1;
    TopicDetail firstTopic =
        generateTopicDetail(FIRST_TOPIC_TO_TEST, paritionCount, replicationFactor);

    // Create first topic
    topicListToCreate.add(firstTopic);
    final Map<String, GeneralResponse> createTopicResult =
        kafkaAdminServiceUnderTest.createTopic(topicListToCreate);

    // Create first topic again
    final Map<String, GeneralResponse> createTopicExistResult =
        kafkaAdminServiceUnderTest.createTopic(topicListToCreate);

    assertEquals(
        GeneralResponseState.success, createTopicResult.get(FIRST_TOPIC_TO_TEST).getState());
    assertEquals(
        GeneralResponseState.failure, createTopicExistResult.get(FIRST_TOPIC_TO_TEST).getState());
    assertTrue(
        createTopicExistResult.get(FIRST_TOPIC_TO_TEST).getMsg().contains("TopicExistsException"));
  }

  @Test
  public void testCreateTopicWithInvalidReplicator() {
    List<TopicDetail> topicListToCreate = new ArrayList<>();

    int paritionCount = 3;
    int replicationFactor = TEST_KAFKA_BOOTSTRAP_SERVERS_ID.size() + 1;

    // Create first topic with replicator that is more than broker count
    TopicDetail firstTopic =
        generateTopicDetail(FIRST_TOPIC_TO_TEST, paritionCount, replicationFactor);

    // Create firsttopic
    topicListToCreate.add(firstTopic);
    final Map<String, GeneralResponse> createTopicResult =
        kafkaAdminServiceUnderTest.createTopic(topicListToCreate);

    assertEquals(
        GeneralResponseState.failure, createTopicResult.get(FIRST_TOPIC_TO_TEST).getState());
    assertTrue(
        createTopicResult
            .get(FIRST_TOPIC_TO_TEST)
            .getMsg()
            .contains("InvalidReplicationFactorException"));
  }

  @Test
  public void testListTopics() {
    // Run the test
    final List<String> allTopicsList = kafkaAdminServiceUnderTest.listTopics();

    assertEquals(allTopicsInClusterBeforeTest.size(), allTopicsList.size());

    Iterator<String> iterator = allTopicsInClusterBeforeTest.iterator();
    while (iterator.hasNext()) {
      String topicName = iterator.next();
      assertTrue(allTopicsList.contains(topicName));
    }
  }

  @Test
  public void testExistTopic() {
    // Run the test
    final boolean internalDefaultTopicExist =
        kafkaAdminServiceUnderTest.existTopic(GROUP_METADATA_TOPIC_NAME);
    final boolean nonExistTopicExist =
        kafkaAdminServiceUnderTest.existTopic(NON_EXIST_TOPIC_TO_TEST);

    //    // Verify the results
    assertTrue(internalDefaultTopicExist);
    assertFalse(nonExistTopicExist);
  }

  @Test
  public void testListTopicBrief() {
    // Run the test
    final List<TopicBrief> topicBriefList = kafkaAdminServiceUnderTest.listTopicBrief();

    // Verify the results
    assertEquals(allTopicsInClusterBeforeTest.size(), topicBriefList.size());

    Iterator<String> iterator = allTopicsInClusterBeforeTest.iterator();
    while (iterator.hasNext()) {
      String topicName = iterator.next();
      boolean exists = false;
      for (TopicBrief topicBrief : topicBriefList) {
        if (topicBrief.getTopic().equals(topicName)) {
          exists = true;
          break;
        }
      }
      assertTrue(exists);
    }
  }

  @Test
  public void testDescribeTopic() {
    // Setup
    final String topicName = GROUP_METADATA_TOPIC_NAME;

    // Run the test
    final TopicMeta topicMeta = kafkaAdminServiceUnderTest.describeTopic(topicName);

    // Verify the results
    assertEquals(true, topicMeta.isInternal());
    assertEquals(GROUP_METADATA_TOPIC_PARTITION_COUNT, topicMeta.getPartitionCount());
    assertEquals(GROUP_METADATA_TOPIC_PARTITION_COUNT, topicMeta.getTopicPartitionInfos().size());
  }

  @Test
  public void testDeleteTopicList() {
    // Setup
    List<String> topicListToDelete = new ArrayList<>();
    // Create firsttopic and secondtopic
    createTwoTopics(1);

    // Run the test
    topicListToDelete.add(FIRST_TOPIC_TO_TEST);
    topicListToDelete.add(SECOND_TOPIC_TO_TEST);
    topicListToDelete.add(NON_EXIST_TOPIC_TO_TEST);
    final Map<String, GeneralResponse> deleteTopicResult =
        kafkaAdminServiceUnderTest.deleteTopicList(topicListToDelete);

    // Verify the results
    assertEquals(3, deleteTopicResult.size());
    assertTrue(deleteTopicResult.containsKey(FIRST_TOPIC_TO_TEST));
    assertTrue(deleteTopicResult.containsKey(SECOND_TOPIC_TO_TEST));
    assertTrue(deleteTopicResult.containsKey(NON_EXIST_TOPIC_TO_TEST));

    GeneralResponse firstTopicDeleteResult = deleteTopicResult.get(FIRST_TOPIC_TO_TEST);
    GeneralResponse secondTopicDeleteResult = deleteTopicResult.get(SECOND_TOPIC_TO_TEST);
    GeneralResponse nonExistTopicDeleteResult = deleteTopicResult.get(NON_EXIST_TOPIC_TO_TEST);

    assertEquals(GeneralResponseState.success, firstTopicDeleteResult.getState());
    assertEquals(GeneralResponseState.success, secondTopicDeleteResult.getState());
    // Delete topic:nonexisttopic failure since it has not been created.
    assertEquals(GeneralResponseState.failure, nonExistTopicDeleteResult.getState());
  }

  @Test
  public void testDeleteInternalTopic() {
    List<String> topicListToDelete = new ArrayList<>();

    topicListToDelete.add(GROUP_METADATA_TOPIC_NAME);

    final Map<String, GeneralResponse> deleteTopicResult =
        kafkaAdminServiceUnderTest.deleteTopicList(topicListToDelete);

    GeneralResponse topicDeleteResult = deleteTopicResult.get(GROUP_METADATA_TOPIC_NAME);

    assertEquals(GeneralResponseState.failure, topicDeleteResult.getState());
  }

  class ConsumerRunnable implements Runnable {

    private final KafkaConsumer consumer;

    public ConsumerRunnable(String groupId, String clientId, List<String> topicList) {
      this.consumer = mockKafkaUtils.createNewConsumerByClientId(groupId, clientId);
      consumer.subscribe(topicList);
    }

    @Override
    public void run() {
      consumer.poll(200);
      consumer.commitSync();
    }

    public void close() {
      consumer.close();
    }
  }

  class ConsumerGroup {

    private List<ConsumerRunnable> consumers;
    private List<Thread> threadList;

    public ConsumerGroup(
        String groupId, int numConsumers, List<String> clientIdList, List<String> topicList) {
      consumers = new ArrayList<>(numConsumers);
      threadList = new ArrayList<>();
      for (int i = 0; i < numConsumers; i++) {
        ConsumerRunnable consumerRunnable =
            new ConsumerRunnable(groupId, clientIdList.get(i), topicList);
        consumers.add(consumerRunnable);
      }
    }

    public void execute() {
      for (ConsumerRunnable consumerRunnable : consumers) {
        Thread thread = new Thread(consumerRunnable);
        threadList.add(thread);
        thread.start();
      }
    }

    public void close() {
      for (int i = 0; i < consumers.size(); i++) {
        try {
          ConsumerRunnable consumerRunnable = consumers.get(i);
          consumerRunnable.close();
        } catch (Exception closeException) {
          threadList.get(i).stop();
        }
      }
    }
  }

  @Test
  public void testListAllConsumerGroupsAndIsNewConsumerGroup() throws InterruptedException {
    // Setup, just test the new consumer groups
    final ConsumerType type = ConsumerType.NEW;

    String consumerGroup = FIRST_CONSUMER_GROUP_TO_TEST;

    createOneTopic();

    final int numConsumers = 1;
    ConsumerGroup group =
        new ConsumerGroup(
            consumerGroup,
            numConsumers,
            Arrays.asList(FIRST_CONSUMER_CLIENT_TO_TEST),
            Arrays.asList(FIRST_TOPIC_TO_TEST));

    group.execute();
    Thread.sleep(1000);

    // Run the test
    final Map<String, Set<String>> consumerGroups =
        kafkaAdminServiceUnderTest.listAllConsumerGroups(type);
    final boolean isNewConsumerGroup = kafkaAdminServiceUnderTest.isNewConsumerGroup(consumerGroup);

    Set<String> newConsumerGroups = consumerGroups.get("new");

    // Verify the results
    assertTrue(newConsumerGroups.contains(consumerGroup));
    assertTrue(isNewConsumerGroup);

    group.close();
  }

  @Test
  public void testListConsumerGroupsByTopic() throws InterruptedException {
    // Setup
    final String topic = FIRST_TOPIC_TO_TEST;
    final ConsumerType type = ConsumerType.NEW;
    String testGroup1 = FIRST_CONSUMER_GROUP_TO_TEST;
    String testGroup2 = SECOND_CONSUMER_GROUP_TO_TEST;

    createOneTopic();
    List<String> subscribedTopicList = Arrays.asList(topic);

    final int numConsumers = 1;
    ConsumerGroup group1 =
        new ConsumerGroup(
            testGroup1,
            numConsumers,
            Arrays.asList(FIRST_CONSUMER_CLIENT_TO_TEST),
            subscribedTopicList);
    ConsumerGroup group2 =
        new ConsumerGroup(
            testGroup2,
            numConsumers,
            Arrays.asList(SECOND_CONSUMER_CLIENT_TO_TEST),
            subscribedTopicList);
    group1.execute();
    group2.execute();
    Thread.sleep(1000);

    // Run the test
    final Map<String, Set<String>> consumerGroupsByTopic =
        kafkaAdminServiceUnderTest.listConsumerGroupsByTopic(topic, type);

    // Verify the results
    Set<String> newConsumerGroupsByTopic = consumerGroupsByTopic.get("new");
    assertTrue(newConsumerGroupsByTopic.contains(testGroup1));
    assertTrue(newConsumerGroupsByTopic.contains(testGroup2));

    group1.close();
    group2.close();
  }

  private void createOneTopic() {
    List<TopicDetail> topicListToCreate = new ArrayList<>();
    TopicDetail firstTopic =
        generateTopicDetail(FIRST_TOPIC_TO_TEST, TEST_TOPIC_PARTITION_COUNT, 1);
    topicListToCreate.add(firstTopic);

    kafkaAdminServiceUnderTest.createTopic(topicListToCreate);
  }

  private void createOneTopic(String topic, int partitionCount, int repcliaFactor) {
    List<TopicDetail> topicListToCreate = new ArrayList<>();
    TopicDetail topicDetail = generateTopicDetail(topic, partitionCount, repcliaFactor);
    topicListToCreate.add(topicDetail);

    kafkaAdminServiceUnderTest.createTopic(topicListToCreate);
  }

  private void createOneTopic(String topic, Map<Integer, List<Integer>> replicaAssignment) {
    List<TopicDetail> topicListToCreate = new ArrayList<>();
    TopicDetail topicDetail =
        TopicDetail.builder().name(topic).replicasAssignments(replicaAssignment).build();
    topicListToCreate.add(topicDetail);

    kafkaAdminServiceUnderTest.createTopic(topicListToCreate);
  }

  private void createTwoTopics(int replicationFactor) {
    List<TopicDetail> topicListToCreate = new ArrayList<>();
    TopicDetail firstTopic =
        generateTopicDetail(FIRST_TOPIC_TO_TEST, TEST_TOPIC_PARTITION_COUNT, replicationFactor);
    TopicDetail secondTopic =
        generateTopicDetail(SECOND_TOPIC_TO_TEST, TEST_TOPIC_PARTITION_COUNT, replicationFactor);
    topicListToCreate.add(firstTopic);
    topicListToCreate.add(secondTopic);

    kafkaAdminServiceUnderTest.createTopic(topicListToCreate);
  }

  @Test
  public void testListTopicsByConsumerGroup() throws InterruptedException {
    // Setup
    final String consumerGroup = FIRST_CONSUMER_GROUP_TO_TEST;
    final ConsumerType type = ConsumerType.NEW;

    // Create two topics
    createTwoTopics(1);

    List<String> subscribedTopicList = Arrays.asList(FIRST_TOPIC_TO_TEST, SECOND_TOPIC_TO_TEST);
    final int numConsumers = 1;
    ConsumerGroup group =
        new ConsumerGroup(
            consumerGroup,
            numConsumers,
            Arrays.asList(FIRST_CONSUMER_CLIENT_TO_TEST),
            subscribedTopicList);

    group.execute();
    Thread.sleep(1000);

    // Run the test
    final Set<String> topicsByConsumerGroup =
        kafkaAdminServiceUnderTest.listTopicsByConsumerGroup(consumerGroup, type);

    // Verify the results
    assertEquals(2, topicsByConsumerGroup.size());
    assertTrue(topicsByConsumerGroup.contains(FIRST_TOPIC_TO_TEST));
    assertTrue(topicsByConsumerGroup.contains(SECOND_TOPIC_TO_TEST));

    group.close();
  }

  @Test
  public void testGetConsumerGroupMeta() throws InterruptedException {
    // Setup
    final String consumerGroup = SECOND_CONSUMER_GROUP_TO_TEST;
    createTwoTopics(1);

    final int numConsumers = 1;
    ConsumerGroup group =
        new ConsumerGroup(
            consumerGroup,
            numConsumers,
            Arrays.asList(FIRST_CONSUMER_CLIENT_TO_TEST),
            Arrays.asList(FIRST_TOPIC_TO_TEST, SECOND_TOPIC_TO_TEST));
    group.execute();
    Thread.sleep(1000);

    // Run the test
    final ConsumerGroupMeta groupMeta =
        kafkaAdminServiceUnderTest.getConsumerGroupMeta(consumerGroup);

    // Verify the results
    assertEquals(consumerGroup, groupMeta.getGroupId());
    List<MemberDescription> members = groupMeta.getMembers();
    assertEquals(numConsumers, members.size());
    String memberClientId1 = members.get(0).getClientId();
    assertTrue(memberClientId1.equals(FIRST_CONSUMER_CLIENT_TO_TEST));

    group.close();
  }

  @Test
  public void testDescribeNewConsumerGroupWithoutFilter() throws InterruptedException {
    // Setup
    final String consumerGroup = FIRST_CONSUMER_GROUP_TO_TEST;
    final boolean filtered = false;

    // Create first topic with 2 partitions and 1 replica
    createOneTopic();

    // Create a group with only one consumer
    ConsumerGroup group =
        new ConsumerGroup(
            consumerGroup,
            1,
            Arrays.asList(FIRST_CONSUMER_CLIENT_TO_TEST),
            Arrays.asList(FIRST_TOPIC_TO_TEST));
    group.execute();
    Thread.sleep(1000);

    // Run the test
    final List<PartitionAssignmentState> partitionAssignments =
        kafkaAdminServiceUnderTest.describeNewConsumerGroup(consumerGroup, filtered, null);

    // Verify the results
    // Assign the consumer with 2 partitions, since there is only one consumer in the group
    assertEquals(2, partitionAssignments.size());
    PartitionAssignmentState assignment1 = partitionAssignments.get(0);
    PartitionAssignmentState assignment2 = partitionAssignments.get(1);
    assertEquals(consumerGroup, assignment1.getGroup());
    assertEquals(FIRST_TOPIC_TO_TEST, assignment1.getTopic());
    assertEquals(FIRST_CONSUMER_CLIENT_TO_TEST, assignment1.getClientId());
    assertTrue(assignment1.getPartition() == 0);
    assertEquals(consumerGroup, assignment2.getGroup());
    assertEquals(FIRST_TOPIC_TO_TEST, assignment2.getTopic());
    assertEquals(FIRST_CONSUMER_CLIENT_TO_TEST, assignment2.getClientId());
    assertTrue(assignment2.getPartition() == 1);

    group.close();
  }

  @Test
  public void testDescribeNewConsumerGroupWithFilterTopic() throws InterruptedException {
    // Setup
    final String consumerGroup = FIRST_CONSUMER_GROUP_TO_TEST;
    final boolean filtered = true;
    final String topic = SECOND_TOPIC_TO_TEST;

    // Create first and second topic
    createTwoTopics(1);

    // Create a group with only one consumer
    ConsumerGroup group =
        new ConsumerGroup(
            consumerGroup,
            1,
            Arrays.asList(FIRST_CONSUMER_CLIENT_TO_TEST),
            Arrays.asList(FIRST_TOPIC_TO_TEST, SECOND_TOPIC_TO_TEST));
    group.execute();
    Thread.sleep(1000);

    // Run the test
    final List<PartitionAssignmentState> partitionAssignmentsFilteredByTopic =
        kafkaAdminServiceUnderTest.describeNewConsumerGroup(consumerGroup, filtered, topic);

    // Verify the results
    assertEquals(2, partitionAssignmentsFilteredByTopic.size());
    PartitionAssignmentState assignment1 = partitionAssignmentsFilteredByTopic.get(0);
    PartitionAssignmentState assignment2 = partitionAssignmentsFilteredByTopic.get(1);
    assertEquals(consumerGroup, assignment1.getGroup());
    assertEquals(topic, assignment1.getTopic());
    assertEquals(FIRST_CONSUMER_CLIENT_TO_TEST, assignment1.getClientId());
    assertTrue(assignment1.getPartition() == 0);
    assertEquals(consumerGroup, assignment2.getGroup());
    assertEquals(topic, assignment2.getTopic());
    assertEquals(FIRST_CONSUMER_CLIENT_TO_TEST, assignment2.getClientId());
    assertTrue(assignment2.getPartition() == 1);

    group.close();
  }

  @Test
  public void testDescribeConsumerGroup() throws InterruptedException {
    // Setup
    final String consumerGroup = SECOND_CONSUMER_GROUP_TO_TEST;
    final ConsumerType type = ConsumerType.NEW;

    // Create first topic with 2 partitions and 1 replica
    createTwoTopics(1);

    // Create a group with only one consumer
    ConsumerGroup group =
        new ConsumerGroup(
            consumerGroup,
            1,
            Arrays.asList(FIRST_CONSUMER_CLIENT_TO_TEST),
            Arrays.asList(FIRST_TOPIC_TO_TEST, SECOND_TOPIC_TO_TEST));
    group.execute();
    Thread.sleep(1000);

    // Run the test
    final Map<String, List<ConsumerGroupDesc>> result =
        kafkaAdminServiceUnderTest.describeConsumerGroup(consumerGroup, type);

    // Verify the results
    assertEquals(2, result.size());
    assertTrue(result.containsKey(FIRST_TOPIC_TO_TEST));
    assertTrue(result.containsKey(SECOND_TOPIC_TO_TEST));
    List<ConsumerGroupDesc> firstTopicConsumersDesc = result.get(FIRST_TOPIC_TO_TEST);
    List<ConsumerGroupDesc> secondTopicConsumersDesc = result.get(SECOND_TOPIC_TO_TEST);
    assertEquals(2, firstTopicConsumersDesc.size());
    assertEquals(consumerGroup, firstTopicConsumersDesc.get(0).getGroupName());
    assertEquals(consumerGroup, firstTopicConsumersDesc.get(1).getGroupName());
    assertEquals(FIRST_CONSUMER_CLIENT_TO_TEST, firstTopicConsumersDesc.get(0).getClientId());
    assertEquals(FIRST_CONSUMER_CLIENT_TO_TEST, firstTopicConsumersDesc.get(1).getClientId());
    assertEquals(0, firstTopicConsumersDesc.get(0).getPartitionId());
    assertEquals(1, firstTopicConsumersDesc.get(1).getPartitionId());

    assertEquals(2, secondTopicConsumersDesc.size());
    assertEquals(consumerGroup, secondTopicConsumersDesc.get(0).getGroupName());
    assertEquals(consumerGroup, secondTopicConsumersDesc.get(1).getGroupName());
    assertEquals(FIRST_CONSUMER_CLIENT_TO_TEST, secondTopicConsumersDesc.get(0).getClientId());
    assertEquals(FIRST_CONSUMER_CLIENT_TO_TEST, secondTopicConsumersDesc.get(1).getClientId());
    assertEquals(0, secondTopicConsumersDesc.get(0).getPartitionId());
    assertEquals(1, secondTopicConsumersDesc.get(1).getPartitionId());

    group.close();
  }

  private boolean isCollectionEqual(Collection collection1, Collection collection2) {
    if (collection1.size() != collection2.size()) {
      return false;
    }

    Iterator iterator = collection2.iterator();
    while (iterator.hasNext()) {
      Object object = iterator.next();
      if (!collection1.contains(object)) {
        return false;
      }
    }

    return true;
  }

  @Test
  public void testDeleteConsumerGroup() throws InterruptedException {
    // Setup
    final String consumerGroup = FIRST_CONSUMER_GROUP_TO_TEST;
    final ConsumerType type = ConsumerType.NEW;

    Set<String> allConsumerGroupsBefore =
        kafkaAdminServiceUnderTest.listAllConsumerGroups(type).get("new");
    // Create first topic with 2 partitions and 1 replica
    createOneTopic();

    // Create a group with only one consumer
    ConsumerGroup group =
        new ConsumerGroup(
            consumerGroup,
            1,
            Arrays.asList(FIRST_CONSUMER_CLIENT_TO_TEST),
            Arrays.asList(FIRST_TOPIC_TO_TEST));
    group.execute();
    Thread.sleep(1000);
    Set<String> allConsumerGroupsAfterAdd =
        kafkaAdminServiceUnderTest.listAllConsumerGroups(type).get("new");
    Set<String> expectedAllConsumerGroupsAfterAdd = new HashSet<>(allConsumerGroupsBefore);
    expectedAllConsumerGroupsAfterAdd.add(consumerGroup);

    group.close();
    // Run the test
    final GeneralResponse deleteResult =
        kafkaAdminServiceUnderTest.deleteConsumerGroup(consumerGroup, type);

    // Verify the results
    assertEquals(expectedAllConsumerGroupsAfterAdd, allConsumerGroupsAfterAdd);
    assertEquals(GeneralResponseState.success, deleteResult.getState());
    assertEquals(consumerGroup, deleteResult.getData());

    Set<String> allConsumerGroupsAfterDel =
        kafkaAdminServiceUnderTest.listAllConsumerGroups(type).get("new");
    assertTrue(isCollectionEqual(allConsumerGroupsBefore, allConsumerGroupsAfterDel));
  }

  @Test
  public void testDeleteConsumerGroupNonEmpty() throws InterruptedException {
    // Setup
    final String consumerGroup = FIRST_CONSUMER_GROUP_TO_TEST;
    final ConsumerType type = ConsumerType.NEW;

    // Create first topic with 2 partitions and 1 replica
    createOneTopic();

    // Create a group with only one consumer
    ConsumerGroup group =
        new ConsumerGroup(
            consumerGroup,
            1,
            Arrays.asList(FIRST_CONSUMER_CLIENT_TO_TEST),
            Arrays.asList(FIRST_TOPIC_TO_TEST));
    group.execute();
    Thread.sleep(1000);

    // Run the test
    final GeneralResponse deleteResult =
        kafkaAdminServiceUnderTest.deleteConsumerGroup(consumerGroup, type);

    // Verify the results
    assertEquals(GeneralResponseState.failure, deleteResult.getState());
    assertTrue(deleteResult.getMsg().contains("GroupNotEmptyException"));

    group.close();
  }

  @Test
  public void testAddPartitionsByAddPartitionCount() {
    // Setup
    final int numPartitionsToAdd = 1;
    final List<AddPartition> addPartitions =
        Arrays.asList(
            AddPartition.builder()
                .topic(FIRST_TOPIC_TO_TEST)
                .numPartitionsAdded(numPartitionsToAdd)
                .build(),
            AddPartition.builder()
                .topic(SECOND_TOPIC_TO_TEST)
                .numPartitionsAdded(numPartitionsToAdd)
                .build());

    // Create first and second topic with 2 partitions and 1 replica
    createTwoTopics(1);
    // Run the test
    final Map<String, GeneralResponse> addPartitionsResult =
        kafkaAdminServiceUnderTest.addPartitions(addPartitions);

    // Verify the results
    GeneralResponse firstTopicResult = addPartitionsResult.get(FIRST_TOPIC_TO_TEST);
    assertEquals(GeneralResponseState.success, firstTopicResult.getState());
    TopicMeta firstTopicMeta = (TopicMeta) firstTopicResult.getData();
    assertEquals(FIRST_TOPIC_TO_TEST, firstTopicMeta.getTopicName());
    assertEquals(
        TEST_TOPIC_PARTITION_COUNT + numPartitionsToAdd, firstTopicMeta.getPartitionCount());

    GeneralResponse secondTopicResult = addPartitionsResult.get(SECOND_TOPIC_TO_TEST);
    assertEquals(GeneralResponseState.success, secondTopicResult.getState());
    TopicMeta secondTopicMeta = (TopicMeta) secondTopicResult.getData();
    assertEquals(SECOND_TOPIC_TO_TEST, secondTopicMeta.getTopicName());
    assertEquals(
        TEST_TOPIC_PARTITION_COUNT + numPartitionsToAdd, secondTopicMeta.getPartitionCount());
  }

  @Test
  public void testAddPartitionsByReplicaAssignment() {
    // Setup
    final int brokerCount = TEST_KAFKA_BOOTSTRAP_SERVERS_ID.size();
    final int replicaFactor = brokerCount - 1;
    List<List<Integer>> replicaAssignment =
        Arrays.asList(
            TEST_KAFKA_BOOTSTRAP_SERVERS_ID.subList(0, brokerCount - 1),
            TEST_KAFKA_BOOTSTRAP_SERVERS_ID.subList(1, brokerCount));
    final int numPartitionsToAdd = 2;
    AddPartition addPartition =
        AddPartition.builder()
            .topic(FIRST_TOPIC_TO_TEST)
            .numPartitionsAdded(numPartitionsToAdd)
            .replicaAssignment(replicaAssignment)
            .build();

    // In my env, replicaAssignment is [[111,113],[113,115]]
    final List<AddPartition> addPartitions = Arrays.asList(addPartition);

    // Create first topic with 2 partitions and 2 replicas
    createOneTopic(FIRST_TOPIC_TO_TEST, TEST_TOPIC_PARTITION_COUNT, replicaFactor);
    // Run the test
    final Map<String, GeneralResponse> addPartitionsResult =
        kafkaAdminServiceUnderTest.addPartitions(addPartitions);

    // Verify the results
    GeneralResponse response = addPartitionsResult.get(FIRST_TOPIC_TO_TEST);
    assertEquals(GeneralResponseState.success, response.getState());

    TopicMeta topicMeta = (TopicMeta) response.getData();
    int partitionCount = topicMeta.getPartitionCount();
    assertEquals(TEST_TOPIC_PARTITION_COUNT + numPartitionsToAdd, partitionCount);

    List<CustomTopicPartitionInfo> topicPartitionInfoList = topicMeta.getTopicPartitionInfos();
    TopicPartitionInfo firstAddedPartition =
        topicPartitionInfoList.get(partitionCount - numPartitionsToAdd).getTopicPartitionInfo();
    assertEquals(replicaFactor, firstAddedPartition.replicas().size());
    for (int i = 0; i < firstAddedPartition.replicas().size(); i++) {
      assertEquals(
          replicaAssignment.get(0).get(i).intValue(), firstAddedPartition.replicas().get(i).id());
    }

    TopicPartitionInfo secondAddedPartition =
        topicPartitionInfoList.get(partitionCount - numPartitionsToAdd + 1).getTopicPartitionInfo();
    assertEquals(replicaFactor, secondAddedPartition.replicas().size());
    for (int i = 0; i < secondAddedPartition.replicas().size(); i++) {
      assertEquals(
          replicaAssignment.get(1).get(i).intValue(), secondAddedPartition.replicas().get(i).id());
    }
  }

  private int getMaxBrokerId(List<Integer> brokerIdList) {
    int max = -1;
    for (int id : brokerIdList) {
      if (id > max) {
        max = id;
      }
    }

    return max;
  }

  @Test
  public void testAddPartitionsByReplicaAssignmentWithInvalidBrokerId() {
    // Setup
    final int replicaFactor = 1;
    // In my env, kafka broker id list is [111, 113, 115]. Invalid broker id is 116.
    final int invalidBrokerId = getMaxBrokerId(TEST_KAFKA_BOOTSTRAP_SERVERS_ID) + 1;
    List<List<Integer>> replicaAssignment = Arrays.asList(Arrays.asList(invalidBrokerId));
    final int numPartitionsToAdd = 1;
    AddPartition addPartition =
        AddPartition.builder()
            .topic(FIRST_TOPIC_TO_TEST)
            .numPartitionsAdded(numPartitionsToAdd)
            .replicaAssignment(replicaAssignment)
            .build();

    final List<AddPartition> addPartitions = Arrays.asList(addPartition);

    // Create first topic with 2 partitions and 1 replica
    createOneTopic(FIRST_TOPIC_TO_TEST, TEST_TOPIC_PARTITION_COUNT, replicaFactor);
    // Run the test
    final Map<String, GeneralResponse> addPartitionsResult =
        kafkaAdminServiceUnderTest.addPartitions(addPartitions);

    // Verify the results
    GeneralResponse response = addPartitionsResult.get(FIRST_TOPIC_TO_TEST);
    assertEquals(GeneralResponseState.failure, response.getState());
    assertTrue(response.getMsg().contains("InvalidReplicaAssignmentException: Unknown broker"));
  }

  @Test
  public void testAddPartitionsByReplicaAssignmentWithInconsistentReplicationFactor() {
    // Setup
    final int brokerCount = TEST_KAFKA_BOOTSTRAP_SERVERS_ID.size();
    final int replicaFactor = brokerCount - 1;
    // In my env, replicaAssignment is [[111,113],[115]]. And it is invalid.
    List<List<Integer>> replicaAssignment =
        Arrays.asList(
            TEST_KAFKA_BOOTSTRAP_SERVERS_ID.subList(0, brokerCount - 1),
            TEST_KAFKA_BOOTSTRAP_SERVERS_ID.subList(2, brokerCount));
    final int numPartitionsToAdd = 2;
    AddPartition addPartition =
        AddPartition.builder()
            .topic(FIRST_TOPIC_TO_TEST)
            .numPartitionsAdded(numPartitionsToAdd)
            .replicaAssignment(replicaAssignment)
            .build();

    final List<AddPartition> addPartitions = Arrays.asList(addPartition);

    // Create first topic with 2 partitions and 2 replicas
    createOneTopic(FIRST_TOPIC_TO_TEST, TEST_TOPIC_PARTITION_COUNT, replicaFactor);
    // Run the test
    final Map<String, GeneralResponse> addPartitionsResult =
        kafkaAdminServiceUnderTest.addPartitions(addPartitions);

    // Verify the results
    GeneralResponse response = addPartitionsResult.get(FIRST_TOPIC_TO_TEST);
    assertEquals(GeneralResponseState.failure, response.getState());
    assertTrue(
        response
            .getMsg()
            .contains(
                "InvalidReplicaAssignmentException: Inconsistent replication factor between partitions"));
  }

  @Test
  public void testAddPartitionsByReplicaAssignmentWithInconsistentPartitionCount() {
    // Setup
    final int brokerCount = TEST_KAFKA_BOOTSTRAP_SERVERS_ID.size();
    final int replicaFactor = 1;
    // In my env, replicaAssignment is [[111,113],[113,115]]. But numPartitionsToAdd is 3, so it is
    // invalid.
    List<List<Integer>> replicaAssignment =
        Arrays.asList(
            TEST_KAFKA_BOOTSTRAP_SERVERS_ID.subList(0, brokerCount - 1),
            TEST_KAFKA_BOOTSTRAP_SERVERS_ID.subList(1, brokerCount));
    final int numPartitionsToAdd = 3;
    AddPartition addPartition =
        AddPartition.builder()
            .topic(FIRST_TOPIC_TO_TEST)
            .numPartitionsAdded(numPartitionsToAdd)
            .replicaAssignment(replicaAssignment)
            .build();

    final List<AddPartition> addPartitions = Arrays.asList(addPartition);

    // Create first topic with 2 partitions and 2 replicas
    createOneTopic(FIRST_TOPIC_TO_TEST, TEST_TOPIC_PARTITION_COUNT, replicaFactor);
    // Run the test
    final Map<String, GeneralResponse> addPartitionsResult =
        kafkaAdminServiceUnderTest.addPartitions(addPartitions);

    // Verify the results
    GeneralResponse response = addPartitionsResult.get(FIRST_TOPIC_TO_TEST);
    assertEquals(GeneralResponseState.failure, response.getState());
    assertTrue(
        response
            .getMsg()
            .contains(
                "InvalidReplicaAssignmentException: Increasing the number of partitions by "
                    + numPartitionsToAdd
                    + " but "
                    + replicaAssignment.size()
                    + " assignments provided."));
  }

  @Test
  public void testAddPartitionsOnNonExistTopic() {
    // Setup
    final int numPartitionsToAdd = 1;
    final List<AddPartition> addPartitions =
        Arrays.asList(
            AddPartition.builder()
                .topic(NON_EXIST_TOPIC_TO_TEST)
                .numPartitionsAdded(numPartitionsToAdd)
                .build());

    // Run the test
    final Map<String, GeneralResponse> addPartitionsResult =
        kafkaAdminServiceUnderTest.addPartitions(addPartitions);

    // Verify the results
    GeneralResponse response = addPartitionsResult.get(NON_EXIST_TOPIC_TO_TEST);
    assertEquals(GeneralResponseState.failure, response.getState());
    assertEquals("Topic:" + NON_EXIST_TOPIC_TO_TEST + " non-exist.", response.getMsg());
  }

  @Test
  public void testGenerateReassignPartition() {
    // Setup
    // Create first topic with 2 partitions and 1 replica
    String topic = FIRST_TOPIC_TO_TEST;
    createOneTopic();
    TopicMeta topicMeta = kafkaAdminServiceUnderTest.describeTopic(topic);

    final ReassignWrapper reassignWrapper = new ReassignWrapper();
    reassignWrapper.setTopics(Arrays.asList(topic));
    // Reassign on only one broker
    List assignedBrokerList = Arrays.asList(TEST_KAFKA_BOOTSTRAP_SERVERS_ID.get(0));
    reassignWrapper.setBrokers(assignedBrokerList);

    // Run the test
    final List<ReassignModel> reassignResult =
        kafkaAdminServiceUnderTest.generateReassignPartition(reassignWrapper);
    assertEquals(2, reassignResult.size());

    // Verify the results
    // The first element is current topic assignment
    List<TopicPartitionReplicaAssignment> currentTopicPartitionReplicaAssignments =
        reassignResult.get(0).getPartitions();
    List<CustomTopicPartitionInfo> expectedTopicPartitionInfos = topicMeta.getTopicPartitionInfos();
    assertEquals(
        expectedTopicPartitionInfos.size(), currentTopicPartitionReplicaAssignments.size());
    for (int i = 0; i < expectedTopicPartitionInfos.size(); i++) {
      int expectedBrokerId =
          expectedTopicPartitionInfos.get(i).getTopicPartitionInfo().replicas().get(0).id();
      int brokerId = currentTopicPartitionReplicaAssignments.get(i).getReplicas().get(0).intValue();
      assertEquals(expectedBrokerId, brokerId);
    }

    // The second element is proposed topic assignment
    List<TopicPartitionReplicaAssignment> proposedTopicPartitionReplicaAssignments =
        reassignResult.get(1).getPartitions();
    for (TopicPartitionReplicaAssignment tpra : proposedTopicPartitionReplicaAssignments) {
      List<Integer> proposedReplicas = tpra.getReplicas();
      assertTrue(isCollectionEqual(assignedBrokerList, proposedReplicas));
    }
  }

  @Test
  public void testGenerateReassignPartitionWithInvalidReplicator() {
    // Setup
    // Create first topic with 1 partition and 2 replicas
    String topic = FIRST_TOPIC_TO_TEST;
    createOneTopic(topic, 1, 2);

    final ReassignWrapper reassignWrapper = new ReassignWrapper();
    reassignWrapper.setTopics(Arrays.asList(topic));
    // Reassign on only one broker, but replica factor is 2, so it's invalid
    List assignedBrokerList = Arrays.asList(TEST_KAFKA_BOOTSTRAP_SERVERS_ID.get(0));
    reassignWrapper.setBrokers(assignedBrokerList);

    // Run the test
    try {
      kafkaAdminServiceUnderTest.generateReassignPartition(reassignWrapper);
    } catch (ApiException apiException) {
      assertTrue(
          apiException
              .getMessage()
              .contains("InvalidReplicationFactorException: Replication factor"));
    }
  }

  private boolean isReassignComplete(Map<TopicPartition, Integer> partitionsReassignStatus) {
    Iterator<Map.Entry<TopicPartition, Integer>> iterator =
        partitionsReassignStatus.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<TopicPartition, Integer> entry = iterator.next();
      if (entry.getValue().intValue() != ReassignmentState.ReassignmentCompleted.code()) {
        return false;
      }
    }

    return true;
  }

  private boolean isReassignComplete(
      Map<TopicPartition, Integer> partitionsReassignStatus,
      Map<TopicPartitionReplica, Integer> replicasReassignStatus) {
    Iterator<Map.Entry<TopicPartition, Integer>> iterator =
        partitionsReassignStatus.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<TopicPartition, Integer> entry = iterator.next();
      if (entry.getValue().intValue() != ReassignmentState.ReassignmentCompleted.code()) {
        return false;
      }
    }

    Iterator<Map.Entry<TopicPartitionReplica, Integer>> iterator2 =
        replicasReassignStatus.entrySet().iterator();
    while (iterator2.hasNext()) {
      Map.Entry<TopicPartitionReplica, Integer> entry = iterator2.next();
      if (entry.getValue().intValue() != ReassignmentState.ReassignmentCompleted.code()) {
        return false;
      }
    }

    return true;
  }

  @Test
  public void testExecuteReassignPartition() throws InterruptedException {
    // Setup
    final Long interBrokerThrottle = -2000L;
    final Long replicaAlterLogDirsThrottle = -3000L;
    final Long timeoutMs = 10000L;

    // Create first topic with 2 partitions and 1 replica
    createOneTopic();
    List assignedBrokerList = Arrays.asList(TEST_KAFKA_BOOTSTRAP_SERVERS_ID.get(0));

    List<TopicPartitionReplicaAssignment> topicPartitionReplicaAssignment = new ArrayList<>();
    TopicPartitionReplicaAssignment tpr =
        TopicPartitionReplicaAssignment.builder()
            .topic(FIRST_TOPIC_TO_TEST)
            .partition(0)
            .replicas(assignedBrokerList)
            .log_dirs(Arrays.asList("any"))
            .build();
    TopicPartitionReplicaAssignment tpr2 =
        TopicPartitionReplicaAssignment.builder()
            .topic(FIRST_TOPIC_TO_TEST)
            .partition(1)
            .replicas(assignedBrokerList)
            .log_dirs(Arrays.asList("any"))
            .build();
    topicPartitionReplicaAssignment.add(tpr);
    topicPartitionReplicaAssignment.add(tpr2);
    ReassignModel reassignModel =
        ReassignModel.builder().partitions(topicPartitionReplicaAssignment).build();

    // Run the test
    final ReassignStatus reassignResult =
        kafkaAdminServiceUnderTest.executeReassignPartition(
            reassignModel, interBrokerThrottle, replicaAlterLogDirsThrottle, timeoutMs);

    // Verify the results
    Map<TopicPartition, Integer> partitionsReassignStatus =
        reassignResult.getPartitionsReassignStatus();
    TopicPartition topicPartition0 = new TopicPartition(FIRST_TOPIC_TO_TEST, 0);
    int partition0ReassignState = partitionsReassignStatus.get(topicPartition0).intValue();
    assertTrue(
        partition0ReassignState == ReassignmentState.ReassignmentInProgress.code()
            || partition0ReassignState == ReassignmentState.ReassignmentCompleted.code());
    TopicPartition topicPartition1 = new TopicPartition(FIRST_TOPIC_TO_TEST, 1);
    int partition1ReassignState = partitionsReassignStatus.get(topicPartition1).intValue();
    assertTrue(
        partition1ReassignState == ReassignmentState.ReassignmentInProgress.code()
            || partition1ReassignState == ReassignmentState.ReassignmentCompleted.code());
    if (partition0ReassignState == ReassignmentState.ReassignmentCompleted.code()
        && partition1ReassignState == ReassignmentState.ReassignmentCompleted.code()) {
      // If all completed, removeThrottle will be true
      assertTrue(reassignResult.isRemoveThrottle());
    } else {
      assertFalse(reassignResult.isRemoveThrottle());
    }

    // Wait reassign to complete
    int count = 30;
    while (true) {
      Thread.sleep(2000);
      ReassignStatus reassignmentStatusFinal =
          kafkaAdminServiceUnderTest.checkReassignStatus(reassignModel);
      Map<TopicPartition, Integer> partitionsReassignStatusFinal =
          reassignmentStatusFinal.getPartitionsReassignStatus();
      if (isReassignComplete(partitionsReassignStatusFinal)) {
        for (int i = 0; i < TEST_KAFKA_BOOTSTRAP_SERVERS_ID.size(); i++) {
          Properties properties =
              kafkaAdminServiceUnderTest.getConfigInZk(
                  Type.BROKER, String.valueOf(TEST_KAFKA_BOOTSTRAP_SERVERS_ID.get(i)));
          assertFalse(properties.containsKey(KafkaAdminService.LeaderReplicationThrottledRateProp));
          assertFalse(
              properties.containsKey(KafkaAdminService.FollowerReplicationThrottledRateProp));
          assertFalse(
              properties.containsKey(KafkaAdminService.ReplicaAlterLogDirsIoMaxBytesPerSecondProp));
        }
        break;
      }
      count++;
      if (count == 30) {
        break;
      }
    }

    // Describe topic
    List<CustomTopicPartitionInfo> topicPartitionInfos =
        kafkaAdminServiceUnderTest.describeTopic(FIRST_TOPIC_TO_TEST).getTopicPartitionInfos();
    for (CustomTopicPartitionInfo ctpi : topicPartitionInfos) {
      assertEquals(1, ctpi.getTopicPartitionInfo().replicas().size());
      assertEquals(
          TEST_KAFKA_BOOTSTRAP_SERVERS_ID.get(0).intValue(),
          ctpi.getTopicPartitionInfo().replicas().get(0).id());
    }
  }

  private void waitReassignToComplete(ReassignModel reassignModel) throws InterruptedException {
    int count = 30;
    while (true) {
      Thread.sleep(2000);
      ReassignStatus reassignmentStatusFinal =
          kafkaAdminServiceUnderTest.checkReassignStatus(reassignModel);
      Map<TopicPartition, Integer> partitionsReassignStatusFinal =
          reassignmentStatusFinal.getPartitionsReassignStatus();
      Map<TopicPartitionReplica, Integer> replicasReassignStatusFinal =
          reassignmentStatusFinal.getReplicasReassignStatus();
      if (isReassignComplete(partitionsReassignStatusFinal, replicasReassignStatusFinal)) {
        break;
      }
      count++;
      if (count == 30) {
        break;
      }
    }
  }

  @Test
  public void testExecuteReassignReplicasInnerBroker() throws InterruptedException {
    // Setup
    final Long interBrokerThrottle = -1L;
    final Long replicaAlterLogDirsThrottle = -1L;
    final Long timeoutMs = 10000L;

    // Create first topic with 1 partition and 1 replica on broker 111
    int brokerId = TEST_KAFKA_BOOTSTRAP_SERVERS_ID.get(0);
    List<Integer> assignedBrokerList = TEST_KAFKA_BOOTSTRAP_SERVERS_ID.subList(0, 1);

    Map<Integer, List<Integer>> replicaAssignment = new HashMap<>();
    replicaAssignment.put(0, assignedBrokerList);
    createOneTopic(FIRST_TOPIC_TO_TEST, replicaAssignment);

    // Get first-0 log dir
    TopicPartitionReplica topicPartitionReplica =
        new TopicPartitionReplica(FIRST_TOPIC_TO_TEST, 0, brokerId);
    List<TopicPartitionReplica> replicas = Arrays.asList(topicPartitionReplica);
    String replicaLogDirBeforeReassign =
        kafkaAdminServiceUnderTest
            .describeReplicaLogDirs(replicas)
            .get(topicPartitionReplica)
            .getCurrentReplicaLogDir();

    // Find another log dir on broker 111 to reassign
    String replicaLogDirToReassign = "any";
    String[] logDirs = TEST_KAFKA_LOG_DIRS.get(0).split(",");
    for (int i = 0; i < logDirs.length; i++) {
      if (!logDirs[i].equals(replicaLogDirBeforeReassign)) {
        replicaLogDirToReassign = logDirs[i];
        break;
      }
    }

    List<TopicPartitionReplicaAssignment> topicPartitionReplicaAssignment = new ArrayList<>();
    TopicPartitionReplicaAssignment tpr =
        TopicPartitionReplicaAssignment.builder()
            .topic(FIRST_TOPIC_TO_TEST)
            .partition(0)
            .replicas(assignedBrokerList)
            .log_dirs(Arrays.asList(replicaLogDirToReassign))
            .build();

    topicPartitionReplicaAssignment.add(tpr);
    ReassignModel reassignModel =
        ReassignModel.builder().partitions(topicPartitionReplicaAssignment).build();

    // Run the test
    final ReassignStatus reassignResult =
        kafkaAdminServiceUnderTest.executeReassignPartition(
            reassignModel, interBrokerThrottle, replicaAlterLogDirsThrottle, timeoutMs);

    // Verify the results
    Map<TopicPartition, Integer> partitionsReassignStatus =
        reassignResult.getPartitionsReassignStatus();
    TopicPartition topicPartition = new TopicPartition(FIRST_TOPIC_TO_TEST, 0);
    int partitionReassignState = partitionsReassignStatus.get(topicPartition).intValue();
    assertTrue(
        partitionReassignState == ReassignmentState.ReassignmentInProgress.code()
            || partitionReassignState == ReassignmentState.ReassignmentCompleted.code());

    Map<TopicPartitionReplica, Integer> replicasReassignStatus =
        reassignResult.getReplicasReassignStatus();
    int replicaReassignState = replicasReassignStatus.get(topicPartitionReplica);
    assertTrue(
        replicaReassignState == ReassignmentState.ReassignmentInProgress.code()
            || replicaReassignState == ReassignmentState.ReassignmentCompleted.code());
    if (partitionReassignState == ReassignmentState.ReassignmentCompleted.code()
        && replicaReassignState == ReassignmentState.ReassignmentCompleted.code()) {
      // If all completed, removeThrottle will be true
      assertTrue(reassignResult.isRemoveThrottle());
    } else {
      assertFalse(reassignResult.isRemoveThrottle());
    }

    // Wait reassign to complete
    waitReassignToComplete(reassignModel);

    // Describe topic log dir
    String currentLogDir =
        kafkaAdminServiceUnderTest
            .describeReplicaLogDirs(replicas)
            .get(topicPartitionReplica)
            .getCurrentReplicaLogDir();
    assertEquals(replicaLogDirToReassign, currentLogDir);
  }

  @Test
  public void testExecuteReassignReplicasInterBroker() throws InterruptedException {
    // Setup
    final Long interBrokerThrottle = -1L;
    final Long replicaAlterLogDirsThrottle = -1L;
    final Long timeoutMs = 10000L;

    // Create first topic with 1 partition and 1 replica on broker 111
    List<Integer> assignedBrokerListBefore = TEST_KAFKA_BOOTSTRAP_SERVERS_ID.subList(0, 1);

    Map<Integer, List<Integer>> replicaAssignment = new HashMap<>();
    replicaAssignment.put(0, assignedBrokerListBefore);
    createOneTopic(FIRST_TOPIC_TO_TEST, replicaAssignment);

    // Find a log dir on broker 113 to reassign
    String replicaLogDirToReassign = TEST_KAFKA_LOG_DIRS.get(1).split(",")[0];

    int newBrokerId = TEST_KAFKA_BOOTSTRAP_SERVERS_ID.get(1);
    List<TopicPartitionReplicaAssignment> topicPartitionReplicaAssignment = new ArrayList<>();
    TopicPartitionReplicaAssignment tpr =
        TopicPartitionReplicaAssignment.builder()
            .topic(FIRST_TOPIC_TO_TEST)
            .partition(0)
            .replicas(Arrays.asList(newBrokerId))
            .log_dirs(Arrays.asList(replicaLogDirToReassign))
            .build();

    topicPartitionReplicaAssignment.add(tpr);
    ReassignModel reassignModel =
        ReassignModel.builder().partitions(topicPartitionReplicaAssignment).build();

    // Run the test
    final ReassignStatus reassignResult =
        kafkaAdminServiceUnderTest.executeReassignPartition(
            reassignModel, interBrokerThrottle, replicaAlterLogDirsThrottle, timeoutMs);

    // Verify the results
    Map<TopicPartition, Integer> partitionsReassignStatus =
        reassignResult.getPartitionsReassignStatus();
    TopicPartition topicPartition = new TopicPartition(FIRST_TOPIC_TO_TEST, 0);
    int partitionReassignState = partitionsReassignStatus.get(topicPartition).intValue();
    assertTrue(
        partitionReassignState == ReassignmentState.ReassignmentInProgress.code()
            || partitionReassignState == ReassignmentState.ReassignmentCompleted.code());

    TopicPartitionReplica topicPartitionReplica =
        new TopicPartitionReplica(FIRST_TOPIC_TO_TEST, 0, newBrokerId);
    Map<TopicPartitionReplica, Integer> replicasReassignStatus =
        reassignResult.getReplicasReassignStatus();
    int replicaReassignState = replicasReassignStatus.get(topicPartitionReplica);
    assertTrue(
        replicaReassignState == ReassignmentState.ReassignmentInProgress.code()
            || replicaReassignState == ReassignmentState.ReassignmentCompleted.code());
    if (partitionReassignState == ReassignmentState.ReassignmentCompleted.code()
        && replicaReassignState == ReassignmentState.ReassignmentCompleted.code()) {
      // If all completed, removeThrottle will be true
      assertTrue(reassignResult.isRemoveThrottle());
    } else {
      assertFalse(reassignResult.isRemoveThrottle());
    }

    // Wait reassign to complete
    waitReassignToComplete(reassignModel);

    // Describe topic log dir
    String currentLogDir =
        kafkaAdminServiceUnderTest
            .describeReplicaLogDirs(Arrays.asList(topicPartitionReplica))
            .get(topicPartitionReplica)
            .getCurrentReplicaLogDir();

    assertEquals(replicaLogDirToReassign, currentLogDir);
  }

  private List<RecordMetadata> produceRecords(String topic, int recordsCount) {
    KafkaProducer kafkaProducer = mockKafkaUtils.createProducer();
    List<RecordMetadata> recordMetadataList = new ArrayList<>();

    for (int i = 0; i < recordsCount; i++) {
      ProducerRecord<String, String> record = new ProducerRecord(topic, "record" + i);
      try {
        RecordMetadata metadata = (RecordMetadata) kafkaProducer.send(record).get();
        recordMetadataList.add(metadata);
      } catch (Exception exception) {
        log.error("Produce record:" + i + " error." + exception);
      }
    }

    kafkaProducer.close();
    return recordMetadataList;
  }

  private String getEncoder(Class<Object> type) {
    for (Map.Entry<String, Class<Object>> entry:KafkaUtils.SERIALIZER_TYPE_MAP.entrySet()) {
      if (entry.getValue().equals(type)) {
        return entry.getKey();
      }
    }

    return null;
  }

  private Map<Class<Object>, List<Long>> produceRecords(
      String topic, Map<Class<Object>, List<Object>> testData) {
    Map<Class<Object>, List<Long>> dataOffsetMap = new HashMap<>();
    KafkaProducer kafkaProducer = null;

    for (Map.Entry<Class<Object>, List<Object>> test : testData.entrySet()) {
      Class<Object> type = test.getKey();
      List<Long> offsetList = new ArrayList<>();
      try {
        kafkaProducer =
            mockKafkaUtils.createProducer(null, getEncoder(type));
        for (Object value : test.getValue()) {
          ProducerRecord record = new ProducerRecord(topic, value);
          try {
            RecordMetadata metadata = (RecordMetadata) kafkaProducer.send(record).get();
            Long offset = metadata.offset();
            offsetList.add(offset);
          } catch (Exception exception) {
            log.error("Send record exception." + exception);
          }
        }
      } catch (ClassNotFoundException classNotFoundException) {
        log.error("Encoder class not found. " + classNotFoundException);
      }
      dataOffsetMap.put(type, offsetList);
    }

    kafkaProducer.close();
    return dataOffsetMap;
  }

  private List<Long> produceAvroRecords(String topic, List<byte[]> testData) {
    KafkaProducer kafkaProducer = null;

    Class<byte[]> type = byte[].class;
    List<Long> offsetList = new ArrayList<>();
    try {
      kafkaProducer =
          mockKafkaUtils.createProducer(null, "AvroSerializer");
      for (byte[] value : testData) {
        ProducerRecord record = new ProducerRecord(topic, value);
        try {
          RecordMetadata metadata = (RecordMetadata) kafkaProducer.send(record).get();
          log.info(
              "Avro record has sent to topic:"
                  + topic
                  + ", partition:"
                  + metadata.partition()
                  + ", offset:"
                  + metadata.offset());
          Long offset = metadata.offset();
          offsetList.add(offset);
        } catch (Exception exception) {
          log.error("Send avro record exception." + exception);
        }
      }
    } catch (ClassNotFoundException classNotFoundException) {
      log.error("Encoder class not found. " + classNotFoundException);
    }

    kafkaProducer.close();

    return offsetList;
  }

  private RecordMetadata produceRecord(String topic, double key, String value) throws Exception{
    final String keySerializer = "DoubleSerializer";
    final String valueSerializer = "StringSerializer";
    KafkaProducer kafkaProducer = mockKafkaUtils.createProducer(keySerializer, valueSerializer);
    RecordMetadata metadata = null;

    ProducerRecord<String, String> record = new ProducerRecord(topic, key, value);
    try {
      metadata = (RecordMetadata) kafkaProducer.send(record).get();
      log.info("Record(key:" + key + ", value:" + value + ") has been sent to topic:" +
          metadata.topic() + ", partition:" + metadata.partition() + ", offset:" + metadata.offset());
    } catch (Exception exception) {
      log.error("Produce record:" + 0 + " error." + exception);
    }

    kafkaProducer.close();
    return metadata;
  }

  @Test
  public void testResetOffsetToBeginning() throws InterruptedException {
    // Setup
    final String topic = FIRST_TOPIC_TO_TEST;
    final int partition = 0;
    final String consumerGroup = FIRST_CONSUMER_GROUP_TO_TEST;
    final ConsumerType type = ConsumerType.NEW;
    final String offset = "earliest";
    final int recordsCount = 5;

    // Create first topic with 1 partition and 1 replica
    createOneTopic(topic, 1, 1);

    // Produce data
    produceRecords(topic, recordsCount);

    // Create a group with only one consumer
    ConsumerGroup group =
        new ConsumerGroup(
            consumerGroup,
            1,
            Arrays.asList(FIRST_CONSUMER_CLIENT_TO_TEST),
            Arrays.asList(FIRST_TOPIC_TO_TEST));
    group.execute();
    Thread.sleep(1000);

    // Reset offset can only be done when consumer group is inactive
    group.close();

    // Run the test
    final GeneralResponse resetResult =
        kafkaAdminServiceUnderTest.resetOffset(topic, partition, consumerGroup, type, offset);
    assertEquals(GeneralResponseState.success, resetResult.getState());
    assertEquals(0L, Long.parseLong(resetResult.getData().toString()));
    // Verify the results
    KafkaConsumer consumer = mockKafkaUtils.createNewConsumer(consumerGroup);
    consumer.subscribe(Arrays.asList(topic));
    while (true) {
      ConsumerRecords<String, String> messages = consumer.poll(100);
      if (messages.count() == 0) {
        break;
      }
      assertEquals(recordsCount, messages.count());
      int i = 0;
      for (ConsumerRecord<String, String> message : messages) {
        // Consumer will consume start from offset:0
        assertEquals(i, message.offset());
        i++;
      }
    }
    consumer.close();
  }

  @Test
  public void testResetOffsetToLatest() throws InterruptedException {
    // Setup
    final String topic = FIRST_TOPIC_TO_TEST;
    final int partition = 0;
    final String consumerGroup = FIRST_CONSUMER_GROUP_TO_TEST;
    final ConsumerType type = ConsumerType.NEW;
    final String offset = "latest";
    final int recordsCount = 5;

    // Create first topic with 1 partition and 1 replica
    createOneTopic(topic, 1, 1);

    // Produce data
    produceRecords(topic, recordsCount);

    // Create a group with only one consumer
    ConsumerGroup group =
        new ConsumerGroup(
            consumerGroup,
            1,
            Arrays.asList(FIRST_CONSUMER_CLIENT_TO_TEST),
            Arrays.asList(FIRST_TOPIC_TO_TEST));
    group.execute();
    Thread.sleep(1000);

    // Reset offset can only be done when consumer group is inactive
    group.close();

    // Run the test
    final GeneralResponse resetResult =
        kafkaAdminServiceUnderTest.resetOffset(topic, partition, consumerGroup, type, offset);
    assertEquals(GeneralResponseState.success, resetResult.getState());
    assertEquals(recordsCount, Long.parseLong(resetResult.getData().toString()));
    // Verify the results
    // Produce another 5 records
    produceRecords(topic, recordsCount);
    KafkaConsumer consumer = mockKafkaUtils.createNewConsumer(consumerGroup);
    consumer.subscribe(Arrays.asList(topic));
    while (true) {
      ConsumerRecords<String, String> messages = consumer.poll(100);
      if (messages.count() == 0) {
        break;
      }
      assertEquals(recordsCount, messages.count());
      int i = 0;
      for (ConsumerRecord<String, String> message : messages) {
        // Consumer will consume start from offset:5
        assertEquals(recordsCount + i, message.offset());
        i++;
      }
    }
    consumer.close();
  }

  @Test
  public void testResetOffsetToSpecificOffset() throws InterruptedException {
    // Setup
    final String topic = FIRST_TOPIC_TO_TEST;
    final int partition = 0;
    final String consumerGroup = FIRST_CONSUMER_GROUP_TO_TEST;
    final ConsumerType type = ConsumerType.NEW;
    final String offset = "2";
    final int recordsCount = 5;

    // Create first topic with 1 partition and 1 replica
    createOneTopic(topic, 1, 1);

    // Produce data
    produceRecords(topic, recordsCount);

    // Create a group with only one consumer
    ConsumerGroup group =
        new ConsumerGroup(
            consumerGroup,
            1,
            Arrays.asList(FIRST_CONSUMER_CLIENT_TO_TEST),
            Arrays.asList(FIRST_TOPIC_TO_TEST));
    group.execute();
    Thread.sleep(1000);

    // Reset offset can only be done when consumer group is inactive
    group.close();

    // Run the test
    final GeneralResponse resetResult =
        kafkaAdminServiceUnderTest.resetOffset(topic, partition, consumerGroup, type, offset);

    assertEquals(GeneralResponseState.success, resetResult.getState());
    assertEquals(Long.parseLong(offset), Long.parseLong(resetResult.getData().toString()));
    // Verify the results
    KafkaConsumer consumer = mockKafkaUtils.createNewConsumer(consumerGroup);
    consumer.subscribe(Arrays.asList(topic));
    while (true) {
      ConsumerRecords<String, String> messages = consumer.poll(100);
      if (messages.count() == 0) {
        break;
      }
      assertEquals(recordsCount - Long.parseLong(offset), messages.count());
      int i = 0;
      for (ConsumerRecord<String, String> message : messages) {
        // Consumer will consume start from offset:3
        assertEquals(Long.parseLong(offset) + i, message.offset());
        i++;
      }
    }
    consumer.close();
  }

  @Test
  public void testResetOffsetToSpecificOffsetButOutOfRange() throws InterruptedException {
    // Setup
    final String topic = FIRST_TOPIC_TO_TEST;
    final int partition = 0;
    final String consumerGroup = FIRST_CONSUMER_GROUP_TO_TEST;
    final ConsumerType type = ConsumerType.NEW;
    final String offset = "10";
    final int recordsCount = 5;

    // Create first topic with 1 partition and 1 replica
    createOneTopic(topic, 1, 1);

    // Produce data
    produceRecords(topic, recordsCount);

    // Create a group with only one consumer
    ConsumerGroup group =
        new ConsumerGroup(
            consumerGroup,
            1,
            Arrays.asList(FIRST_CONSUMER_CLIENT_TO_TEST),
            Arrays.asList(FIRST_TOPIC_TO_TEST));
    group.execute();
    Thread.sleep(1000);

    // Reset offset can only be done when consumer group is inactive
    group.close();

    // Run the test
    final GeneralResponse resetResult =
        kafkaAdminServiceUnderTest.resetOffset(topic, partition, consumerGroup, type, offset);

    assertEquals(GeneralResponseState.failure, resetResult.getState());
    assertTrue(resetResult.getMsg().contains("Invalid request offset:" + offset));
  }

  @Test
  public void testResetOffsetByTime() throws InterruptedException {
    // Setup
    final String topic = FIRST_TOPIC_TO_TEST;
    final int partition = 0;
    final String consumerGroup = FIRST_CONSUMER_GROUP_TO_TEST;
    final ConsumerType type = ConsumerType.NEW;
    final int recordsCount = 5;

    // Create first topic with 1 partition and 1 replica
    createOneTopic(topic, 1, 1);

    // Produce data
    List<RecordMetadata> metadataList = produceRecords(topic, recordsCount);

    // Get the record timestamp to reset
    int recordIndexToReset = 3;
    Long timeToReset = metadataList.get(recordIndexToReset).timestamp();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    final String offset = sdf.format(new Date(timeToReset));

    // Create a group with only one consumer
    ConsumerGroup group =
        new ConsumerGroup(
            consumerGroup,
            1,
            Arrays.asList(FIRST_CONSUMER_CLIENT_TO_TEST),
            Arrays.asList(FIRST_TOPIC_TO_TEST));
    group.execute();
    Thread.sleep(1000);

    // Reset offset can only be done when consumer group is inactive
    group.close();

    // Run the test
    final GeneralResponse resetResult =
        kafkaAdminServiceUnderTest.resetOffset(topic, partition, consumerGroup, type, offset);

    assertEquals(GeneralResponseState.success, resetResult.getState());
    assertEquals(recordIndexToReset, Long.parseLong(resetResult.getData().toString()));
    // Verify the results
    KafkaConsumer consumer = mockKafkaUtils.createNewConsumer(consumerGroup);
    consumer.subscribe(Arrays.asList(topic));
    while (true) {
      ConsumerRecords<String, String> messages = consumer.poll(100);
      if (messages.count() == 0) {
        break;
      }
      assertEquals(recordsCount - recordIndexToReset, messages.count());
      int i = 0;
      for (ConsumerRecord<String, String> message : messages) {
        // Consumer will consume start from offset:0
        assertEquals(i + recordIndexToReset, message.offset());
        i++;
      }
    }
    consumer.close();
  }

  @Test
  public void testResetOffsetByTimeButMoreThanEndOffsetTime() throws InterruptedException {
    // Setup
    final String topic = FIRST_TOPIC_TO_TEST;
    final int partition = 0;
    final String consumerGroup = FIRST_CONSUMER_GROUP_TO_TEST;
    final ConsumerType type = ConsumerType.NEW;
    final int recordsCount = 5;

    // Create first topic with 1 partition and 1 replica
    createOneTopic(topic, 1, 1);

    // Produce data
    List<RecordMetadata> metadataList = produceRecords(topic, recordsCount);

    // Get the (last record timestamp + 1000) to reset
    Long timeToReset = metadataList.get(recordsCount - 1).timestamp() + 1000;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    final String offset = sdf.format(new Date(timeToReset));

    // Create a group with only one consumer
    ConsumerGroup group =
        new ConsumerGroup(
            consumerGroup,
            1,
            Arrays.asList(FIRST_CONSUMER_CLIENT_TO_TEST),
            Arrays.asList(FIRST_TOPIC_TO_TEST));
    group.execute();
    Thread.sleep(1000);

    // Reset offset can only be done when consumer group is inactive
    group.close();

    // Run the test
    final GeneralResponse resetResult =
        kafkaAdminServiceUnderTest.resetOffset(topic, partition, consumerGroup, type, offset);

    assertEquals(GeneralResponseState.failure, resetResult.getState());
    assertTrue(
        resetResult
            .getMsg()
            .contains(
                "No offset's timestamp is greater than or equal to the given timestamp:" + offset));
  }

  @Test
  public void testResetOffsetOnActiveConsumerGroup() throws InterruptedException {
    // Setup
    final String topic = FIRST_TOPIC_TO_TEST;
    final int partition = 0;
    final String consumerGroup = FIRST_CONSUMER_GROUP_TO_TEST;
    final ConsumerType type = ConsumerType.NEW;
    final String offset = "1";
    final int recordsCount = 5;

    // Create first topic with 1 partition and 1 replica
    createOneTopic(topic, 1, 1);

    // Produce data
    produceRecords(topic, recordsCount);

    // Create a group with only one consumer
    ConsumerGroup group =
        new ConsumerGroup(
            consumerGroup,
            1,
            Arrays.asList(FIRST_CONSUMER_CLIENT_TO_TEST),
            Arrays.asList(FIRST_TOPIC_TO_TEST));
    group.execute();
    Thread.sleep(1000);

    // Run the test
    final GeneralResponse resetResult =
        kafkaAdminServiceUnderTest.resetOffset(topic, partition, consumerGroup, type, offset);

    assertEquals(GeneralResponseState.failure, resetResult.getState());
    assertTrue(
        resetResult
            .getMsg()
            .contains("Offsets can only be reset if the group " + consumerGroup + " is inactive"));

    group.close();
  }

  @Test
  public void testResetOffsetOnNonExistConsumerGroup() throws InterruptedException {
    // Setup
    final String topic = FIRST_TOPIC_TO_TEST;
    final int partition = 0;
    final String consumerGroup = FIRST_CONSUMER_GROUP_TO_TEST;
    final ConsumerType type = ConsumerType.NEW;
    final String offset = "1";
    final int recordsCount = 5;

    // Create first topic with 1 partition and 1 replica
    createOneTopic(topic, 1, 1);

    // Produce data
    produceRecords(topic, recordsCount);

    // Create a group with only one consumer
    ConsumerGroup group =
        new ConsumerGroup(
            consumerGroup,
            1,
            Arrays.asList(FIRST_CONSUMER_CLIENT_TO_TEST),
            Arrays.asList(FIRST_TOPIC_TO_TEST));
    group.execute();
    Thread.sleep(1000);

    // Reset offset can only be done when consumer group is inactive
    group.close();

    // Run the test
    final GeneralResponse resetResult =
        kafkaAdminServiceUnderTest.resetOffset(
            topic, partition, SECOND_CONSUMER_GROUP_TO_TEST, type, offset);

    assertEquals(GeneralResponseState.failure, resetResult.getState());
    assertTrue(resetResult.getMsg().contains("non-exists"));
  }

  @Test
  public void testResetOffsetOnInvalidPartition() throws InterruptedException {
    // Setup
    final String topic = FIRST_TOPIC_TO_TEST;
    final int partition = 1;
    final String consumerGroup = FIRST_CONSUMER_GROUP_TO_TEST;
    final ConsumerType type = ConsumerType.NEW;
    final String offset = "1";
    final int recordsCount = 5;

    // Create first topic with 1 partition and 1 replica
    createOneTopic(topic, 1, 1);

    // Produce data
    produceRecords(topic, recordsCount);

    // Create a group with only one consumer
    ConsumerGroup group =
        new ConsumerGroup(
            consumerGroup,
            1,
            Arrays.asList(FIRST_CONSUMER_CLIENT_TO_TEST),
            Arrays.asList(FIRST_TOPIC_TO_TEST));
    group.execute();
    Thread.sleep(1000);

    // Reset offset can only be done when consumer group is inactive
    group.close();

    // Run the test
    final GeneralResponse resetResult =
        kafkaAdminServiceUnderTest.resetOffset(topic, partition, consumerGroup, type, offset);

    assertEquals(GeneralResponseState.failure, resetResult.getState());
    assertTrue(resetResult.getMsg().contains("has no partition"));
  }

  @Test
  public void testDescribeLogDirsByBrokerAndTopicPartition() {
    // Setup
    final String topic = FIRST_TOPIC_TO_TEST;
    final Map<String, List<Integer>> topicPartitionMap = new HashMap<>();
    topicPartitionMap.put(topic, Arrays.asList(0));

    int brokerId = TEST_KAFKA_BOOTSTRAP_SERVERS_ID.get(0);
    final List<Integer> brokerList = Arrays.asList(brokerId);
    Map<Integer, List<Integer>> replicaAssignment = new HashMap<>();
    replicaAssignment.put(0, brokerList);

    // Create first topic with 1 partition on first broker
    createOneTopic(topic, replicaAssignment);

    // Run the test
    final Map<Integer, Map<String, LogDirInfo>> result =
        kafkaAdminServiceUnderTest
            .describeLogDirsByBrokerAndTopic(brokerList, null, topicPartitionMap);

    //     Verify the results
    assertTrue(result.containsKey(brokerId));

    Map<String, LogDirInfo> logDirInfoMap = result.get(brokerId);
    String[] logDirsOnBroker = TEST_KAFKA_LOG_DIRS.get(0).split(",");
    TopicPartition topicPartition = new TopicPartition(topic, 0);
    boolean logDirExist = false;
    for (int i = 0; i < logDirsOnBroker.length; i++) {
      String logDir = logDirsOnBroker[i];
      assertTrue(logDirInfoMap.containsKey(logDir));
      LogDirInfo logDirInfo = logDirInfoMap.get(logDir);
      logDirExist = logDirInfo.replicaInfos.containsKey(topicPartition);
      if (logDirExist) {
        break;
      }
    }
    assertTrue(logDirExist);
  }

  @Test
  public void testDescribeLogDirsByBrokerLogDirAndTopicPartition() {
    // Setup
    final String topic = FIRST_TOPIC_TO_TEST;
    final Map<String, List<Integer>> topicPartitionMap = new HashMap<>();
    topicPartitionMap.put(topic, Arrays.asList(0));

    int brokerId = TEST_KAFKA_BOOTSTRAP_SERVERS_ID.get(0);
    final List<Integer> brokerList = Arrays.asList(brokerId);
    Map<Integer, List<Integer>> replicaAssignment = new HashMap<>();
    replicaAssignment.put(0, brokerList);
    replicaAssignment.put(1, brokerList);

    // Create first topic with 2 partition on first broker
    createOneTopic(topic, replicaAssignment);

    TopicPartitionReplica topicPartitionReplica = new TopicPartitionReplica(topic, 0, brokerId);
    Map<TopicPartitionReplica, ReplicaLogDirInfo> replicaReplicaLogDirInfoMap = kafkaAdminServiceUnderTest
        .describeReplicaLogDirs(Arrays.asList(topicPartitionReplica));
    String logDir = replicaReplicaLogDirInfoMap.get(topicPartitionReplica)
        .getCurrentReplicaLogDir();

    // Run the test
    final Map<Integer, Map<String, LogDirInfo>> result =
        kafkaAdminServiceUnderTest
            .describeLogDirsByBrokerAndTopic(brokerList, Arrays.asList(logDir), topicPartitionMap);

    // Verify the results
    assertTrue(result.containsKey(brokerId));
    Map<String, LogDirInfo> logDirInfoMap = result.get(brokerId);
    assertEquals(1, logDirInfoMap.size());
    assertTrue(logDirInfoMap.containsKey(logDir));
    LogDirInfo logDirInfo = logDirInfoMap.get(logDir);
    TopicPartition topicPartition = new TopicPartition(topic, 0);
    assertTrue(logDirInfo.replicaInfos.containsKey(topicPartition));
    TopicPartition topicPartition1 = new TopicPartition(topic, 1);
    assertFalse(logDirInfo.replicaInfos.containsKey(topicPartition1));
  }

  @Test
  public void testDescribeLogDirsByBrokerLogDirAndNullTopicPartition() {
    // Setup
    final String topic = FIRST_TOPIC_TO_TEST;
    final Map<String, List<Integer>> topicPartitionMap = new HashMap<>();
    topicPartitionMap.put(topic, null);

    int brokerId = TEST_KAFKA_BOOTSTRAP_SERVERS_ID.get(0);
    final List<Integer> brokerList = Arrays.asList(brokerId);
    Map<Integer, List<Integer>> replicaAssignment = new HashMap<>();
    replicaAssignment.put(0, brokerList);

    // Create first topic with 1 partition on first broker
    createOneTopic(topic, replicaAssignment);

    TopicPartitionReplica topicPartitionReplica = new TopicPartitionReplica(topic, 0, brokerId);
    Map<TopicPartitionReplica, ReplicaLogDirInfo> replicaReplicaLogDirInfoMap = kafkaAdminServiceUnderTest
        .describeReplicaLogDirs(Arrays.asList(topicPartitionReplica));
    String logDir = replicaReplicaLogDirInfoMap.get(topicPartitionReplica)
        .getCurrentReplicaLogDir();

    // Run the test
    final Map<Integer, Map<String, LogDirInfo>> result =
        kafkaAdminServiceUnderTest
            .describeLogDirsByBrokerAndTopic(brokerList, Arrays.asList(logDir), topicPartitionMap);

    // Verify the results
    assertTrue(result.containsKey(brokerId));
    Map<String, LogDirInfo> logDirInfoMap = result.get(brokerId);
    assertEquals(1, logDirInfoMap.size());
    assertTrue(logDirInfoMap.containsKey(logDir));
    LogDirInfo logDirInfo = logDirInfoMap.get(logDir);
    TopicPartition topicPartition = new TopicPartition(topic, 0);
    assertTrue(logDirInfo.replicaInfos.containsKey(topicPartition));
  }

  @Test
  public void testDescribeReplicaLogDirs() {
    // Setup
    final String topic = FIRST_TOPIC_TO_TEST;
    int brokerId = TEST_KAFKA_BOOTSTRAP_SERVERS_ID.get(0);
    final List<Integer> brokerList = Arrays.asList(brokerId);
    Map<Integer, List<Integer>> replicaAssignment = new HashMap<>();
    replicaAssignment.put(0, brokerList);

    // Create first topic with 1 partition on first broker
    createOneTopic(topic, replicaAssignment);

    TopicPartitionReplica topicPartitionReplica = new TopicPartitionReplica(topic, 0, brokerId);
    final List<TopicPartitionReplica> replicas = Arrays.asList(topicPartitionReplica);
    // Run the test
    final Map<TopicPartitionReplica, ReplicaLogDirInfo> result =
        kafkaAdminServiceUnderTest.describeReplicaLogDirs(replicas);

    // Verify the results
    String currentLogDir = result.get(topicPartitionReplica).getCurrentReplicaLogDir();
    final Map<Integer, Map<String, LogDirInfo>> logDirsByBrokerAndTopic =
        kafkaAdminServiceUnderTest.describeLogDirsByBrokerAndTopic(
            brokerList, null, null);
    Map<String, LogDirInfo> logDirInfoMap = logDirsByBrokerAndTopic.get(brokerId);
    assertTrue(logDirInfoMap.containsKey(currentLogDir));
    TopicPartition topicPartition = new TopicPartition(topic, 0);
    assertTrue(logDirInfoMap.get(currentLogDir).replicaInfos.containsKey(topicPartition));
  }

  @Test
  public void testGetBeginningOffset() {
    // Setup
    final String topic = FIRST_TOPIC_TO_TEST;
    final int partitionId = 0;
    final int recordsCount = 5;

    // Create first topic with 1 partition and 1 replica
    createOneTopic(topic, 1, 1);

    // Produce data
    produceRecords(topic, recordsCount);

    // Run the test
    final long result = kafkaAdminServiceUnderTest.getBeginningOffset(topic, partitionId);

    // Verify the results
    assertEquals(0, result);
  }

  @Test
  public void testGetEndOffset() {
    // Setup
    final String topic = FIRST_TOPIC_TO_TEST;
    final int partitionId = 0;
    final int recordsCount = 5;

    // Create first topic with 1 partition and 1 replica
    createOneTopic(topic, 1, 1);

    // Produce data
    produceRecords(topic, recordsCount);

    // Run the test
    final long result = kafkaAdminServiceUnderTest.getEndOffset(topic, partitionId);

    // Verify the results
    assertEquals(recordsCount, result);
  }

  @Test
  public void testGetMessage() {
    // Setup
    final String topic = FIRST_TOPIC_TO_TEST;
    final int partition = 0;
    final long offset = 1L;
    final String decoder = "decoder";
    final String avroSchema = "avroSchema";
    final int recordsCount = 5;

    // Create first topic with 1 partition and 1 replica
    createOneTopic(topic, 1, 1);

    // Produce data
    List<RecordMetadata> metadataList = produceRecords(topic, recordsCount);

    // Run the test
    final String result =
        kafkaAdminServiceUnderTest.getMessage(topic, partition, offset, decoder, avroSchema);

    // Verify the results
    String expectedResult =
        "Value: record"
            + offset
            + ", Offset: "
            + offset
            + ", timestamp:"
            + metadataList.get(1).timestamp();
    assertEquals(expectedResult, result);
  }

  @Test
  public void testGetRecordByOffset() {
    final Map<Class<Object>, List<Object>> testData =
        new HashMap() {
          {
            put(String.class, Arrays.asList("my string"));
            put(Short.class, Arrays.asList((short) 32767, (short) -32768));
            put(Integer.class, Arrays.asList((int) 423412424, (int) -41243432));
            put(Long.class, Arrays.asList(922337203685477580L, -922337203685477581L));
            put(Float.class, Arrays.asList(5678567.12312f, -5678567.12341f));
            put(Double.class, Arrays.asList(5678567.12312d, -5678567.12341d));
            put(byte[].class, Arrays.asList("my string".getBytes()));
            put(
                ByteBuffer.class,
                Arrays.asList(ByteBuffer.allocate(10).put("my string".getBytes())));
            put(Bytes.class, Arrays.asList(new Bytes("my string".getBytes())));
          }
        };
    // Setup
    final String topic = FIRST_TOPIC_TO_TEST;
    final int partition = 0;
    final String avroSchema = "avroSchema";
    final int maxRecords = 1;
    final long fetchTimeoutMs = 30000;

    // Create first topic
    createOneTopic(topic, 1, 1);

    // Produce records
    try {
      Map<Class<Object>, List<Long>> dataOffsetMap = produceRecords(topic, testData);
      for (Map.Entry<Class<Object>, List<Long>> entry : dataOffsetMap.entrySet()) {
        Class<Object> type = entry.getKey();
        List<Long> offsetList = entry.getValue();
        Serde<Object> serde = Serdes.serdeFrom(type);
        for (int i = 0; i < offsetList.size(); i++) {
          Long offset = offsetList.get(i);
          String decoder = serde.deserializer().getClass().getSimpleName();
          List<Record> result =
              kafkaAdminServiceUnderTest.getRecordsByOffset(
                  topic, partition, offset, maxRecords, decoder, decoder, avroSchema,
                  fetchTimeoutMs);

          Object exceptedValue =
              serde
                  .deserializer()
                  .deserialize(
                      topic, serde.serializer().serialize(topic, testData.get(type).get(i)));
          if (type.equals(ByteBuffer.class)) {
            ByteBuffer byteBuffer = (ByteBuffer) exceptedValue;
            assertEquals(new String(byteBuffer.array()), result.get(0).getValue());
          } else if (type.equals(byte[].class)) {
            assertEquals(new String((byte[]) exceptedValue), result.get(0).getValue());
          } else {
            assertEquals(exceptedValue.toString(), result.get(0).getValue());
          }
        }
      }
    } catch (Exception exception) {
      log.error("Catch exception." + exception);
    }
  }

  @Test
  public void testGetRecordByOffsetWithDifferentKeyValueDecoder() throws Exception {
    final String keyDecoder = "DoubleDeserializer";
    final String valueDecoder = "StringDeserializer";
    final double key = 0.01;
    final String value = "test";

    // Setup
    final String topic = FIRST_TOPIC_TO_TEST;
    final int partition = 0;
    final String avroSchema = "";
    final int maxRecords = 1;
    final long fetchTimeoutMs = 30000;

    // Create first topic
    createOneTopic(topic, 1, 1);

    produceRecord(topic, key, value);
    List<Record> result = kafkaAdminServiceUnderTest.getRecordsByOffset(
        topic, partition, 0, maxRecords, keyDecoder, valueDecoder, avroSchema,
        fetchTimeoutMs);

    assertEquals(1, result.size());
    assertEquals(String.valueOf(key), result.get(0).getKey());
    assertEquals(value, result.get(0).getValue());
  }

  @Test
  public void testGetRecordByOffsetWithInvalidDecoder() {
    final Map<Class<Object>, List<Object>> testData =
        new HashMap() {
          {
            put(String.class, Arrays.asList("my string"));
          }
        };
    // Setup
    final String topic = FIRST_TOPIC_TO_TEST;
    final int partition = 0;
    final String avroSchema = "avroSchema";
    final int maxRecords = 1;
    final long fetchTimeoutMs = 30000;

    // Create first topic
    createOneTopic(topic, 1, 1);

    String decoder = "DoubleDeserializer";
    // Produce records
    try {
      Map<Class<Object>, List<Long>> dataOffsetMap = produceRecords(topic, testData);
      for (Map.Entry<Class<Object>, List<Long>> entry : dataOffsetMap.entrySet()) {
        List<Long> offsetList = entry.getValue();
        for (int i = 0; i < offsetList.size(); i++) {
          Long offset = offsetList.get(i);
          // Use DoubleDeserializer to dese string record
          List<Record> result =
              kafkaAdminServiceUnderTest.getRecordsByOffset(
                  topic, partition, offset, maxRecords, decoder, decoder, avroSchema,
                  fetchTimeoutMs);
        }
      }
    } catch (ApiException apiException) {
      assertTrue(
          apiException
              .getMessage()
              .contains(
                  "Consume "
                      + topic
                      + "-"
                      + partition
                      + " offset:"
                      + 0
                      + " using keyDecoder:" + decoder + ", valueDecoder:"
                      + decoder
                      + " exception."));
    }
  }

  @Test
  public void testGetRecordByAvroDeseriliazer() throws InterruptedException {
    String topic = FIRST_TOPIC_TO_TEST;

    // create first topic
    createOneTopic(topic, 1, 1);

    String schemaStr =
        "{\"namespace\": \"com.example.avro.model\",\n"
            + " \"type\": \"record\",\n"
            + " \"name\": \"User\",\n"
            + " \"fields\": [\n"
            + "     {\"name\": \"name\", \"type\": \"string\"},\n"
            + "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n"
            + "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n"
            + " ]\n"
            + "}";

    Schema schema = new Schema.Parser().parse(schemaStr);

    User user = new User("cmbc", 1, "green");
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    BinaryEncoder binaryEncoder =
        EncoderFactory.get().directBinaryEncoder(byteArrayOutputStream, null);
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    byte[] bytes = null;
    List<Long> offsetList = new ArrayList<>();
    try {
      datumWriter.write(user, binaryEncoder);
      binaryEncoder.flush();
      bytes = byteArrayOutputStream.toByteArray();
      offsetList = produceAvroRecords(topic, Arrays.asList(bytes));
      byteArrayOutputStream.close();
    } catch (Exception exception) {
      log.error("exception:" + exception);
    }

    String decoder = "AvroDeserializer";
    if (offsetList.size() > 0) {
      List<Record> result =
          kafkaAdminServiceUnderTest.getRecordsByOffset(
              topic, 0, offsetList.get(0), 1, null, decoder, schemaStr, 30000);
      assertEquals(1, result.size());
      assertEquals(user.toString(), result.get(0).getValue());
    }
  }

  /*
  @Test
  public void testGetConfigInZk() {
    // Setup
    final Type type = null;
    final String name = "name";
    final Properties expectedResult = new Properties();

    // Run the test
    final Properties result = kafkaAdminServiceUnderTest.getConfigInZk(type, name);

    // Verify the results
    assertEquals(expectedResult, result);
  }

  @Test
  public void testUpdateBrokerDynConf() {
    // Setup
    final int brokerId = 0;
    final Properties propsToBeUpdated = new Properties();
    final Properties expectedResult = new Properties();

    // Run the test
    final Properties result =
        kafkaAdminServiceUnderTest.updateBrokerDynConf(brokerId, propsToBeUpdated);

    // Verify the results
    assertEquals(expectedResult, result);
  }

  @Test
  public void testRemoveConfigInZk() {
    // Setup
    final Type type = null;
    final String name = "name";
    final List<String> configKeysToBeRemoved = Arrays.asList();

    // Run the test
    kafkaAdminServiceUnderTest.removeConfigInZk(type, name, configKeysToBeRemoved);

    // Verify the results
  }

  @Test
  public void testDescribeConfig() {
    // Setup
    final Type type = null;
    final String name = "name";
    final Collection<ConfigEntry> expectedResult = Arrays.asList();

    // Run the test
    final Collection<ConfigEntry> result = kafkaAdminServiceUnderTest.describeConfig(type, name);

    // Verify the results
    assertEquals(expectedResult, result);
  }

  @Test
  public void testAlterConfig() {
    // Setup
    final Type type = null;
    final String name = "name";
    final Collection<ConfigEntry> configEntries = Arrays.asList();
    final boolean expectedResult = false;

    // Run the test
    final boolean result = kafkaAdminServiceUnderTest.alterConfig(type, name, configEntries);

    // Verify the results
    assertEquals(expectedResult, result);
  }

  @Test
  public void testUpdateTopicConf() {
    // Setup
    final String topic = "topic";
    final Properties props = new Properties();
    final Collection<CustomConfigEntry> expectedResult = Arrays.asList();

    // Run the test
    final Collection<CustomConfigEntry> result =
        kafkaAdminServiceUnderTest.updateTopicConf(topic, props);

    // Verify the results
    assertEquals(expectedResult, result);
  }

  @Test
  public void testGetTopicConf() {
    // Setup
    final String topic = "topic";
    final Collection<CustomConfigEntry> expectedResult = Arrays.asList();

    // Run the test
    final Collection<CustomConfigEntry> result = kafkaAdminServiceUnderTest.getTopicConf(topic);

    // Verify the results
    assertEquals(expectedResult, result);
  }

  @Test
  public void testGetTopicConfByKey() {
    // Setup
    final String topic = "topic";
    final String key = "key";
    final Properties expectedResult = new Properties();

    // Run the test
    final Properties result = kafkaAdminServiceUnderTest.getTopicConfByKey(topic, key);

    // Verify the results
    assertEquals(expectedResult, result);
  }

  @Test
  public void testUpdateTopicConfByKey() {
    // Setup
    final String topic = "topic";
    final String key = "key";
    final String value = "value";
    final Collection<CustomConfigEntry> expectedResult = Arrays.asList();

    // Run the test
    final Collection<CustomConfigEntry> result =
        kafkaAdminServiceUnderTest.updateTopicConfByKey(topic, key, value);

    // Verify the results
    assertEquals(expectedResult, result);
  }

  @Test
  public void testGetLastCommitTime() {
    // Setup
    final String consumerGroup = "consumerGroup";
    final String topic = "topic";
    final ConsumerType type = null;
    final Map<String, Map<Integer, Long>> expectedResult = new HashMap<>();

    // Run the test
    final Map<String, Map<Integer, Long>> result =
        kafkaAdminServiceUnderTest.getLastCommitTime(consumerGroup, topic, type);

    // Verify the results
    assertEquals(expectedResult, result);
  }
  */
}
