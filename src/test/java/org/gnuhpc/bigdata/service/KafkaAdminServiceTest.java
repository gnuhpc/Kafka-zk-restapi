package org.gnuhpc.bigdata.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertTrue;

import io.swagger.models.auth.In;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.gnuhpc.bigdata.componet.OffsetStorage;
import org.gnuhpc.bigdata.config.KafkaConfig;
import org.gnuhpc.bigdata.config.ZookeeperConfig;
import org.gnuhpc.bigdata.constant.ConsumerType;
import org.gnuhpc.bigdata.constant.GeneralResponseState;
import org.gnuhpc.bigdata.model.BrokerInfo;
import org.gnuhpc.bigdata.model.ClusterInfo;
import org.gnuhpc.bigdata.model.ConsumerGroupMeta;
import org.gnuhpc.bigdata.model.CustomConfigEntry;
import org.gnuhpc.bigdata.model.GeneralResponse;
import org.gnuhpc.bigdata.model.MemberDescription;
import org.gnuhpc.bigdata.model.PartitionAssignmentState;
import org.gnuhpc.bigdata.model.TopicBrief;
import org.gnuhpc.bigdata.model.TopicDetail;
import org.gnuhpc.bigdata.model.TopicMeta;
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
public class KafkaAdminServiceTest {
  @Mock private static KafkaConfig mockKafkaConfig;
  @Mock private static ZookeeperConfig mockZookeeperConfig;
  @Spy private ZookeeperUtils mockZookeeperUtils = new ZookeeperUtils();
  @Spy private KafkaUtils mockKafkaUtils = new KafkaUtils();
  @Mock private OffsetStorage mockStorage;

  @InjectMocks private KafkaAdminService kafkaAdminServiceUnderTest;

  private static final String TEST_KAFKA_BOOTSTRAP_SERVERS =
      "localhost:19092,localhost:19093,localhost:19095";
  private static final List<Integer> TEST_KAFKA_BOOTSTRAP_SERVERS_ID = Arrays.asList(111, 113, 115);
  private static final int KAFKA_NODES_COUNT = TEST_KAFKA_BOOTSTRAP_SERVERS_ID.size();
  private static final String TEST_ZK = "localhost:2183";
  private static final int TEST_CONTROLLER_ID = 113;
  private static final List<String> TEST_KAFKA_LOG_DIRS =
      Arrays.asList(
          "/home/xiangli/bigdata/kafka_2.11-1.1.1/kafka-logs,/home/xiangli/bigdata/kafka_2.11-1.1.1/kafka111_2-logs,/home/xiangli/bigdata/kafka_2.11-1.1.1/kafka111_3-logs",
          "/home/xiangli/bigdata/kafka_2.11-1.1.1/kafka113-logs,/home/xiangli/bigdata/kafka_2.11-1.1.1/kafka113_2-logs,/home/xiangli/bigdata/kafka_2.11-1.1.1/kafka113_3-logs",
          "/home/xiangli/bigdata/kafka_2.11-1.1.1/kafka115-logs,/home/xiangli/bigdata/kafka_2.11-1.1.1/kafka115_2-logs,/home/xiangli/bigdata/kafka_2.11-1.1.1/kafka115_3-logs");

  private static final String FIRST_TOPIC_TO_TEST = "first";
  private static final String SECOND_TOPIC_TO_TEST = "second";
  private static final String NON_EXIST_TOPIC_TO_TEST = "nontopic";
  private static final String FIRST_CONSUMER_GROUP_TO_TEST = "testConsumerGroup1";
  private static final String SECOND_CONSUMER_GROUP_TO_TEST = "testConsumerGroup2";
  private static final String FIRST_CONSUMER_CLIENT_TO_TEST = "testConsumerClient1";
  private static final String SECOND_CONSUMER_CLIENT_TO_TEST = "testConsumerClient2";

  public static final String GROUP_METADATA_TOPIC_NAME = "__consumer_offsets";
  public static final int GROUP_METADATA_TOPIC_PARTITION_COUNT = 50;

  private Set<String> allTopicsInClusterBeforeTest;

  @BeforeClass
  public static void start() {}

  @Before
  public void setUp() throws InterruptedException {
    initMocks(this);
    when(mockKafkaConfig.getBrokers()).thenReturn(TEST_KAFKA_BOOTSTRAP_SERVERS);
    when(mockZookeeperConfig.getUris()).thenReturn(TEST_ZK);
    //    when(mockZookeeperUtils.createKafkaZkClient())
    //        .thenReturn(
    //            KafkaZkClient.apply(
    //                MOCK_ZK,
    //                false,
    //                5000,
    //                5000,
    //                Integer.MAX_VALUE,
    //                Time.SYSTEM,
    //                "kafka.zk.rest",
    //                "rest"));
    //    when(mockZookeeperUtils.getZkUtils())
    //        .thenReturn(
    //            new ZkUtils(
    //                new ZkClient(MOCK_ZK, 5000, 5000, ZKStringSerializer$.MODULE$),
    //                new ZkConnection(MOCK_ZK),
    //                false));

    mockKafkaUtils.setKafkaConfig(mockKafkaConfig);
    mockZookeeperUtils.setZookeeperConfig(mockZookeeperConfig);

    clean();
    allTopicsInClusterBeforeTest = kafkaAdminServiceUnderTest.getAllTopics();
  }

  private void clean() throws InterruptedException {
    // Delete test topics
    kafkaAdminServiceUnderTest.deleteTopicList(
        Arrays.asList(FIRST_TOPIC_TO_TEST, SECOND_TOPIC_TO_TEST));

    // Delete test consumers
    if (kafkaAdminServiceUnderTest.isNewConsumerGroup(FIRST_CONSUMER_GROUP_TO_TEST)) {
      kafkaAdminServiceUnderTest.deleteConsumerGroup(
          FIRST_CONSUMER_GROUP_TO_TEST, ConsumerType.NEW);
    }
    if (kafkaAdminServiceUnderTest.isNewConsumerGroup(SECOND_CONSUMER_GROUP_TO_TEST)) {
      kafkaAdminServiceUnderTest.deleteConsumerGroup(
          SECOND_CONSUMER_GROUP_TO_TEST, ConsumerType.NEW);
    }
    Thread.sleep(1000);
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
    //    clean();
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

    //Create first topic again
    final Map<String, GeneralResponse> createTopicExistResult =
        kafkaAdminServiceUnderTest.createTopic(topicListToCreate);

    assertEquals(GeneralResponseState.success, createTopicResult.get(FIRST_TOPIC_TO_TEST).getState());
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

    System.out.println("////factor:" + replicationFactor);
    //Create first topic with replicator that is more than broker count
    TopicDetail firstTopic =
        generateTopicDetail(FIRST_TOPIC_TO_TEST, paritionCount, replicationFactor);

    // Create firsttopic
    topicListToCreate.add(firstTopic);
    final Map<String, GeneralResponse> createTopicResult =
        kafkaAdminServiceUnderTest.createTopic(topicListToCreate);

    assertEquals(
        GeneralResponseState.failure, createTopicResult.get(FIRST_TOPIC_TO_TEST).getState());
    assertTrue(
        createTopicResult.get(FIRST_TOPIC_TO_TEST).getMsg().contains("InvalidReplicationFactorException"));
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
    createTwoTopics();

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

    public ConsumerGroup(
        String groupId, int numConsumers, List<String> clientIdList, List<String> topicList) {
      consumers = new ArrayList<>(numConsumers);
      for (int i = 0; i < numConsumers; i++) {
        ConsumerRunnable consumerRunnable =
            new ConsumerRunnable(groupId, clientIdList.get(i), topicList);
        consumers.add(consumerRunnable);
      }
    }

    public void execute() {
      for (ConsumerRunnable consumerRunnable : consumers) {
        new Thread(consumerRunnable).start();
      }
    }

    public void close() {
      for (ConsumerRunnable consumerRunnable : consumers) {
        consumerRunnable.close();
      }
    }
  }

  private KafkaConsumer consumer(
      String consumerGroup, String clientId, List<String> subscribedTopicList) {
    KafkaConsumer kafkaConsumer =
        mockKafkaUtils.createNewConsumerByClientId(consumerGroup, clientId);
    kafkaConsumer.subscribe(subscribedTopicList);
    kafkaConsumer.poll(1000);
    kafkaConsumer.commitSync();
    return kafkaConsumer;
  }

  private KafkaConsumer consumer(String consumerGroup, List<String> subscribedTopicList) {
    KafkaConsumer kafkaConsumer = mockKafkaUtils.createNewConsumer(consumerGroup);
    kafkaConsumer.subscribe(subscribedTopicList);
    kafkaConsumer.poll(3000);
    kafkaConsumer.commitSync();
    return kafkaConsumer;
  }

  @Test
  public void testListAllConsumerGroups() {
    // Setup, just test the new consumer groups
    final ConsumerType type = ConsumerType.NEW;

    String consumerGroup = FIRST_CONSUMER_GROUP_TO_TEST;

    KafkaConsumer kafkaConsumer = consumer(consumerGroup, Arrays.asList(GROUP_METADATA_TOPIC_NAME));

    // Run the test
    final Map<String, Set<String>> consumerGroups =
        kafkaAdminServiceUnderTest.listAllConsumerGroups(type);

    Set<String> newConsumerGroups = consumerGroups.get("new");

    // Verify the results
    assertTrue(newConsumerGroups.contains(consumerGroup));

    kafkaConsumer.close();
  }

  @Test
  public void testListConsumerGroupsByTopic() {
    // Setup
    final String topic = GROUP_METADATA_TOPIC_NAME;
    final ConsumerType type = ConsumerType.NEW;
    String testGroup1 = FIRST_CONSUMER_GROUP_TO_TEST;
    String testGroup2 = SECOND_CONSUMER_GROUP_TO_TEST;

    List<String> subscribedTopicList = Arrays.asList(GROUP_METADATA_TOPIC_NAME);
    KafkaConsumer kafkaConsumer1 = consumer(testGroup1, subscribedTopicList);
    KafkaConsumer kafkaConsumer2 = consumer(testGroup2, subscribedTopicList);

    // Run the test
    final Map<String, Set<String>> consumerGroupsByTopic =
        kafkaAdminServiceUnderTest.listConsumerGroupsByTopic(topic, type);

    // Verify the results
    Set<String> newConsumerGroupsByTopic = consumerGroupsByTopic.get("new");
    assertTrue(newConsumerGroupsByTopic.contains(testGroup1));
    assertTrue(newConsumerGroupsByTopic.contains(testGroup2));

    kafkaConsumer1.close();
    kafkaConsumer2.close();
  }

  private void createOneTopic() {
    List<TopicDetail> topicListToCreate = new ArrayList<>();
    TopicDetail firstTopic = generateTopicDetail(FIRST_TOPIC_TO_TEST, 2, 1);
    topicListToCreate.add(firstTopic);

    kafkaAdminServiceUnderTest.createTopic(topicListToCreate);
  }

  private void createTwoTopics() {
    List<TopicDetail> topicListToCreate = new ArrayList<>();
    TopicDetail firstTopic = generateTopicDetail(FIRST_TOPIC_TO_TEST, 2, 1);
    TopicDetail secondTopic = generateTopicDetail(SECOND_TOPIC_TO_TEST, 2, 1);
    topicListToCreate.add(firstTopic);
    topicListToCreate.add(secondTopic);

    kafkaAdminServiceUnderTest.createTopic(topicListToCreate);
  }

  @Test
  public void testListTopicsByConsumerGroup() {
    // Setup
    final String consumerGroup = FIRST_CONSUMER_GROUP_TO_TEST;
    final ConsumerType type = ConsumerType.NEW;

    // Create two topics
    createTwoTopics();

    List<String> subscribedTopicList = Arrays.asList(FIRST_TOPIC_TO_TEST, SECOND_TOPIC_TO_TEST);
    KafkaConsumer kafkaConsumer = consumer(consumerGroup, subscribedTopicList);

    // Run the test
    final Set<String> topicsByConsumerGroup =
        kafkaAdminServiceUnderTest.listTopicsByConsumerGroup(consumerGroup, type);

    kafkaConsumer.close();
    // Verify the results
    assertEquals(2, topicsByConsumerGroup.size());
    assertTrue(topicsByConsumerGroup.contains(FIRST_TOPIC_TO_TEST));
    assertTrue(topicsByConsumerGroup.contains(SECOND_TOPIC_TO_TEST));
  }

  @Test
  public void testGetConsumerGroupMeta() throws InterruptedException {
    // Setup
    final String consumerGroup = FIRST_CONSUMER_GROUP_TO_TEST;
    createTwoTopics();

    final int numConsumers = 2;
    ConsumerGroup group =
        new ConsumerGroup(
            consumerGroup,
            numConsumers,
            Arrays.asList(FIRST_CONSUMER_CLIENT_TO_TEST, SECOND_CONSUMER_CLIENT_TO_TEST),
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
    String memberClientId2 = members.get(1).getClientId();
    assertTrue(
        memberClientId1.equals(FIRST_CONSUMER_CLIENT_TO_TEST)
            || memberClientId1.equals(SECOND_CONSUMER_CLIENT_TO_TEST));
    assertTrue(
        memberClientId2.equals(FIRST_CONSUMER_CLIENT_TO_TEST)
            || memberClientId2.equals(SECOND_CONSUMER_CLIENT_TO_TEST));

    group.close();
  }

  @Test
  public void testDescribeNewConsumerGroupWithoutFilter() throws InterruptedException {
    // Setup
    final String consumerGroup = FIRST_CONSUMER_GROUP_TO_TEST;
    final boolean filtered = false;

    //Create first topic with 2 partitions and 1 replica
    createOneTopic();

    //Create a group with only one consumer
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
    //Assign the consumer with 2 paritions, since there is only one consumer in the group
    assertEquals(2, partitionAssignments.size());
    PartitionAssignmentState assignment1 = partitionAssignments.get(0);
    PartitionAssignmentState assignment2 = partitionAssignments.get(1);
    assertEquals(consumerGroup, assignment1.getGroup());
    assertEquals(FIRST_TOPIC_TO_TEST, assignment1.getTopic());
    assertEquals(FIRST_CONSUMER_CLIENT_TO_TEST, assignment1.getClientId());
    assertTrue(assignment1.getPartition() == 0 || assignment1.getPartition() == 1);
    assertEquals(consumerGroup, assignment2.getGroup());
    assertEquals(FIRST_TOPIC_TO_TEST, assignment2.getTopic());
    assertEquals(FIRST_CONSUMER_CLIENT_TO_TEST, assignment2.getClientId());
    assertTrue(assignment2.getPartition() == 0 || assignment2.getPartition() == 1);

    group.close();
  }

  @Test
  public void testDescribeNewConsumerGroupWithFilterTopic() throws InterruptedException {
    // Setup
    final String consumerGroup = FIRST_CONSUMER_GROUP_TO_TEST;
    final boolean filtered = true;
    final String topic = SECOND_TOPIC_TO_TEST;

    //Create first and second topic
    createTwoTopics();

    //Create a group with only one consumer
    ConsumerGroup group =
        new ConsumerGroup(
            consumerGroup,
            1,
            Arrays.asList(FIRST_CONSUMER_CLIENT_TO_TEST, SECOND_CONSUMER_CLIENT_TO_TEST),
            Arrays.asList(FIRST_TOPIC_TO_TEST, SECOND_TOPIC_TO_TEST));
    group.execute();

    Thread.sleep(1000);

    // Run the test
    final List<PartitionAssignmentState> partitionAssignmentsFilteredByTopic =
        kafkaAdminServiceUnderTest.describeNewConsumerGroup(consumerGroup, filtered, topic);

    System.out.println("/////result: " + partitionAssignmentsFilteredByTopic);
    // Verify the results

    assertEquals(2, partitionAssignmentsFilteredByTopic.size());
    PartitionAssignmentState assignment1 = partitionAssignmentsFilteredByTopic.get(0);
    PartitionAssignmentState assignment2 = partitionAssignmentsFilteredByTopic.get(1);
    assertEquals(consumerGroup, assignment1.getGroup());
    assertEquals(topic, assignment1.getTopic());
    assertEquals(FIRST_CONSUMER_CLIENT_TO_TEST, assignment1.getClientId());
    assertTrue(assignment1.getPartition() == 0 || assignment1.getPartition() == 1);
    assertEquals(consumerGroup, assignment2.getGroup());
    assertEquals(topic, assignment2.getTopic());
    assertEquals(FIRST_CONSUMER_CLIENT_TO_TEST, assignment2.getClientId());
    assertTrue(assignment2.getPartition() == 0 || assignment2.getPartition() == 1);

    group.close();
  }

  /*
  @Test
  public void testDescribeLogDirsByBrokerAndTopic() {
    // Setup
    final List<Integer> brokerList = Arrays.asList();
    final List<String> topicList = Arrays.asList();
    final Map<Integer, Map<String, LogDirInfo>> expectedResult = new HashMap<>();

    // Run the test
    final Map<Integer, Map<String, LogDirInfo>> result =
        kafkaAdminServiceUnderTest.describeLogDirsByBrokerAndTopic(brokerList, topicList);

    // Verify the results
    assertEquals(expectedResult, result);
  }


    @Test
    public void testDescribeReplicaLogDirs() {
      // Setup
      final List<TopicPartitionReplica> replicas = Arrays.asList();
      final Map<TopicPartitionReplica, ReplicaLogDirInfo> expectedResult = new HashMap<>();

      // Run the test
      final Map<TopicPartitionReplica, ReplicaLogDirInfo> result =
          kafkaAdminServiceUnderTest.describeReplicaLogDirs(replicas);

      // Verify the results
      assertEquals(expectedResult, result);
    }

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
    public void testIsOldConsumerGroup() {
      // Setup
      final String consumerGroup = "consumerGroup";
      final boolean expectedResult = false;

      // Run the test
      final boolean result = kafkaAdminServiceUnderTest.isOldConsumerGroup(consumerGroup);

      // Verify the results
      assertEquals(expectedResult, result);
    }

    @Test
    public void testIsNewConsumerGroup() {
      // Setup
      final String consumerGroup = "consumerGroup";
      final boolean expectedResult = false;

      // Run the test
      final boolean result = kafkaAdminServiceUnderTest.isNewConsumerGroup(consumerGroup);

      // Verify the results
      assertEquals(expectedResult, result);
    }

    @Test
    public void testDescribeConsumerGroup() {
      // Setup
      final String consumerGroup = "consumerGroup";
      final ConsumerType type = null;
      final Map<String, List<ConsumerGroupDesc>> expectedResult = new HashMap<>();

      // Run the test
      final Map<String, List<ConsumerGroupDesc>> result =
          kafkaAdminServiceUnderTest.describeConsumerGroup(consumerGroup, type);

      // Verify the results
      assertEquals(expectedResult, result);
    }



    @Test
    public void testDescribeOldConsumerGroup() {
      // Setup
      final String consumerGroup = "consumerGroup";
      final boolean filtered = false;
      final String topic = "topic";
      final List<PartitionAssignmentState> expectedResult = Arrays.asList();

      // Run the test
      final List<PartitionAssignmentState> result =
          kafkaAdminServiceUnderTest.describeOldConsumerGroup(consumerGroup, filtered, topic);

      // Verify the results
      assertEquals(expectedResult, result);
    }

    @Test
    public void testDescribeNewConsumerGroupByTopic() {
      // Setup
      final String consumerGroup = "consumerGroup";
      final String topic = "topic";
      final List<ConsumerGroupDesc> expectedResult = Arrays.asList();

      // Run the test
      final List<ConsumerGroupDesc> result =
          kafkaAdminServiceUnderTest.describeNewConsumerGroupByTopic(consumerGroup, topic);

      // Verify the results
      assertEquals(expectedResult, result);
    }

    @Test
    public void testDescribeOldConsumerGroupByTopic() {
      // Setup
      final String consumerGroup = "consumerGroup";
      final String topic = "topic";
      final List<ConsumerGroupDesc> expectedResult = Arrays.asList();

      // Run the test
      final List<ConsumerGroupDesc> result =
          kafkaAdminServiceUnderTest.describeOldConsumerGroupByTopic(consumerGroup, topic);

      // Verify the results
      assertEquals(expectedResult, result);
    }

    @Test
    public void testAddPartitions() {
      // Setup
      final List<AddPartition> addPartitions = Arrays.asList();
      final Map<String, GeneralResponse> expectedResult = new HashMap<>();

      // Run the test
      final Map<String, GeneralResponse> result =
          kafkaAdminServiceUnderTest.addPartitions(addPartitions);

      // Verify the results
      assertEquals(expectedResult, result);
    }

    @Test
    public void testGenerateReassignPartition() {
      // Setup
      final ReassignWrapper reassignWrapper = null;
      final List<String> expectedResult = Arrays.asList();

      // Run the test
      final List<String> result =
          kafkaAdminServiceUnderTest.generateReassignPartition(reassignWrapper);

      // Verify the results
      assertEquals(expectedResult, result);
    }

    @Test
    public void testExecuteReassignPartition() {
      // Setup
      final String reassignStr = "reassignStr";
      final Long interBrokerThrottle = 0L;
      final Long replicaAlterLogDirsThrottle = 0L;
      final Long timeoutMs = 0L;
      final Map<String, Object> expectedResult = new HashMap<>();

      // Run the test
      final Map<String, Object> result =
          kafkaAdminServiceUnderTest.executeReassignPartition(
              reassignStr, interBrokerThrottle, replicaAlterLogDirsThrottle, timeoutMs);

      // Verify the results
      assertEquals(expectedResult, result);
    }

    @Test
    public void testCheckReassignStatusByStr() {
      // Setup
      final String reassignStr = "reassignStr";
      final Map<String, Object> expectedResult = new HashMap<>();

      // Run the test
      final Map<String, Object> result =
          kafkaAdminServiceUnderTest.checkReassignStatusByStr(reassignStr);

      // Verify the results
      assertEquals(expectedResult, result);
    }

    @Test
    public void testGetMessage() {
      // Setup
      final String topic = "topic";
      final int partition = 0;
      final long offset = 0L;
      final String decoder = "decoder";
      final String avroSchema = "avroSchema";
      final String expectedResult = "result";

      // Run the test
      final String result =
          kafkaAdminServiceUnderTest.getMessage(topic, partition, offset, decoder, avroSchema);

      // Verify the results
      assertEquals(expectedResult, result);
    }

    @Test
    public void testGetRecordByOffset() {
      // Setup
      final String topic = "topic";
      final int partition = 0;
      final long offset = 0L;
      final String decoder = "decoder";
      final String avroSchema = "avroSchema";
      final Record expectedResult = null;

      // Run the test
      final Record result =
          kafkaAdminServiceUnderTest.getRecordByOffset(topic, partition, offset, decoder, avroSchema);

      // Verify the results
      assertEquals(expectedResult, result);
    }

    @Test
    public void testResetOffset() {
      // Setup
      final String topic = "topic";
      final int partition = 0;
      final String consumerGroup = "consumerGroup";
      final ConsumerType type = null;
      final String offset = "offset";
      final GeneralResponse expectedResult = null;

      // Run the test
      final GeneralResponse result =
          kafkaAdminServiceUnderTest.resetOffset(topic, partition, consumerGroup, type, offset);

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

    @Test
    public void testDeleteConsumerGroup() {
      // Setup
      final String consumerGroup = "consumerGroup";
      final ConsumerType type = null;
      final GeneralResponse expectedResult = null;

      // Run the test
      final GeneralResponse result =
          kafkaAdminServiceUnderTest.deleteConsumerGroup(consumerGroup, type);

      // Verify the results
      assertEquals(expectedResult, result);
    }

    @Test
    public void testGetBeginningOffset() {
      // Setup
      final String topic = "topic";
      final int partitionId = 0;
      final long expectedResult = 0L;

      // Run the test
      final long result = kafkaAdminServiceUnderTest.getBeginningOffset(topic, partitionId);

      // Verify the results
      assertEquals(expectedResult, result);
    }

    @Test
    public void testGetEndOffset() {
      // Setup
      final String topic = "topic";
      final int partitionId = 0;
      final long expectedResult = 0L;

      // Run the test
      final long result = kafkaAdminServiceUnderTest.getEndOffset(topic, partitionId);

      // Verify the results
      assertEquals(expectedResult, result);
    }

    @Test
    public void testCountPartition() {
      // Setup
      final String topic = "topic";
      final Map<Integer, Long> expectedResult = new HashMap<>();

      // Run the test
      final Map<Integer, Long> result = kafkaAdminServiceUnderTest.countPartition(topic);

      // Verify the results
      assertEquals(expectedResult, result);
    }

    @Test
    public void testHealthCheck() {
      // Setup
      final HealthCheckResult expectedResult = null;

      // Run the test
      final HealthCheckResult result = kafkaAdminServiceUnderTest.healthCheck();

      // Verify the results
      assertEquals(expectedResult, result);
    }
    */
}
