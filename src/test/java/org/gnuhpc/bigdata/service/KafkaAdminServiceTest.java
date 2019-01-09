package org.gnuhpc.bigdata.service;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import kafka.server.KafkaServerStartable;
import org.gnuhpc.bigdata.componet.OffsetStorage;
import org.gnuhpc.bigdata.config.KafkaConfig;
import org.gnuhpc.bigdata.config.ZookeeperConfig;
import org.gnuhpc.bigdata.constant.GeneralResponseState;
import org.gnuhpc.bigdata.model.GeneralResponse;
import org.gnuhpc.bigdata.model.TopicBrief;
import org.gnuhpc.bigdata.model.TopicDetail;
import org.gnuhpc.bigdata.model.TopicMeta;
import org.gnuhpc.bigdata.utils.KafkaStarterUtils;
import org.gnuhpc.bigdata.utils.KafkaUtils;
import org.gnuhpc.bigdata.utils.ZkStarter;
import org.gnuhpc.bigdata.utils.ZookeeperUtils;
import org.junit.AfterClass;
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

  @Mock private KafkaConfig mockKafkaConfig;
  @Mock private ZookeeperConfig mockZookeeperConfig;
  @Spy private ZookeeperUtils mockZookeeperUtils = new ZookeeperUtils();
  @Spy private KafkaUtils mockKafkaUtils = new KafkaUtils();
  @Mock private OffsetStorage mockStorage;

  @InjectMocks private KafkaAdminService kafkaAdminServiceUnderTest;

  private static KafkaServerStartable kafkaStarter;

  private final String firstTopicName = "firsttopic";
  private final String secondTopicName = "secondtopic";
  private final String nonExistTopicName = "nontopic";

  @BeforeClass
  public static void start() {
    ZkStarter.startLocalZkServer();
    kafkaStarter =
        KafkaStarterUtils.startServer(
            KafkaStarterUtils.DEFAULT_KAFKA_PORT,
            KafkaStarterUtils.DEFAULT_BROKER_ID,
            KafkaStarterUtils.DEFAULT_ZK_STR,
            KafkaStarterUtils.getDefaultKafkaConfiguration());
  }

  @Before
  public void setUp() {
    initMocks(this);

    String MOCK_KAFKA_BOOTSTRAP_SERVERS = KafkaStarterUtils.DEFAULT_KAFKA_BROKER;
    String MOCK_ZK = ZkStarter.DEFAULT_ZK_STR;

    when(mockKafkaConfig.getBrokers()).thenReturn(MOCK_KAFKA_BOOTSTRAP_SERVERS);
    when(mockZookeeperConfig.getUris()).thenReturn(MOCK_ZK);

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
  }

  @AfterClass
  public static void stop() {
    kafkaStarter.shutdown();
    ZkStarter.stopLocalZkServer();
  }

  @Test
  public void testListTopics() {
    // Setup
    final List<String> expectedResult = Arrays.asList();

    // Run the test
    final List<String> result = kafkaAdminServiceUnderTest.listTopics();

    // Verify the results
    assertEquals(expectedResult, result);
  }

  @Test
  public void testCreateTopic() {
    final List<String> allTopicsBeforeCreate = kafkaAdminServiceUnderTest.listTopics();
    assertTrue(allTopicsBeforeCreate.isEmpty());

    List<TopicDetail> topicList = new ArrayList<>();
    // Create first topic by topic name, partition count, replication factor
    final int partitionCount = 3;
    final int replicationFactor = 1;
    final TopicDetail topic =
        TopicDetail.builder()
            .name(firstTopicName)
            .partitions(partitionCount)
            .factor(replicationFactor)
            .build();

    // Create second topic by replica assignment
    HashMap<Integer, List<Integer>> replicaAssignments = new HashMap<>();
    replicaAssignments.put(0, Arrays.asList(KafkaStarterUtils.DEFAULT_BROKER_ID));
    TopicDetail topic2 =
        TopicDetail.builder().name(secondTopicName).replicasAssignments(replicaAssignments).build();

    topicList.add(topic);
    topicList.add(topic2);

    // Run the test
    final Map<String, GeneralResponse> result = kafkaAdminServiceUnderTest.createTopic(topicList);

    // Verify the first topic result
    TopicMeta firstTopicMeta = (TopicMeta) result.get(firstTopicName).getData();
    assertEquals(GeneralResponseState.success, result.get(firstTopicName).getState());
    assertEquals(firstTopicName, firstTopicMeta.getTopicName());
    assertEquals(partitionCount, firstTopicMeta.getPartitionCount());
    assertEquals(replicationFactor, firstTopicMeta.getReplicationFactor());

    // Verify the second topic result
    TopicMeta secondTopicMeta = (TopicMeta) result.get(secondTopicName).getData();
    assertEquals(GeneralResponseState.success, result.get(secondTopicName).getState());
    assertEquals(secondTopicName, secondTopicMeta.getTopicName());
    assertEquals(1, secondTopicMeta.getPartitionCount());
    assertEquals(replicationFactor, secondTopicMeta.getReplicationFactor());
    assertEquals(
        KafkaStarterUtils.DEFAULT_BROKER_ID,
        secondTopicMeta
            .getTopicPartitionInfos()
            .get(0)
            .getTopicPartitionInfo()
            .replicas()
            .get(0)
            .id());
  }

  @Test
  public void testGetAllTopics() {
    // Setup
    final Set<String> expectedResult = new HashSet<>();

    // Run the test
    final Set<String> result = kafkaAdminServiceUnderTest.getAllTopics();

    final Set<String> allTopicsAfterCreate = kafkaAdminServiceUnderTest.getAllTopics();

    // Verify the results
    assertEquals(2, allTopicsAfterCreate.size());
    assertTrue(allTopicsAfterCreate.contains(firstTopicName));
    assertTrue(allTopicsAfterCreate.contains(secondTopicName));
  }

  @Test
  public void testListTopicBrief() {
    // Setup
    final List<TopicBrief> expectedResult = Arrays.asList();

    // Run the test
    final List<TopicBrief> result = kafkaAdminServiceUnderTest.listTopicBrief();

    // Verify the results
//    assertEquals(expectedResult, result);
  }

  @Test
  public void testExistTopic() {
    // Setup
    final boolean expectedResult = false;

    // Run the test
//    final boolean firstTopicExist = kafkaAdminServiceUnderTest.existTopic(firstTopicName);
//    final boolean nonExistTopicExist = kafkaAdminServiceUnderTest.existTopic(nonExistTopicName);
//
//    // Verify the results
//    assertTrue(firstTopicExist);
//    assertFalse(nonExistTopicExist);
  }

  @Test
  public void testDescribeCluster() {
    // Setup
    final Map<String, Object> expectedResult = new HashMap<>();

    // Run the test
    final Map<String, Object> result = kafkaAdminServiceUnderTest.describeCluster();

    System.out.println("result:" + result);
    // Verify the results
    assertEquals(1, result.size());
  }
/*
  @Test
  public void testListBrokers() {
    // Setup
    final List<BrokerInfo> expectedResult = Arrays.asList();

    // Run the test
    final List<BrokerInfo> result = kafkaAdminServiceUnderTest.listBrokers();

    // Verify the results
    assertEquals(expectedResult, result);
  }

  @Test
  public void testGetControllerId() {
    // Setup
    final int expectedResult = 0;

    // Run the test
    final int result = kafkaAdminServiceUnderTest.getControllerId();

    // Verify the results
    assertEquals(expectedResult, result);
  }

  @Test
  public void testListLogDirsByBroker() {
    // Setup
    final List<Integer> brokerList = Arrays.asList();
    final Map<Integer, List<String>> expectedResult = new HashMap<>();

    // Run the test
    final Map<Integer, List<String>> result =
        kafkaAdminServiceUnderTest.listLogDirsByBroker(brokerList);

    // Verify the results
    assertEquals(expectedResult, result);
  }

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
  public void testGetBrokerConf() {
    // Setup
    final int brokerId = 0;
    final Collection<CustomConfigEntry> expectedResult = Arrays.asList();

    // Run the test
    final Collection<CustomConfigEntry> result = kafkaAdminServiceUnderTest.getBrokerConf(brokerId);

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
  public void testDescribeTopic() {
    // Setup
    final String topicName = "topicName";
    final TopicMeta expectedResult = null;

    // Run the test
    final TopicMeta result = kafkaAdminServiceUnderTest.describeTopic(topicName);

    // Verify the results
    assertEquals(expectedResult, result);
  }

  @Test
  public void testDeleteTopicList() {
    // Setup
    final List<String> topicList = Arrays.asList();
    final Map<String, GeneralResponse> expectedResult = new HashMap<>();

    // Run the test
    final Map<String, GeneralResponse> result =
        kafkaAdminServiceUnderTest.deleteTopicList(topicList);

    // Verify the results
    assertEquals(expectedResult, result);
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
  public void testListAllConsumerGroups() {
    // Setup
    final ConsumerType type = null;
    final Map<String, Set<String>> expectedResult = new HashMap<>();

    // Run the test
    final Map<String, Set<String>> result = kafkaAdminServiceUnderTest.listAllConsumerGroups(type);

    // Verify the results
    assertEquals(expectedResult, result);
  }

  @Test
  public void testListConsumerGroupsByTopic() {
    // Setup
    final String topic = "topic";
    final ConsumerType type = null;
    final Map<String, Set<String>> expectedResult = new HashMap<>();

    // Run the test
    final Map<String, Set<String>> result =
        kafkaAdminServiceUnderTest.listConsumerGroupsByTopic(topic, type);

    // Verify the results
    assertEquals(expectedResult, result);
  }

  @Test
  public void testListTopicsByConsumerGroup() {
    // Setup
    final String consumerGroup = "consumerGroup";
    final ConsumerType type = null;
    final Set<String> expectedResult = new HashSet<>();

    // Run the test
    final Set<String> result =
        kafkaAdminServiceUnderTest.listTopicsByConsumerGroup(consumerGroup, type);

    // Verify the results
    assertEquals(expectedResult, result);
  }

  @Test
  public void testGetConsumerGroupMeta() {
    // Setup
    final String consumerGroup = "consumerGroup";
    final ConsumerGroupMeta expectedResult = null;

    // Run the test
    final ConsumerGroupMeta result = kafkaAdminServiceUnderTest.getConsumerGroupMeta(consumerGroup);

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
  public void testDescribeNewConsumerGroup() {
    // Setup
    final String consumerGroup = "consumerGroup";
    final boolean filtered = false;
    final String topic = "topic";
    final List<PartitionAssignmentState> expectedResult = Arrays.asList();

    // Run the test
    final List<PartitionAssignmentState> result =
        kafkaAdminServiceUnderTest.describeNewConsumerGroup(consumerGroup, filtered, topic);

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
