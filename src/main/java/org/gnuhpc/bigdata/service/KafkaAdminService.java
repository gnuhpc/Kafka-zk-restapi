package org.gnuhpc.bigdata.service;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import kafka.admin.AdminClient;
import kafka.admin.AdminClient.ConsumerGroupSummary;
import kafka.admin.AdminClient.ConsumerSummary;
import kafka.admin.AdminUtils;
import kafka.admin.ConsumerGroupCommand;
import kafka.admin.ReassignPartitionsCommand;
import kafka.admin.ReassignPartitionsCommand.Throttle;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.coordinator.group.GroupOverview;
import kafka.coordinator.group.GroupTopicPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.log.LogConfig;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import lombok.extern.log4j.Log4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DescribeLogDirsResponse.LogDirInfo;
import org.apache.kafka.common.serialization.Serdes;
import org.gnuhpc.bigdata.CollectionConvertor;
import org.gnuhpc.bigdata.componet.OffsetStorage;
import org.gnuhpc.bigdata.config.KafkaConfig;
import org.gnuhpc.bigdata.constant.ConsumerGroupState;
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
import org.gnuhpc.bigdata.model.TwoTuple;
import org.gnuhpc.bigdata.utils.KafkaUtils;
import org.gnuhpc.bigdata.utils.ZookeeperUtils;
import org.gnuhpc.bigdata.validator.ConsumerGroupExistConstraint;
import org.gnuhpc.bigdata.validator.TopicExistConstraint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;
import scala.Function0;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.mutable.ListBuffer;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

/** Created by gnuhpc on 2017/7/17. */
@Service
@Log4j
@Validated
public class KafkaAdminService {

  private static final int channelSocketTimeoutMs = 600;
  private static final int channelRetryBackoffMs = 600;
  private static final long kafkaAdminClientGetTimeoutMs = 120000;
  private static final long kafkaAdminClientAlterTimeoutMs = 120000;
  private static final String CONSUMERPATHPREFIX = "/consumers/";
  private static final String OFFSETSPATHPREFIX = "/offsets/";
  public static final String LeaderReplicationThrottledRateProp =
      "leader.replication.throttled.rate";
  public static final String FollowerReplicationThrottledRateProp =
      "follower.replication.throttled.rate";
  public static final String ReplicaAlterLogDirsIoMaxBytesPerSecondProp =
      "replica.alter.log.dirs.io.max.bytes.per.second";

  @Autowired private ZookeeperUtils zookeeperUtils;

  @Autowired private KafkaUtils kafkaUtils;

  @Autowired private KafkaConfig kafkaConfig;

  @Autowired private OffsetStorage storage;

  // For AdminUtils use
  private ZkUtils zkUtils;

  private org.apache.kafka.clients.admin.AdminClient kafkaAdminClient = null;

  private AdminClient oldAdminClient = null;

  private scala.Option<String> none = scala.Option.apply(null);

  @PostConstruct
  private void init() {}

  private org.apache.kafka.clients.admin.AdminClient createKafkaAdminClient() {
    if (this.kafkaAdminClient == null) {
      Properties adminClientProp = new Properties();
      adminClientProp.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBrokers());
      this.kafkaAdminClient = KafkaAdminClient.create(adminClientProp);
    }

    return this.kafkaAdminClient;
  }

  public HashMap<String, GeneralResponse> createTopic(List<TopicDetail> topicList) {
    List<NewTopic> newTopicList = new ArrayList<>();
    HashMap<String, GeneralResponse> createResults = new HashMap<>();

    for (TopicDetail topic : topicList) {
      NewTopic newTopic;
      Map<Integer, List<Integer>> replicasAssignments = topic.getReplicasAssignments();

      try {
        Topic.validate(topic.getName());

        if (Topic.hasCollisionChars(topic.getName())) {
          throw new InvalidTopicException("Invalid topic name, it contains '.' or '_'.");
        }
      } catch (Exception exception) {
        GeneralResponse generalResponse =
            GeneralResponse.builder()
                .state(GeneralResponseState.failure)
                .msg(exception.getMessage())
                .build();
        createResults.put(topic.getName(), generalResponse);
        continue;
      }

      if (replicasAssignments != null && !replicasAssignments.isEmpty()) {
        newTopic = new NewTopic(topic.getName(), replicasAssignments);
      } else {
        newTopic = new NewTopic(topic.getName(), topic.getPartitions(), (short) topic.getFactor());
      }

      if (topic.getProp() != null) {
        newTopic.configs((Map) topic.getProp());
      }

      newTopicList.add(newTopic);
    }

    org.apache.kafka.clients.admin.AdminClient kafkaAdminClient = createKafkaAdminClient();

    CreateTopicsOptions createTopicsOptions = new CreateTopicsOptions();
    createTopicsOptions.timeoutMs((int) kafkaAdminClientAlterTimeoutMs);
    CreateTopicsResult createTopicsResult =
        kafkaAdminClient.createTopics(newTopicList, createTopicsOptions);

    try {
      createTopicsResult.all().get(kafkaAdminClientGetTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception exception) {
      log.warn("Create topic exception:" + exception);
    } finally {
      createTopicsResult
          .values()
          .forEach(
              (topicName, result) -> {
                GeneralResponse generalResponse;
                if (result.isDone() && !result.isCompletedExceptionally()) {
                  TopicMeta topicMeta = describeTopic(topicName);
                  generalResponse =
                      GeneralResponse.builder()
                          .state(GeneralResponseState.success)
                          .data(topicMeta)
                          .build();
                } else {
                  generalResponse =
                      GeneralResponse.builder()
                          .state(GeneralResponseState.failure)
                          .msg(result.toString())
                          .build();
                }
                createResults.put(topicName, generalResponse);
              });
    }

    return createResults;
  }

  public List<String> listTopics() {
    List<String> topicNamesList = new ArrayList<String>();
    topicNamesList.addAll(getAllTopics());

    return topicNamesList;
  }

  public Set<String> getAllTopics() {
    Set<String> topicNames;
    org.apache.kafka.clients.admin.AdminClient kafkaAdminClient = createKafkaAdminClient();
    ListTopicsOptions options = new ListTopicsOptions();
    // includes internal topics such as __consumer_offsets
    options.listInternal(true);

    ListTopicsResult topics = kafkaAdminClient.listTopics(options);
    try {
      topicNames = topics.names().get(kafkaAdminClientGetTimeoutMs, TimeUnit.MILLISECONDS);
      log.info("Current topics in this cluster: " + topicNames);
    } catch (Exception exception) {
      log.warn("List topic exception : " + exception);
      throw new ApiException("List topic exception : " + exception);
    }

    return topicNames;
  }

  public List<TopicBrief> listTopicBrief() {
    org.apache.kafka.clients.admin.AdminClient kafkaAdminClient = createKafkaAdminClient();

    DescribeTopicsResult describeTopicsResult = kafkaAdminClient.describeTopics(listTopics());
    Map<String, TopicDescription> topicMap;
    List<TopicBrief> result;
    try {
      topicMap =
          describeTopicsResult.all().get(kafkaAdminClientGetTimeoutMs, TimeUnit.MILLISECONDS);
      result =
          topicMap
              .entrySet()
              .parallelStream()
              .map(
                  e -> {
                    String topic = e.getKey();
                    TopicDescription topicDescription = e.getValue();
                    List<org.apache.kafka.common.TopicPartitionInfo> topicPartitionInfoList =
                        topicDescription.partitions();
                    int replicateCount = 0;
                    int isrCount = 0;
                    for (org.apache.kafka.common.TopicPartitionInfo topicPartitionInfo :
                        topicPartitionInfoList) {
                      replicateCount += topicPartitionInfo.replicas().size();
                      isrCount += topicPartitionInfo.isr().size();
                    }
                    if (replicateCount == 0) {
                      return new TopicBrief(topic, topicDescription.partitions().size(), 0, replicateCount);
                    } else {
                      return new TopicBrief(
                          topic,
                          topicDescription.partitions().size(),
                          ((double) isrCount / replicateCount), replicateCount);
                    }
                  })
              .collect(toList());
    } catch (Exception exception) {
      log.warn("Describe all topics exception:" + exception);
      throw new ApiException("Describe all topics exception:" + exception);
    }

    return result;
  }

  public boolean existTopic(String topicName) {
    KafkaZkClient kafkaZkClient = zookeeperUtils.createKafkaZkClient();
    boolean exists = kafkaZkClient.topicExists(topicName);

    return exists;
  }

  public ClusterInfo describeCluster() {
    //    Map<String, Object> clusterDetail = new HashMap<>();
    ClusterInfo clusterInfo = new ClusterInfo();

    org.apache.kafka.clients.admin.AdminClient kafkaAdminClient = createKafkaAdminClient();
    DescribeClusterOptions describeClusterOptions =
        new DescribeClusterOptions().timeoutMs((int) kafkaAdminClientGetTimeoutMs);

    DescribeClusterResult describeClusterResult =
        kafkaAdminClient.describeCluster(describeClusterOptions);

    KafkaFuture<String> clusterIdFuture = describeClusterResult.clusterId();
    KafkaFuture<Node> controllerFuture = describeClusterResult.controller();
    KafkaFuture<Collection<Node>> nodesFuture = describeClusterResult.nodes();
    String clusterId = "";
    Node controller = null;
    Collection<Node> nodes = new ArrayList<>();

    try {
      clusterId = clusterIdFuture.get(kafkaAdminClientGetTimeoutMs, TimeUnit.MILLISECONDS);
      controller = controllerFuture.get(kafkaAdminClientGetTimeoutMs, TimeUnit.MILLISECONDS);
      nodes = nodesFuture.get(kafkaAdminClientGetTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception exception) {
      log.warn("Describe cluster exception:" + exception);
      throw new ApiException("Describe cluster exception:" + exception);
    } finally {
      if (clusterIdFuture.isDone() && !clusterIdFuture.isCompletedExceptionally()) {
        //        clusterDetail.put("clusterId", clusterId);
        clusterInfo.setClusterId(clusterId);
      }
      if (controllerFuture.isDone() && !controllerFuture.isCompletedExceptionally()) {
        //        clusterDetail.put("controllerId", controller);
        clusterInfo.setController(controller);
      }
      if (nodesFuture.isDone() && !nodesFuture.isCompletedExceptionally()) {
        //        clusterDetail.put("nodes", nodes);
        clusterInfo.setNodes(nodes);
      }
    }

    return clusterInfo;
  }

  public List<BrokerInfo> listBrokers() {
    CuratorFramework zkClient = zookeeperUtils.createZkClient();
    KafkaZkClient kafkaZkClient = zookeeperUtils.createKafkaZkClient();
    List<Broker> brokerList =
        CollectionConvertor.seqConvertJavaList(kafkaZkClient.getAllBrokersInCluster());

    List<BrokerInfo> brokerInfoList =
        brokerList
            .parallelStream()
            .collect(Collectors.toMap(Broker::id, Broker::rack))
            .entrySet()
            .parallelStream()
            .map(
                entry -> {
                  String brokerInfoStr = null;
                  try {
                    // TODO replace zkClient with kafkaZKClient
                    brokerInfoStr =
                        new String(
                            zkClient
                                .getData()
                                .forPath(ZkUtils.BrokerIdsPath() + "/" + entry.getKey()));
                  } catch (Exception e) {
                    e.printStackTrace();
                  }
                  BrokerInfo brokerInfo;
                  try {
                    ObjectMapper objectMapper = new ObjectMapper();
                    brokerInfo = objectMapper.readValue(brokerInfoStr, BrokerInfo.class);
                  } catch (Exception exception) {
                    throw new ApiException("List broker exception." + exception);
                  }
                  if (entry.getValue().isEmpty()) {
                    brokerInfo.setRack("");
                  } else {
                    brokerInfo.setRack(entry.getValue().get());
                  }
                  brokerInfo.setId(entry.getKey());
                  return brokerInfo;
                })
            .collect(toList());

    return brokerInfoList;
  }

  public Node getController() {
    return describeCluster().getController();
  }

  public Map<Integer, List<String>> listLogDirsByBroker(List<Integer> brokerList) {
    Map<Integer, List<String>> logDirList = new HashMap<>();

    Map<Integer, Map<String, LogDirInfo>> logDirInfosByBroker =
        describeLogDirsByBrokerAndTopic(brokerList, null,null);
    logDirInfosByBroker
        .entrySet()
        .forEach(
            e -> {
              List<String> dirList = e.getValue().keySet().stream().collect(Collectors.toList());
              Collections.sort(dirList);
              logDirList.put(e.getKey(), dirList);
            });

    return logDirList;
  }

  public Map<Integer, Map<String, LogDirInfo>> describeLogDirsByBrokerAndTopic(
      List<Integer> brokerList, List<String> logDirList, Map<String, List<Integer>> topicPartitionMap) {
    org.apache.kafka.clients.admin.AdminClient kafkaAdminClient = createKafkaAdminClient();

    List<Integer> brokerIdsInCluster =
        listBrokers().stream().map(brokerInfo -> brokerInfo.getId()).collect(Collectors.toList());

    if (brokerList == null || brokerList.isEmpty()) {
      brokerList = brokerIdsInCluster;
    } else {
      for (int brokerId : brokerList) {
        if (!brokerIdsInCluster.contains(brokerId)) {
          throw new ApiException("Bad Request. Broker Id:" + brokerId + " non-exist.");
        }
      }
    }
    // Delete reason: we want to delete the topic data that no longer found on zk, we can still use
    // this
    //    function to get the topic log dir
    //    if (topicList != null && !topicList.isEmpty()) {
    //      for (String topic : topicList) {
    //        if (!existTopic(topic)) {
    //          throw new ApiException("Bad Request. Topic:" + topic + " non-exist.");
    //        }
    //      }
    //    }

    DescribeLogDirsOptions describeLogDirsOptions =
        new DescribeLogDirsOptions().timeoutMs((int) kafkaAdminClientGetTimeoutMs);
    DescribeLogDirsResult describeLogDirsResult =
        kafkaAdminClient.describeLogDirs(brokerList, describeLogDirsOptions);
    Map<Integer, Map<String, LogDirInfo>> logDirInfosByBroker = new HashMap<>();

    try {
      logDirInfosByBroker =
          describeLogDirsResult.all().get(kafkaAdminClientGetTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception exception) {
      log.warn("Describe log dirs exception:" + exception);
      throw new ApiException("Describe log dirs exception:" + exception);
    } finally {
      if (logDirList != null && !logDirList.isEmpty()) {
        logDirInfosByBroker.entrySet().forEach(e->{
          e.getValue().entrySet().removeIf(m -> !logDirList.contains(m.getKey()));
        });
      }
      if (topicPartitionMap != null && !topicPartitionMap.isEmpty()) {
        logDirInfosByBroker
            .entrySet()
            .forEach(
                e -> {
                  e.getValue()
                      .entrySet()
                      .forEach(
                          m -> {
                            m.getValue()
                                .replicaInfos.entrySet()
                                .removeIf(t -> !topicPartitionMap.keySet().contains(t.getKey().topic()) ||
                                    (topicPartitionMap.get(t.getKey().topic()) != null &&
                                        !topicPartitionMap.get(t.getKey().topic()).contains(t.getKey().partition())));
                          });
                });
      }
    }

    return logDirInfosByBroker;
  }

  public Map<TopicPartitionReplica, ReplicaLogDirInfo> describeReplicaLogDirs(
      List<TopicPartitionReplica> replicas) {
    org.apache.kafka.clients.admin.AdminClient kafkaAdminClient = createKafkaAdminClient();
    Map<TopicPartitionReplica, ReplicaLogDirInfo> replicaLogDirInfoMap;

    DescribeReplicaLogDirsOptions describeReplicaLogDirsOptions =
        new DescribeReplicaLogDirsOptions().timeoutMs((int) kafkaAdminClientGetTimeoutMs);
    DescribeReplicaLogDirsResult describeReplicaLogDirsResult =
        kafkaAdminClient.describeReplicaLogDirs(replicas, describeReplicaLogDirsOptions);

    try {
      replicaLogDirInfoMap =
          describeReplicaLogDirsResult
              .all()
              .get(kafkaAdminClientGetTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception exception) {
      log.warn("Describe replica log dirs exception:" + exception);
      throw new ApiException("Describe replica log dirs exception:" + exception);
    }

    return replicaLogDirInfoMap;
  }

  public Collection<CustomConfigEntry> getBrokerConf(int brokerId) {
    String broker = String.valueOf(brokerId);
    Properties dynamicProps = getConfigInZk(Type.BROKER, broker);

    Collection<ConfigEntry> configs = describeConfig(Type.BROKER, String.valueOf(brokerId));

    return mergeConfigs(configs, dynamicProps);
  }

  public Properties getConfigInZk(ConfigResource.Type type, String name) {
    KafkaZkClient kafkaZkClient = zookeeperUtils.createKafkaZkClient();
    AdminZkClient adminZkClient = new AdminZkClient(kafkaZkClient);
    Properties properties = new Properties();

    if (type.equals(Type.BROKER)) {
      properties = adminZkClient.fetchEntityConfig(ConfigType.Broker(), name);
    } else if (type.equals(Type.TOPIC)) {
      properties = adminZkClient.fetchEntityConfig(ConfigType.Topic(), name);
    }

    return properties;
  }

  private Collection<CustomConfigEntry> mergeConfigs(
      Collection<ConfigEntry> configs, Properties dynamicProps) {
    Collection<CustomConfigEntry> mergedConfigs = new ArrayList<>();
    CustomConfigEntry newConfigEntry;

    for (ConfigEntry entry : configs) {
      String key = entry.name();
      String value = entry.value();
      if (dynamicProps.containsKey(key)) {
        value = dynamicProps.getProperty(key);
      }
      newConfigEntry =
          new CustomConfigEntry(
              key, value, entry.isSensitive(), entry.isReadOnly(), entry.source());
      mergedConfigs.add(newConfigEntry);
    }

    return mergedConfigs;
  }

  private void validateConfigs(
      Type type, String name, Properties props, List<String> keysToBeChecked) {
    for (Object configKey : keysToBeChecked) {
      if (!props.containsKey(configKey)) {
        throw new ApiException(type.name() + ":" + name + " has no such property:" + configKey);
      }
    }
  }

  public Properties updateBrokerDynConf(int brokerId, Properties propsToBeUpdated) {
    Properties props = getConfigInZk(Type.BROKER, String.valueOf(brokerId));
    KafkaZkClient kafkaZkClient = zookeeperUtils.createKafkaZkClient();
    AdminZkClient adminZkClient = new AdminZkClient(kafkaZkClient);

    for (String key : propsToBeUpdated.stringPropertyNames()) {
      if (props.containsKey(key)) {
        props.setProperty(key, propsToBeUpdated.getProperty(key));
      } else {
        props.put(key, propsToBeUpdated.getProperty(key));
      }
    }

    adminZkClient.changeBrokerConfig(
        JavaConverters.asScalaBufferConverter(Collections.singletonList((Object) brokerId))
            .asScala()
            .toSeq(),
        props);

    return getConfigInZk(Type.BROKER, String.valueOf(brokerId));
  }

  public void removeConfigInZk(Type type, String name, List<String> configKeysToBeRemoved) {
    KafkaZkClient kafkaZkClient = zookeeperUtils.createKafkaZkClient();
    AdminZkClient adminZkClient = new AdminZkClient(kafkaZkClient);

    Properties props = getConfigInZk(type, name);

    validateConfigs(Type.BROKER, String.valueOf(name), props, configKeysToBeRemoved);

    props.entrySet().removeIf(entry -> configKeysToBeRemoved.contains(entry.getKey()));

    if (type.equals(Type.BROKER)) {
      int brokerId = Integer.parseInt(name);
      adminZkClient.changeBrokerConfig(
          JavaConverters.asScalaBufferConverter(Collections.singletonList((Object) brokerId))
              .asScala()
              .toSeq(),
          props);
    } else if (type.equals(Type.TOPIC)) {
      adminZkClient.changeTopicConfig(name, props);
    }
  }

  public TopicDescription getTopicDescription(@TopicExistConstraint String topicName) {
    TopicDescription topicDescription = null;
    org.apache.kafka.clients.admin.AdminClient kafkaAdminClient = createKafkaAdminClient();

    DescribeTopicsResult describeTopicsResult =
        kafkaAdminClient.describeTopics(Collections.singletonList(topicName));
    try {
      Map<String, TopicDescription> topicMap =
          describeTopicsResult.all().get(kafkaAdminClientGetTimeoutMs, TimeUnit.MILLISECONDS);
      if (topicMap.containsKey(topicName)) {
        topicDescription = topicMap.get(topicName);
      }
    } catch (Exception exception) {
      log.info("Get topic description exception:" + exception);
    }
    return topicDescription;
  }

  public TopicMeta describeTopic(@TopicExistConstraint String topicName) {
//    org.apache.kafka.clients.admin.AdminClient kafkaAdminClient = createKafkaAdminClient();
//
//    DescribeTopicsResult describeTopicsResult =
//        kafkaAdminClient.describeTopics(Collections.singletonList(topicName));
    TopicMeta topicMeta = new TopicMeta(topicName);
//    try {
//      Map<String, TopicDescription> topicMap =
//          describeTopicsResult.all().get(kafkaAdminClientGetTimeoutMs, TimeUnit.MILLISECONDS);
//      if (topicMap.containsKey(topicName)) {
//        TopicDescription topicDescription = topicMap.get(topicName);
    TopicDescription topicDescription = getTopicDescription(topicName);
    if (topicDescription != null) {
      List<TopicPartitionInfo> tmList = topicDescription.partitions();
      topicMeta.setInternal(topicDescription.isInternal());
      topicMeta.setPartitionCount(topicDescription.partitions().size());
      topicMeta.setReplicationFactor(tmList.get(0).replicas().size());
      topicMeta.setTopicPartitionInfos(
          tmList
              .parallelStream()
              .map(
                  tm -> {
                    CustomTopicPartitionInfo customTopicPartitionInfo =
                        new CustomTopicPartitionInfo();
                    customTopicPartitionInfo.setTopicPartitionInfo(tm);
                    customTopicPartitionInfo.setIn_sync();
                    if (tm.leader() != null) {
                      customTopicPartitionInfo.setStartOffset(
                          getBeginningOffset(topicName, tm.partition()));
                      customTopicPartitionInfo.setEndOffset(
                          getEndOffset(topicName, tm.partition()));
                    } else {
                      customTopicPartitionInfo.setStartOffset(-1);
                      customTopicPartitionInfo.setEndOffset(-1);
                    }
                    customTopicPartitionInfo.setMessageAvailable();
                    return customTopicPartitionInfo;
                  })
              .collect(toList()));
      Collections.sort(topicMeta.getTopicPartitionInfos());
    }
//      }
//    } catch (Exception exception) {
//      exception.printStackTrace();
//      throw new ApiException("Describe topic exception." + exception);
//    }

    return topicMeta;
  }

  public Map<String, GeneralResponse> deleteTopicList(List<String> topicList) {
    org.apache.kafka.clients.admin.AdminClient kafkaAdminClient = createKafkaAdminClient();
    HashMap<String, GeneralResponse> deleteResults = new HashMap<>();

    List<String> topicListToBeDeleted = new ArrayList<>(topicList);

    log.warn("Delete topic " + topicList);
    for (int i = 0; i < topicList.size(); i++) {
      String topic = topicList.get(i);
      try {
        if (Topic.isInternal(topic)) {
          throw new ApiException(
              "Topic " + topic + " is a kafka internal topic and is not allowed to be deleted.");
        }
        if (!existTopic(topic)) {
          throw new ApiException("Topic " + topic + " non-exists.");
        }
      } catch (Exception exception) {
        topicListToBeDeleted.remove(topic);
        GeneralResponse generalResponse =
            GeneralResponse.builder()
                .state(GeneralResponseState.failure)
                .msg(exception.getMessage())
                .build();
        deleteResults.put(topic, generalResponse);
      }
    }

    DeleteTopicsResult deleteTopicsResult = kafkaAdminClient.deleteTopics(topicListToBeDeleted);
    try {
      deleteTopicsResult.all().get(kafkaAdminClientGetTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception exception) {
      log.warn("Delete topic exception:" + exception);
    } finally {
      deleteTopicsResult
          .values()
          .forEach(
              (topic, result) -> {
                GeneralResponse generalResponse;
                if (result.isDone() && !result.isCompletedExceptionally()) {
                  generalResponse =
                      GeneralResponse.builder().state(GeneralResponseState.success).build();
                } else {
                  generalResponse =
                      GeneralResponse.builder()
                          .state(GeneralResponseState.failure)
                          .msg(result.toString())
                          .build();
                }
                deleteResults.put(topic, generalResponse);
              });
    }

    return deleteResults;
  }

  public Collection<ConfigEntry> describeConfig(ConfigResource.Type type, String name) {
    Map<ConfigResource, Config> configs;
    org.apache.kafka.clients.admin.AdminClient kafkaAdminClient = createKafkaAdminClient();
    ConfigResource configResource = new ConfigResource(type, name);

    DescribeConfigsResult ret =
        kafkaAdminClient.describeConfigs(Collections.singleton(configResource));
    try {
      configs = ret.all().get(kafkaAdminClientGetTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception exception) {
      log.warn("Describe config type:" + type + ", name:" + name + " exception:" + exception);
      throw new ApiException("Describe config exception:" + exception.getLocalizedMessage());
    }

    return configs.get(configResource).entries();
  }

  public boolean alterConfig(
      ConfigResource.Type type, String name, Collection<ConfigEntry> configEntries) {
    org.apache.kafka.clients.admin.AdminClient kafkaAdminClient = createKafkaAdminClient();
    Config config = new Config(configEntries);
    AlterConfigsResult alterConfigsResult =
        kafkaAdminClient.alterConfigs(
            Collections.singletonMap(new ConfigResource(type, name), config));

    try {
      alterConfigsResult.all().get(kafkaAdminClientAlterTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception exception) {
      log.warn("Alter config type:" + type + ", name:" + name + " exception:" + exception);
      return false;
    }

    return true;
  }

  public Collection<CustomConfigEntry> updateTopicConf(
      @TopicExistConstraint String topic, Properties props) {
    Collection<ConfigEntry> configEntries =
        props
            .entrySet()
            .stream()
            .map(e -> new ConfigEntry(e.getKey().toString(), e.getValue().toString()))
            .collect(Collectors.toList());
    alterConfig(ConfigResource.Type.TOPIC, topic, configEntries);

    return getTopicConf(topic);
  }

  public Collection<CustomConfigEntry> getTopicConf(@TopicExistConstraint String topic) {
    Properties dynamicProps = getConfigInZk(Type.TOPIC, topic);

    Collection<ConfigEntry> configs = describeConfig(Type.TOPIC, String.valueOf(topic));

    return mergeConfigs(configs, dynamicProps);
  }

  public Properties getTopicConfByKey(@TopicExistConstraint String topic, String key) {
    Collection<ConfigEntry> configEntries = describeConfig(ConfigResource.Type.TOPIC, topic);
    Properties returnProps = new Properties();
    for (ConfigEntry entry : configEntries) {
      if (entry.name().equals(key)) {
        returnProps.put(key, entry.value());
        return returnProps;
      }
    }

    return null;
  }

  public Collection<CustomConfigEntry> updateTopicConfByKey(
      @TopicExistConstraint String topic, String key, String value) {
    alterConfig(
        ConfigResource.Type.TOPIC, topic, Collections.singletonList(new ConfigEntry(key, value)));

    return getTopicConf(topic);
  }

  public Map<String, Set<String>> listAllConsumerGroups(ConsumerType type) {
    Map<String, Set<String>> result = new HashMap<>();

    if (type == null || type == ConsumerType.OLD) {
      Set<String> oldConsumerGroupList = listAllOldConsumerGroups();
      if (oldConsumerGroupList == null || oldConsumerGroupList.size() == 0) {
        result.put("old", new HashSet<>());
      } else {
        result.put("old", oldConsumerGroupList);
      }
    }

    if (type == null || type == ConsumerType.NEW) {
      Set<String> newConsumerGroupList = listAllNewConsumerGroups();
      if (newConsumerGroupList == null || newConsumerGroupList.size() == 0) {
        result.put("new", new HashSet<>());
      } else {
        result.put("new", newConsumerGroupList);
      }
    }

    return result;
  }

  public Set<String> listAllNewConsumerGroups() {
    AdminClient adminClient = kafkaUtils.createAdminClient();
    log.info("Calling the listAllConsumerGroupsFlattened");
    // Send LIST_GROUPS Request to kafka
    Set activeGroups =
        CollectionConvertor.seqConvertJavaList(adminClient.listAllConsumerGroupsFlattened())
            .stream()
            .map(GroupOverview::groupId)
            .collect(toSet());

    log.info("Finish getting new consumers");
    adminClient.close();
    return activeGroups;
  }

  private Set<String> listAllOldConsumerGroups() {
    log.info("Finish getting old consumers");
    KafkaZkClient kafkaZkClient = zookeeperUtils.createKafkaZkClient();

    Set<String> oldConsumerGroups =
        CollectionConvertor.seqConvertJavaList(kafkaZkClient.getChildren(ZkUtils.ConsumersPath()))
            .stream()
            .collect(toSet());

    return oldConsumerGroups;
  }

  public Map<String, Set<String>> listConsumerGroupsByTopic(
      @TopicExistConstraint String topic, ConsumerType type) {
    Map<String, Set<String>> result = new HashMap<>();

    if (type == null || type == ConsumerType.OLD) {
      Set<String> oldConsumerGroupList = new HashSet<>();
      try {
        oldConsumerGroupList = listOldConsumerGroupsByTopic(topic);
      } catch (Exception e) {
        e.printStackTrace();
      }
      if (oldConsumerGroupList.size() != 0) {
        result.put("old", oldConsumerGroupList);
      }
    }

    if (type == null || type == ConsumerType.NEW) {
      Set<String> newConsumerGroupList = listNewConsumerGroupsByTopic(topic);

      if (newConsumerGroupList.size() != 0) {
        result.put("new", newConsumerGroupList);
      }
    }

    return result;
  }

  private Set<String> listNewConsumerGroupsByTopic(@TopicExistConstraint String topic) {
    Set<String> result = new HashSet();
    Set<String> consumersList = listAllNewConsumerGroups();

    for (String c : consumersList) {
      List<String> topics = getTopicListByConsumerGroup(c);
      if (topics.contains(topic)) {
        result.add(c);
      }
    }

    return result;
  }

  private Set<String> listOldConsumerGroupsByTopic(@TopicExistConstraint String topic) {
    KafkaZkClient kafkaZkClient = zookeeperUtils.createKafkaZkClient();

    List<String> consumersFromZk =
        CollectionConvertor.seqConvertJavaList(kafkaZkClient.getChildren(ZkUtils.ConsumersPath()));
    Set<String> consumerList = new HashSet<>();

    for (String consumer : consumersFromZk) {
      String path = ZkUtils.ConsumersPath() + "/" + consumer + "/offsets";
      List<String> topics = CollectionConvertor.seqConvertJavaList(kafkaZkClient.getChildren(path));
      if (topics != null && topics.contains(topic)) {
        consumerList.add(consumer);
      }
    }

    return consumerList;
  }

  public Set<String> listTopicsByConsumerGroup(String consumerGroup, ConsumerType type) {
    KafkaZkClient kafkaZkClient = zookeeperUtils.createKafkaZkClient();
    Set<String> topicList = new HashSet<>();

    if (type == null) {
      throw new ApiException("Bad Request since type is null.");
    }

    if (type == ConsumerType.OLD) {
      if (!isOldConsumerGroup(consumerGroup)) {
        throw new ApiException("Consumer group:" + consumerGroup + " non-exist");
      }
      String path = ZkUtils.ConsumersPath() + "/" + consumerGroup + "/offsets";
      topicList =
          CollectionConvertor.seqConvertJavaList(kafkaZkClient.getChildren(path))
              .stream()
              .collect(toSet());
    } else if (type == ConsumerType.NEW) {
      if (!isNewConsumerGroup(consumerGroup)) {
        throw new ApiException("Consumer group:" + consumerGroup + " non-exist!");
      }
      topicList.addAll(getTopicListByConsumerGroup(consumerGroup));
    } else {
      throw new ApiException("Unknown Type " + type);
    }

    return topicList;
  }

  private List<String> getTopicListByConsumerGroup(String consumerGroup) {
    AdminClient adminClient = kafkaUtils.createAdminClient();
    Map<TopicPartition, Object> groupOffsets =
        CollectionConvertor.mapConvertJavaMap(adminClient.listGroupOffsets(consumerGroup));

    return groupOffsets
        .entrySet()
        .stream()
        .map(topicPartitionObjectEntry -> topicPartitionObjectEntry.getKey().topic())
        .collect(toList());
  }

  public ConsumerGroupMeta getConsumerGroupMeta(String consumerGroup) {
    List<MemberDescription> members = new ArrayList<>();
    AdminClient adminClient = kafkaUtils.createAdminClient();

    ConsumerGroupSummary consumerGroupSummary = adminClient.describeConsumerGroup(consumerGroup,
        kafkaAdminClientGetTimeoutMs);
    List<ConsumerSummary> consumerSummaryList =
        CollectionConvertor.optionListConvertJavaList(consumerGroupSummary.consumers().get());

    ConsumerGroupMeta consumerGroupMeta =
        ConsumerGroupMeta.builder()
            .groupId(consumerGroup)
            .state(ConsumerGroupState.parse(consumerGroupSummary.state()))
            .assignmentStrategy(consumerGroupSummary.assignmentStrategy())
            .coordinator(consumerGroupSummary.coordinator())
            .build();

    consumerSummaryList.forEach(
        consumerSummary -> {
          List<TopicPartition> topicPartitions =
              CollectionConvertor.listConvertJavaList(consumerSummary.assignment());
          members.add(
              new MemberDescription(
                  consumerSummary.consumerId(),
                  consumerSummary.clientId(),
                  consumerSummary.host(),
                  topicPartitions));
        });

    Collections.sort(members);
    consumerGroupMeta.setMembers(members);

    adminClient.close();
    return consumerGroupMeta;
  }

  public boolean isOldConsumerGroup(String consumerGroup) {
    return listAllOldConsumerGroups().contains(consumerGroup);
  }

  public boolean isNewConsumerGroup(String consumerGroup) {
    // Active Consumergroup or Dead ConsumerGroup is OK
    return (listAllNewConsumerGroups().contains(consumerGroup));
  }

  public Map<String, List<ConsumerGroupDesc>> describeConsumerGroup(
      String consumerGroup, ConsumerType type) {
    Map<String, List<ConsumerGroupDesc>> result = new HashMap<>();
    Set<String> topicList = listTopicsByConsumerGroup(consumerGroup, type);
    if (topicList == null) {
      // Return empty result
      return result;
    }
    if (type == ConsumerType.NEW) {
      if (!isNewConsumerGroup(consumerGroup)) {
        throw new ApiException("Consumer group:" + consumerGroup + " non-exist!");
      }
      List<PartitionAssignmentState> partitionAssignmentStateList =
          describeNewConsumerGroup(consumerGroup, false, null);
      result = convertPasListToMap(consumerGroup, partitionAssignmentStateList, ConsumerType.NEW);
    } else if (type == ConsumerType.OLD) {
      if (!isOldConsumerGroup(consumerGroup)) {
        throw new ApiException("Consumer group:" + consumerGroup + " non-exist");
      }
      List<PartitionAssignmentState> partitionAssignmentStateList =
          describeOldConsumerGroup(consumerGroup, false, null);
      result = convertPasListToMap(consumerGroup, partitionAssignmentStateList, ConsumerType.OLD);
    }

    return result;
  }

  // Convert partition assignment to map, key is topic
  private Map<String, List<ConsumerGroupDesc>> convertPasListToMap(
      String consumerGroup, List<PartitionAssignmentState> pasList, ConsumerType type) {
    Map<String, List<ConsumerGroupDesc>> result = new HashMap<>();
    ConsumerGroupSummary consumerGroupSummary;

    if (type.equals(ConsumerType.NEW)) {
      AdminClient adminClient = kafkaUtils.createAdminClient();
      consumerGroupSummary = adminClient.describeConsumerGroup(consumerGroup, 0);
      adminClient.close();
    } else {
      // Old consumer group has no state, coordinator, assignmentStrategy info
      consumerGroupSummary = null;
    }

    pasList.forEach(
        partitionAssignmentState -> {
          String topic = partitionAssignmentState.getTopic();
          List<ConsumerGroupDesc> consumerGroupDescs;
          if (result.containsKey(topic)) {
            consumerGroupDescs = result.get(topic);
          } else {
            consumerGroupDescs = new ArrayList<>();
          }
          consumerGroupDescs.add(
              convertParitionAssignmentStateToGroupDesc(
                  consumerGroup, consumerGroupSummary, partitionAssignmentState, type));
          result.put(topic, consumerGroupDescs);
        });

    return result;
  }

  private ConsumerGroupDesc convertParitionAssignmentStateToGroupDesc(
      String consumerGroup,
      ConsumerGroupSummary consumerGroupSummary,
      PartitionAssignmentState pas,
      ConsumerType type) {
    ConsumerGroupDesc.ConsumerGroupDescBuilder consumerGroupDescBuilder =
        ConsumerGroupDesc.builder()
            .groupName(consumerGroup)
            .topic(pas.getTopic())
            .partitionId(pas.getPartition())
            .currentOffset(pas.getOffset())
            .logEndOffset(pas.getLogEndOffset())
            .lag(pas.getLag())
            .consumerId(pas.getConsumerId())
            .clientId(pas.getClientId())
            .host(pas.getHost())
            .type(type);

    if (consumerGroupSummary != null) {
      consumerGroupDescBuilder =
          consumerGroupDescBuilder
              .state(ConsumerGroupState.parse(consumerGroupSummary.state()))
              .assignmentStrategy(consumerGroupSummary.assignmentStrategy())
              .coordinator(consumerGroupSummary.coordinator());
    }

    return consumerGroupDescBuilder.build();
  }

  public List<PartitionAssignmentState> describeNewConsumerGroup(
      String consumerGroup, boolean filtered, String topic) {
    AdminClient adminClient = kafkaUtils.createAdminClient();
    ConsumerGroupSummary consumerGroupSummary = adminClient.describeConsumerGroup(consumerGroup, 0);

    adminClient.close();
    return describeNewConsumerGroup(consumerGroup, filtered, topic, consumerGroupSummary);
  }

  public List<PartitionAssignmentState> describeNewConsumerGroup(
      String consumerGroup, boolean filtered, String topic, ConsumerGroupSummary consumerGroupSummary) {
    List<PartitionAssignmentState> partitionAssignmentStateList = new ArrayList<>();

    if (filtered && !existTopic(topic)) {
      return partitionAssignmentStateList;
    }

    AdminClient adminClient = kafkaUtils.createAdminClient();
//    ConsumerGroupSummary consumerGroupSummary = adminClient.describeConsumerGroup(consumerGroup, 0);
    List<ConsumerSummary> consumerSummaryList =
        CollectionConvertor.listConvertJavaList(consumerGroupSummary.consumers().get());
    String consumersInfo = "";
    for (ConsumerSummary cs:consumerSummaryList) {
      consumersInfo = consumersInfo + "{clientId:" + cs.clientId() + ", host:" + cs.host()
          + ", consumerId:" + cs.consumerId() + "}\n";
    }
    log.info("Describe consumer group:" + consumerGroup + " summary. ConsumerSummary List:" + consumersInfo);
    if (consumerSummaryList != null) {
      Map<TopicPartition, Object> offsets =
          CollectionConvertor.mapConvertJavaMap(adminClient.listGroupOffsets(consumerGroup));
      log.info("List group offsets for consumer:" + consumerGroup + ". Result is:\n");
      for(Map.Entry<TopicPartition, Object> offset:offsets.entrySet()) {
        log.info("Topic:" + offset.getKey().topic() + ", Partition:" + offset.getKey().partition() + ", Offset:" + offset.getValue());
      }
      Map<TopicPartition, Object> offsetsFiltered;

      adminClient.close();
      if (filtered && existTopic(topic)) {
        offsetsFiltered =
            offsets
                .entrySet()
                .stream()
                .filter(
                    topicPartitionObjectEntry ->
                        topicPartitionObjectEntry.getKey().topic().equals(topic))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        log.info("After filter by topic:" + topic + " for consumer:" + consumerGroup + ", offsets are:");
        for(Map.Entry<TopicPartition, Object> offset:offsetsFiltered.entrySet()) {
          log.info("Topic:" + offset.getKey().topic() + ", Partition:" + offset.getKey().partition() + ", Offset:" + offset.getValue());
        }
      } else {
        offsetsFiltered = offsets;
      }
      if (offsetsFiltered.isEmpty()) {
        return partitionAssignmentStateList;
      } else {
        ArrayList<TopicPartition> assignedTopicPartitions = new ArrayList<>();
        List<PartitionAssignmentState> rowsWithConsumer =
            consumerSummaryList
                .stream()
                .flatMap(
                    consumerSummary -> {
                      List<TopicPartition> topicPartitions =
                          CollectionConvertor.listConvertJavaList(consumerSummary.assignment());
                      List<TopicPartition> topicPartitionsFiltered = topicPartitions;
                      if (filtered) {
                        topicPartitionsFiltered =
                            topicPartitions
                                .stream()
                                .filter(topicPartition -> topicPartition.topic().equals(topic))
                                .collect(toList());
                      }
                      Map<TopicPartition, Object> partitionOffsets = new HashMap<>();
                      log.info("topicPartitionsFiltered.size = " + topicPartitionsFiltered.size() + ". Detail is:");
                      for (TopicPartition tp:topicPartitionsFiltered) {
                        log.info("topic:" + tp.topic() + ", partition:" + tp.partition() + ", offset:" + offsetsFiltered.get(tp));
                        partitionOffsets.put(tp, offsetsFiltered.get(tp));
                      }
                      assignedTopicPartitions.addAll(topicPartitionsFiltered);
                      /*
                      Map<TopicPartition, Object> partitionOffsets =
                          topicPartitionsFiltered
                              .stream()
                              .collect(
                                  toMap(
                                      Function.identity(),
                                      topicPartition -> offsetsFiltered.get(topicPartition)));
                                      */
                      log.info("Topic partitions with consumers. Detail is:");
                      for(Map.Entry<TopicPartition, Object> offset:partitionOffsets.entrySet()) {
                        log.info("Topic:" + offset.getKey().topic() + ", Partition:" +
                            offset.getKey().partition() + ", Offset:" + offset.getValue() +
                            ", consumerGroup:" + consumerGroup + ", consumerId:" +
                            consumerSummary.consumerId() + "," + ", consumerHost:" +
                            consumerSummary.host() + ", consumerClient:" + consumerSummary.clientId());
                      }
                      return collectConsumerAssignment(
                          consumerGroup,
                          consumerGroupSummary.coordinator(),
                          topicPartitionsFiltered,
                          partitionOffsets,
                          consumerSummary.consumerId(),
                          consumerSummary.host(),
                          consumerSummary.clientId())
                          .stream();
                    })
                .collect(toList());

        List<TopicPartition> topicPartitionsWithoutConsumer = new ArrayList<>();
        Map<TopicPartition, Object> partitionOffsetsWithoutConsumer = new HashMap<>();

        offsetsFiltered
            .entrySet()
            .forEach(
                topicPartitionObjectEntry -> {
                  if (!assignedTopicPartitions.contains(topicPartitionObjectEntry.getKey())) {
                    topicPartitionsWithoutConsumer.add(topicPartitionObjectEntry.getKey());
                    partitionOffsetsWithoutConsumer.put(
                        topicPartitionObjectEntry.getKey(), topicPartitionObjectEntry.getValue());
                  }
                });
        List<PartitionAssignmentState> rowsWithoutConsumer =
            collectConsumerAssignment(
                consumerGroup,
                consumerGroupSummary.coordinator(),
                topicPartitionsWithoutConsumer,
                partitionOffsetsWithoutConsumer,
                "-",
                "-",
                "-");
        if (rowsWithoutConsumer.size() > 0) {
          log.info("Topic Partitions without consumer. Detail is :");
          for (PartitionAssignmentState pas : rowsWithoutConsumer) {
            log.info("Topic:" + pas.getTopic() + ", Partition:" + pas.getPartition() + ", Offset:" +
                pas.getOffset() + ", consumerGroup:" + consumerGroup + ", consumerId:" +
                pas.getConsumerId() + "," + ", consumerHost:" + pas.getHost() +
                ", consumerClient:" + pas.getClientId());
          }
        }
        partitionAssignmentStateList.addAll(rowsWithConsumer);
        partitionAssignmentStateList.addAll(rowsWithoutConsumer);
        log.info("rowsWithConsumer.size:" + rowsWithConsumer.size() + ", rowsWithoutConsumer.size:"
            + rowsWithoutConsumer.size());
      }
    }

    Collections.sort(partitionAssignmentStateList);
    return partitionAssignmentStateList;
  }

  private List<PartitionAssignmentState> collectConsumerAssignment(
      String group,
      Node coordinator,
      List<TopicPartition> topicPartitions,
      Map<TopicPartition, Object> partitionOffsets,
      String consumerId,
      String host,
      String clientId) {
    if (topicPartitions.size() == 0) {
      return new ArrayList<PartitionAssignmentState>();
    } else {
      List<PartitionAssignmentState> list = new ArrayList<>();
      topicPartitions.forEach(
          topicPartition -> {
            long logEndOffset = getEndOffset(topicPartition.topic(), topicPartition.partition());
            long offset = (Long) partitionOffsets.get(topicPartition);
            long lag = (logEndOffset < 0)?0:logEndOffset - offset;
            list.add(
                new PartitionAssignmentState(
                    group,
                    coordinator,
                    topicPartition.topic(),
                    topicPartition.partition(),
                    offset,
                    lag,
                    consumerId,
                    host,
                    clientId,
                    logEndOffset));
          });

      Collections.sort(list);
      return list;
    }
  }

  public List<PartitionAssignmentState> describeOldConsumerGroup(
      String consumerGroup, boolean filtered, String topic) {
    List<PartitionAssignmentState> partitionAssignmentStateList = new ArrayList<>();

    if (filtered && !existTopic(topic)) {
      return partitionAssignmentStateList;
    }

    String[] agrs = {
        "--describe",
        "--zookeeper",
        zookeeperUtils.getZookeeperConfig().getUris(),
        "--group",
        consumerGroup
    };
    ConsumerGroupCommand.ConsumerGroupCommandOptions options =
        new ConsumerGroupCommand.ConsumerGroupCommandOptions(agrs);
    ConsumerGroupCommand.ZkConsumerGroupService zkConsumerGroupService =
        new ConsumerGroupCommand.ZkConsumerGroupService(options);

    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new DefaultScalaModule());
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    try {
      String source =
          mapper.writeValueAsString(zkConsumerGroupService.collectGroupOffsets()._2().get());
      partitionAssignmentStateList =
          mapper.readValue(
              source, getCollectionType(mapper, List.class, PartitionAssignmentState.class));
      List<PartitionAssignmentState> partitionAssignmentStateListFiltered;
      if (filtered && existTopic(topic)) {
        partitionAssignmentStateListFiltered =
            partitionAssignmentStateList
                .stream()
                .filter(
                    partitionAssignmentState -> partitionAssignmentState.getTopic().equals(topic))
                .collect(Collectors.toList());
      } else {
        partitionAssignmentStateListFiltered = partitionAssignmentStateList;
      }
      partitionAssignmentStateListFiltered.sort(
          Comparator.comparing(PartitionAssignmentState::getTopic)
              .thenComparing(PartitionAssignmentState::getPartition));
    } catch (JsonProcessingException jsonProcessingException) {
      log.error("Describe old consumer group exception.", jsonProcessingException);
    } catch (IOException ioexception) {
      log.error("Describe old consumer group exception.", ioexception);
    }

    return partitionAssignmentStateList;
  }

  private JavaType getCollectionType(
      ObjectMapper mapper, Class<?> collectionClass, Class<?>... elementClasses) {
    return mapper.getTypeFactory().constructParametricType(collectionClass, elementClasses);
  }

  private List<ConsumerGroupDesc> getNewConsumerGroupDescByConsumerGroupAndTopic(String consumerGroup, String topic) {
    AdminClient adminClient = kafkaUtils.createAdminClient();
    ConsumerGroupSummary consumerGroupSummary = adminClient.describeConsumerGroup(consumerGroup, kafkaAdminClientGetTimeoutMs);
    log.info("Describe consumer group:" + consumerGroup + ". Summary:" +consumerGroupSummary.consumers());
    List<PartitionAssignmentState> partitionAssignmentStateList =
        describeNewConsumerGroup(consumerGroup, true, topic, consumerGroupSummary);
    log.info("Partition Assignment State List for consumer:" + consumerGroup + " fitlered by topic:"
        + topic + "  size :" + partitionAssignmentStateList.size());

    adminClient.close();

    return partitionAssignmentStateList
        .stream()
        .map(
            partitionAssignmentState ->
                convertParitionAssignmentStateToGroupDesc(
                    consumerGroup,
                    consumerGroupSummary,
                    partitionAssignmentState,
                    ConsumerType.NEW))
        .collect(Collectors.toList());
  }

  public List<ConsumerGroupDesc> describeNewConsumerGroupByTopic(
      String consumerGroup, @TopicExistConstraint String topic) {
    if (consumerGroup != null && !isNewConsumerGroup(consumerGroup)) {
      throw new ApiException("ConsumerGroup:" + consumerGroup + " non-exist!");
    }

    List<ConsumerGroupDesc> consumerGroupDescList = new ArrayList<>();
    if (consumerGroup == null || consumerGroup.length() == 0) {
      // To search all consumer groups
      Set<String> allNewConsumerGroups = listAllNewConsumerGroups();

      for (String cg:allNewConsumerGroups) {
        consumerGroupDescList.addAll(getNewConsumerGroupDescByConsumerGroupAndTopic(cg, topic));
      }
      return consumerGroupDescList;
    } else {
      return getNewConsumerGroupDescByConsumerGroupAndTopic(consumerGroup, topic);
    }
  }

  private List<ConsumerGroupDesc> getOldConsumerGroupDescByConsumerGroupAndTopic(String consumerGroup, String topic) {
    List<PartitionAssignmentState> partitionAssignmentStateList =
        describeOldConsumerGroup(consumerGroup, true, topic);

    ConsumerGroupSummary consumerGroupSummary = null;

    return partitionAssignmentStateList
        .stream()
        .map(
            partitionAssignmentState ->
                convertParitionAssignmentStateToGroupDesc(
                    consumerGroup,
                    consumerGroupSummary,
                    partitionAssignmentState,
                    ConsumerType.OLD))
        .collect(Collectors.toList());
  }

  public List<ConsumerGroupDesc> describeOldConsumerGroupByTopic(
      String consumerGroup, @TopicExistConstraint String topic) {
    if (!isOldConsumerGroup(consumerGroup)) {
      throw new ApiException("ConsumerGroup:" + consumerGroup + " non-exist");
    }

    List<ConsumerGroupDesc> consumerGroupDescList = new ArrayList<>();
    if (consumerGroup == null || consumerGroup.length() == 0) {
      // To search all consumer groups
      Set<String> allNewConsumerGroups = listAllNewConsumerGroups();

      for (String cg:allNewConsumerGroups) {
        consumerGroupDescList.addAll(getOldConsumerGroupDescByConsumerGroupAndTopic(cg, topic));
      }
      return consumerGroupDescList;
    } else {
      return getOldConsumerGroupDescByConsumerGroupAndTopic(consumerGroup, topic);
    }
  }

  public Map<String, GeneralResponse> addPartitions(List<AddPartition> addPartitions) {
    Map<String, GeneralResponse> addPartitionsResult = new HashMap<>();
    org.apache.kafka.clients.admin.AdminClient kafkaAdminClient = createKafkaAdminClient();

    Map<String, NewPartitions> newPartitionsMap = new HashMap<>();
    addPartitions.forEach(
        addPartition -> {
          String topic = addPartition.getTopic();
          if (!existTopic(topic)) {
            addPartitionsResult.put(
                topic,
                GeneralResponse.builder()
                    .state(GeneralResponseState.failure)
                    .msg("Topic:" + topic + " non-exist.")
                    .build());
          } else {
            TopicMeta topicMeta = describeTopic(topic);
            int currentPartionCount = topicMeta.getPartitionCount();
            int numPartitionsAdded = addPartition.getNumPartitionsAdded();
            int totalCount = currentPartionCount + numPartitionsAdded;
            List<List<Integer>> newAssignments = addPartition.getReplicaAssignment();
            NewPartitions newPartitions;
            if (newAssignments == null || newAssignments.isEmpty()) {
              // The assignment of new replicas to brokers will be decided by the broker.
              newPartitions = NewPartitions.increaseTo(totalCount);
            } else {
              newPartitions = NewPartitions.increaseTo(totalCount, newAssignments);
            }
            newPartitionsMap.put(topic, newPartitions);
          }
        });
    CreatePartitionsResult createPartitionsResult =
        kafkaAdminClient.createPartitions(newPartitionsMap);
    try {
      createPartitionsResult.all().get(kafkaAdminClientAlterTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception exception) {
      log.warn("Add partitions exception: " + exception);
    } finally {
      Map<String, KafkaFuture<Void>> result = createPartitionsResult.values();
      result.forEach(
          (topic, createResult) -> {
            GeneralResponse generalResponse;
            if (!createResult.isCompletedExceptionally() && createResult.isDone()) {
              TopicMeta topicMeta = describeTopic(topic);
              generalResponse =
                  GeneralResponse.builder()
                      .state(GeneralResponseState.success)
                      .data(topicMeta)
                      .build();
            } else {
              generalResponse =
                  GeneralResponse.builder()
                      .state(GeneralResponseState.failure)
                      .msg(createResult.toString())
                      .build();
            }
            addPartitionsResult.put(topic, generalResponse);
          });

      return addPartitionsResult;
    }
  }

  // Return <Current partition replica assignment, Proposed partition reassignment>
  public List<ReassignModel> generateReassignPartition(ReassignWrapper reassignWrapper) {
    KafkaZkClient kafkaZkClient = zookeeperUtils.createKafkaZkClient();
    List<ReassignModel> result = new ArrayList<>();

    Seq brokerSeq =
        JavaConverters.asScalaBufferConverter(reassignWrapper.getBrokers()).asScala().toSeq();
    // <Proposed partition reassignment，Current partition replica assignment>
    Tuple2 resultTuple2;
    try {
      resultTuple2 =
          ReassignPartitionsCommand.generateAssignment(
              kafkaZkClient, brokerSeq, reassignWrapper.generateReassignJsonString(), false);
    } catch (Exception exception) {
      throw new ApiException("Generate reassign plan exception." + exception);
    }
    HashMap<TopicPartitionReplica, String> emptyMap = new HashMap<>();
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      result.add(
          objectMapper.readValue(
              ReassignPartitionsCommand.formatAsReassignmentJson(
                  (scala.collection.Map<TopicPartition, Seq<Object>>) resultTuple2._2(),
                  JavaConverters.mapAsScalaMapConverter(emptyMap).asScala()),
              ReassignModel.class));
      result.add(
          objectMapper.readValue(
              ReassignPartitionsCommand.formatAsReassignmentJson(
                  (scala.collection.Map<TopicPartition, Seq<Object>>) resultTuple2._1(),
                  JavaConverters.mapAsScalaMapConverter(emptyMap).asScala()),
              ReassignModel.class));
      Collections.sort(result.get(0).getPartitions());
      Collections.sort(result.get(1).getPartitions());
    } catch (Exception exception) {
      throw new ApiException("Generate reassign plan exception." + exception);
    }

    return result;
  }

  public ReassignStatus executeReassignPartition(
      ReassignModel reassignModel,
      Long interBrokerThrottle,
      Long replicaAlterLogDirsThrottle,
      Long timeoutMs) {
    // Set default value
    interBrokerThrottle = (interBrokerThrottle == null) ? Long.valueOf(-1) : interBrokerThrottle;
    replicaAlterLogDirsThrottle =
        (replicaAlterLogDirsThrottle == null) ? Long.valueOf(-1) : replicaAlterLogDirsThrottle;
    timeoutMs = (timeoutMs == null) ? Long.valueOf(10000) : timeoutMs;

    org.apache.kafka.clients.admin.AdminClient kafkaAdminClient = createKafkaAdminClient();
    KafkaZkClient kafkaZkClient = zookeeperUtils.createKafkaZkClient();
    AdminZkClient adminZkClient = new AdminZkClient(kafkaZkClient);

    TwoTuple<
        scala.collection.mutable.HashMap<TopicPartition, Seq<Object>>,
        scala.collection.Map<TopicPartitionReplica, String>>
        reassignPlan;
    String reassignStr = "";
    try {
      reassignStr = new ObjectMapper().writeValueAsString(reassignModel);
    } catch (Exception exception) {
      throw new ApiException("Json processing exception." + exception);
    }
    reassignPlan = genReassignPlan(reassignStr);
    scala.collection.mutable.HashMap<TopicPartition, Seq<Object>> partitionsToBeReassignedMap =
        reassignPlan.getFirst();
    scala.collection.Map<TopicPartitionReplica, String> replicatAssignment =
        reassignPlan.getSecond();

    ReassignPartitionsCommand reassignPartitionsCommand =
        new ReassignPartitionsCommand(
            kafkaZkClient,
            scala.Option.apply(kafkaAdminClient),
            partitionsToBeReassignedMap,
            replicatAssignment,
            adminZkClient);

    Function0<BoxedUnit> postUpdateAction =
        new AbstractFunction0<BoxedUnit>() {
          @Override
          public BoxedUnit apply() {
            return null;
          }
        };
    Throttle throttle =
        new Throttle(interBrokerThrottle, replicaAlterLogDirsThrottle, postUpdateAction);

    if (kafkaZkClient.reassignPartitionsInProgress()) {
      // check whether zk node /admin/reassign_partitions exists
      reassignPartitionsCommand.maybeLimit(throttle);
      throw new ApiException(
          "Failed to reassign partitions because there is an existing assignment running.");
    } else {
      try {
        reassignPartitionsCommand.reassignPartitions(throttle, timeoutMs);
        log.info("Successfully started reassignment of partitions.");
      } catch (Exception exception) {
        throw new ApiException(
            "Failed to reassign partitions:"
                + reassignPlan.getFirst()
                + ". Exception:"
                + exception.getLocalizedMessage());
      }
    }

    return checkReassignStatus(partitionsToBeReassignedMap, replicatAssignment);
  }

  private ReassignStatus checkReassignStatus(
      scala.collection.Map<TopicPartition, Seq<Object>> partitionsToBeReassigned,
      scala.collection.Map<TopicPartitionReplica, String> replicaAssignement) {
    ReassignStatus reassignStatus = new ReassignStatus();
    Map<TopicPartition, Integer> reassignedPartitionsStatus =
        checkIfPartitionReassignmentSucceeded(partitionsToBeReassigned);
    Map<TopicPartitionReplica, Integer> replicasReassignmentStatus =
        checkIfReplicaReassignmentSucceeded(
            CollectionConvertor.mapConvertJavaMap(replicaAssignement));

    reassignStatus.setPartitionsReassignStatus(reassignedPartitionsStatus);
    reassignStatus.setReplicasReassignStatus(replicasReassignmentStatus);

    if (removeThrottle(reassignedPartitionsStatus, replicasReassignmentStatus)) {
      reassignStatus.setRemoveThrottle(true);
    } else {
      reassignStatus.setRemoveThrottle(false);
    }

    reassignStatus.setMsg(
        "If removeThrottle in response is false, please use check api to remove throttle.");

    return reassignStatus;
  }

  private boolean removeThrottle(
      Map<TopicPartition, Integer> reassignedPartitionsStatus,
      Map<TopicPartitionReplica, Integer> replicasReassignmentStatus) {
    for (Map.Entry entry : reassignedPartitionsStatus.entrySet()) {
      // Partitions reassignemnt not all done
      if (!entry.getValue().equals(ReassignmentState.ReassignmentCompleted.code())) {
        return false;
      }
    }

    for (Map.Entry entry : replicasReassignmentStatus.entrySet()) {
      // Replica reassignement not all done
      if (!entry.getValue().equals(ReassignmentState.ReassignmentCompleted.code())) {
        return false;
      }
    }

    KafkaZkClient kafkaZkClient = zookeeperUtils.createKafkaZkClient();

    List<Broker> brokerList =
        CollectionConvertor.seqConvertJavaList(kafkaZkClient.getAllBrokersInCluster());
    for (Broker broker : brokerList) {
      int brokerId = broker.id();
      List<String> keysToBeRemoved = new ArrayList<>();
      //      We can't access the scala object here
      //      keysToBeRemoved.add(DynamicConfig.Broker.LeaderReplicationThrottledRateProp());
      //      keysToBeRemoved.add(DynamicConfig.Broker.FollowerReplicationThrottledRateProp());
      //
      // keysToBeRemoved.add(DynamicConfig.Broker.ReplicaAlterLogDirsIoMaxBytesPerSecondProp());
      keysToBeRemoved.add(LeaderReplicationThrottledRateProp);
      keysToBeRemoved.add(FollowerReplicationThrottledRateProp);
      keysToBeRemoved.add(ReplicaAlterLogDirsIoMaxBytesPerSecondProp);
      try {
        removeConfigInZk(Type.BROKER, String.valueOf(brokerId), keysToBeRemoved);
      } catch (ApiException apiException) {
        log.info(
            "Remove property on broker:" + brokerId + " failed since " + apiException.getMessage());
      }
    }

    Set<String> topics =
        reassignedPartitionsStatus.keySet().stream().map(tp -> tp.topic()).collect(toSet());
    Set<String> topicInReplicas =
        replicasReassignmentStatus.keySet().stream().map(tpr -> tpr.topic()).collect(toSet());
    topics.addAll(topicInReplicas);

    for (String topic : topics) {
      List<String> keysToBeRemoved = new ArrayList<>();
      keysToBeRemoved.add(LogConfig.LeaderReplicationThrottledReplicasProp());
      keysToBeRemoved.add(LogConfig.FollowerReplicationThrottledReplicasProp());
      try {
        removeConfigInZk(Type.TOPIC, topic, keysToBeRemoved);
      } catch (ApiException apiException) {
        log.info(
            "Remove property on topic:" + topic + " failed since " + apiException.getMessage());
      }
    }

    return true;
  }

  private Map<TopicPartition, Integer> checkIfPartitionReassignmentSucceeded(
      scala.collection.Map<TopicPartition, Seq<Object>> partitionsToBeReassigned) {
    Map<TopicPartition, Integer> reassignedPartitionsStatus = new HashMap<>();
    KafkaZkClient kafkaZkClient = zookeeperUtils.createKafkaZkClient();

    scala.collection.immutable.Map<TopicPartition, Seq<Object>> partitionsBeingReassigned =
        kafkaZkClient.getPartitionReassignment();
    scala.collection.Iterator<TopicPartition> topicPartitionIterator =
        partitionsToBeReassigned.keysIterator();
    while (topicPartitionIterator.hasNext()) {
      TopicPartition topicPartition = topicPartitionIterator.next();
      reassignedPartitionsStatus.put(
          topicPartition,
          ReassignPartitionsCommand.checkIfPartitionReassignmentSucceeded(
              kafkaZkClient,
              topicPartition,
              partitionsToBeReassigned,
              partitionsBeingReassigned)
              .status());
    }

    return reassignedPartitionsStatus;
  }

  private Map<TopicPartitionReplica, Integer> checkIfReplicaReassignmentSucceeded(
      Map<TopicPartitionReplica, String> replicaAssignement) {
    Map<TopicPartitionReplica, Integer> replicasReassignmentStatus = new HashMap<>();
    Map<TopicPartitionReplica, ReplicaLogDirInfo> replicaLogDirInfos = new HashMap<>();

    if (!replicaAssignement.isEmpty()) {
      List<TopicPartitionReplica> replicaList = new ArrayList<>(replicaAssignement.keySet());
      replicaLogDirInfos = describeReplicaLogDirs(replicaList);
    }

    for (Map.Entry<TopicPartitionReplica, String> newLogDirEntry : replicaAssignement.entrySet()) {
      TopicPartitionReplica tpr = newLogDirEntry.getKey();
      String newLogDir = newLogDirEntry.getValue();
      ReplicaLogDirInfo replicaLogDirInfo = replicaLogDirInfos.get(tpr);
      if (replicaLogDirInfo.getCurrentReplicaLogDir() == null) {
        // tpr log dir not found
        replicasReassignmentStatus.put(tpr, ReassignmentState.ReassignmentFailed.code());
      } else if (replicaLogDirInfo.getFutureReplicaLogDir() != null
          && replicaLogDirInfo.getFutureReplicaLogDir().equals(newLogDir)) {
        replicasReassignmentStatus.put(tpr, ReassignmentState.ReassignmentInProgress.code());
      } else if (replicaLogDirInfo.getFutureReplicaLogDir() != null
          && !replicaLogDirInfo.getFutureReplicaLogDir().equals(newLogDir)) {
        // tpr is being moved to another logdir instead of newLogDir
        replicasReassignmentStatus.put(tpr, ReassignmentState.ReassignmentFailed.code());
      } else if (replicaLogDirInfo.getCurrentReplicaLogDir() != null
          && replicaLogDirInfo.getCurrentReplicaLogDir().equals(newLogDir)) {
        replicasReassignmentStatus.put(tpr, ReassignmentState.ReassignmentCompleted.code());
      } else {
        replicasReassignmentStatus.put(tpr, ReassignmentState.ReassignmentFailed.code());
      }
    }

    return replicasReassignmentStatus;
  }

  public ReassignStatus checkReassignStatus(ReassignModel reassignModel) {
    String reassignJsonStr = "";
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      reassignJsonStr = objectMapper.writeValueAsString(reassignModel);
    } catch (JsonProcessingException exception) {
      throw new ApiException("Json processing exception." + exception);
    }
    TwoTuple<
        scala.collection.mutable.HashMap<TopicPartition, Seq<Object>>,
        scala.collection.Map<TopicPartitionReplica, String>>
        reassignPlan = genReassignPlan(reassignJsonStr);
    scala.collection.mutable.HashMap<TopicPartition, Seq<Object>> partitionsToBeReassignedMap =
        reassignPlan.getFirst();
    scala.collection.Map<TopicPartitionReplica, String> replicatAssignment =
        reassignPlan.getSecond();

    return checkReassignStatus(partitionsToBeReassignedMap, replicatAssignment);
  }

  private TwoTuple<
      scala.collection.mutable.HashMap<TopicPartition, Seq<Object>>,
      scala.collection.Map<TopicPartitionReplica, String>>
  genReassignPlan(String reassignJsonStr) {
    Tuple2 resultTuple2;

    KafkaZkClient kafkaZkClient = zookeeperUtils.createKafkaZkClient();

    try {
      // Parse and validate reassignment json string, return (partitionsToBeReassigned,
      // replicaAssignment)
      resultTuple2 = ReassignPartitionsCommand.parseAndValidate(kafkaZkClient, reassignJsonStr);
    } catch (Exception exception) {
      throw new ApiException("Bad Request. " + exception.getMessage());
    }
    // Change list buffer to map
    ListBuffer partitionsToBeReassignedList =
        (scala.collection.mutable.ListBuffer) resultTuple2._1();
    scala.collection.mutable.HashMap<TopicPartition, Seq<Object>> partitionsToBeReassignedMap =
        new scala.collection.mutable.HashMap<>();
    for (int i = 0; i < partitionsToBeReassignedList.size(); ++i) {
      Tuple2 tup = (Tuple2) partitionsToBeReassignedList.apply(i);
      partitionsToBeReassignedMap.put((TopicPartition) tup._1(), (Seq<Object>) tup._2());
    }

    scala.collection.Map<TopicPartitionReplica, String> replicatAssignment =
        (scala.collection.Map<TopicPartitionReplica, String>) resultTuple2._2();

    return new TwoTuple<>(partitionsToBeReassignedMap, replicatAssignment);
  }

  public String getMessage(
      @TopicExistConstraint String topic,
      int partition,
      long offset,
      String decoder,
      String avroSchema) {
    KafkaConsumer consumer =
        kafkaUtils.createNewConsumer(String.valueOf(System.currentTimeMillis()));
    TopicPartition tp = new TopicPartition(topic, partition);
    long beginningOffset = getBeginningOffset(topic, partition);
    long endOffset = getEndOffset(topic, partition);
    if (beginningOffset == endOffset) {
      throw new ApiException("There is no message in this partition of this topic");
    }
    if (offset < beginningOffset || offset >= endOffset) {
      log.error(offset + " error");
      consumer.close();
      throw new ApiException(
          "offsets must be between " + String.valueOf(beginningOffset + " and " + (endOffset - 1)));
    }
    consumer.assign(Collections.singletonList(tp));
    consumer.seek(tp, offset);

    String last = null;

    // ConsumerRecords<String, String> crs = consumer.poll(channelRetryBackoffMs);
    ConsumerRecords<String, String> crs = consumer.poll(3000);
    log.info(
        "Seek to offset:"
            + offset
            + ", topic:"
            + topic
            + ", partition:"
            + partition
            + ", crs.count:"
            + crs.count());
    if (crs.count() != 0) {
      Iterator<ConsumerRecord<String, String>> it = crs.iterator();
      while (it.hasNext()) {
        ConsumerRecord<String, String> initCr = it.next();
        last =
            "Value: "
                + initCr.value()
                + ", Offset: "
                + String.valueOf(initCr.offset())
                + ", timestamp:"
                + initCr.timestamp();
        log.info(
            "Value: " + initCr.value() + ", initCr.Offset: " + String.valueOf(initCr.offset()));
        if (last != null && initCr.offset() == offset) {
          break;
        }
      }
    }
    log.info("last:" + last);
    consumer.close();
    return last;
  }

  public Record getRecordByOffset(
      @TopicExistConstraint String topic,
      int partition,
      long offset,
      String decoder,
      String avroSchema) {
    if (!isTopicPartitionValid(topic, partition)) {
      throw new ApiException("Bad request. Topic:" + topic + " has no partition:" + partition);
    }

    checkOffsetValid(topic, partition, offset);

    if (!kafkaUtils.DESERIALIZER_TYPE_MAP.containsKey(decoder)) {
      throw new ApiException(
          "Bad request. Decoder class:"
              + decoder
              + " not found. ByteArrayDeserializer, ByteBufferDeserializer, BytesDeserializer, "
              + "DoubleDeserializer, FloatDeserializer, "
              + "IntegerDeserializer, LongDeserializer, ShortDeserializer, StringDeserializer, AvroDeserializer "
              + "are supported.");
    }

    if (decoder != null && decoder.equals("AvroDeserializer")) {
      if (avroSchema == null || avroSchema.isEmpty()) {
        throw new ApiException("Bad request. Schema is needed when choosing AvroDeserializer.");
      } else {
        return getAvroRecordByOffset(topic, partition, offset, avroSchema);
      }
    }

    if (decoder == null || decoder.isEmpty()) {
      // default decoder is StringDeserializer
      decoder = "StringDeserializer";
    }

    Class<Object> type = kafkaUtils.DESERIALIZER_TYPE_MAP.get(decoder);
    String dese = Serdes.serdeFrom(type).deserializer().getClass().getCanonicalName();

    KafkaConsumer consumer;
    try {
      consumer = kafkaUtils.createNewConsumer(String.valueOf(System.currentTimeMillis()), dese);
    } catch (ClassNotFoundException classNotFoundException) {
      throw new ApiException("Class " + dese + "not found exception." + classNotFoundException);
    }

    TopicPartition tp = new TopicPartition(topic, partition);
    consumer.assign(Collections.singletonList(tp));
    consumer.seek(tp, offset);

    Record record = Record.builder().topic(topic).type(type).decoder(decoder).build();

    try {
      ConsumerRecords<Object, Object> crs = consumer.poll(3000);
      log.info(
          "Seek to offset:"
              + offset
              + ", topic:"
              + topic
              + ", partition:"
              + partition
              + ", crs.count:"
              + crs.count());
      if (crs.count() != 0) {
        Iterator<ConsumerRecord<Object, Object>> it = crs.iterator();
        while (it.hasNext()) {
          ConsumerRecord<Object, Object> initCr = it.next();
          if (initCr.offset() == offset) {
            record.setOffset(offset);
            record.setTimestamp(initCr.timestamp());
            record.setKey(initCr.key());
            record.setValue(initCr.value());
            break;
          }
          log.info(
              "Value: "
                  + initCr.value()
                  + ", initCr.Offset: "
                  + String.valueOf(initCr.offset())
                  + ", timestamp:"
                  + initCr.timestamp());
        }
      }
    } catch (Exception exception) {
      throw new ApiException(
          "Consume "
              + topic
              + "-"
              + partition
              + " offset:"
              + offset
              + " using "
              + decoder
              + " exception. "
              + exception.getLocalizedMessage());
    } finally {
      consumer.close();
    }

    return record;
  }

  private boolean isTopicPartitionValid(String topic, int partition) {
    TopicMeta topicMeta = describeTopic(topic);

    for (CustomTopicPartitionInfo topicPartitionInfo : topicMeta.getTopicPartitionInfos()) {
      if (topicPartitionInfo.getTopicPartitionInfo().partition() == partition) {
        return true;
      }
    }

    return false;
  }

  public void checkOffsetValid(String topic, int partition, long offset) {
    long beginningOffset = getBeginningOffset(topic, partition);
    long endOffset = getEndOffset(topic, partition);

    log.info(
        "Topic:"
            + topic
            + ", partition:"
            + partition
            + " begin offset:"
            + beginningOffset
            + ", end offset:"
            + endOffset);
    if (beginningOffset == endOffset) {
      throw new ApiException("There is no message in this partition of this topic");
    }
    if (offset < beginningOffset || offset >= endOffset) {
      log.error(offset + " error");
      throw new ApiException(
          "offsets must be between " + String.valueOf(beginningOffset + " and " + (endOffset - 1)));
    }
  }

  public Record getAvroRecordByOffset(String topic, int partition, long offset, String avroSchema) {
    TopicPartition tp = new TopicPartition(topic, partition);
    KafkaConsumer consumer = null;
    try {
      consumer =
          kafkaUtils.createNewConsumer(
              String.valueOf(System.currentTimeMillis()),
              "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    } catch (Exception exception) {
      log.error("Create consumer exception." + exception);
    }
    consumer.assign(Collections.singletonList(tp));
    consumer.seek(tp, offset);

    Record record =
        Record.builder().topic(topic).type(byte[].class).decoder("AvorDeserializer").build();

    try {
      ConsumerRecords<byte[], byte[]> crs = consumer.poll(3000);
      log.info(
          "Seek to offset:"
              + offset
              + ", topic:"
              + topic
              + ", partition:"
              + partition
              + ", crs.count:"
              + crs.count());
      if (crs.count() != 0) {
        Iterator<ConsumerRecord<byte[], byte[]>> it = crs.iterator();
        while (it.hasNext()) {
          ConsumerRecord<byte[], byte[]> initCr = it.next();
          if (initCr.offset() == offset) {
            record.setOffset(offset);
            record.setTimestamp(initCr.timestamp());
            record.setKey(initCr.key());
            record.setValue(avroDeserialize(initCr.value(), avroSchema));
            break;
          }
          log.info(
              "Value: "
                  + initCr.value()
                  + ", initCr.Offset: "
                  + String.valueOf(initCr.offset())
                  + ", timestamp:"
                  + initCr.timestamp());
        }
      }
    } catch (Exception exception) {
      throw new ApiException(
          "Consume "
              + topic
              + "-"
              + partition
              + " offset:"
              + offset
              + " using "
              + "ByteArrayDeserializer"
              + " exception. "
              + exception.getLocalizedMessage());
    } finally {
      consumer.close();
    }

    return record;
  }

  private Object avroDeserialize(byte[] bytes, String avroSchema) {
    Schema schema = new Schema.Parser().parse(avroSchema);
    DatumReader reader = new GenericDatumReader<GenericRecord>(schema);
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    Object object = null;
    try {
      object =
          reader.read(
              null, DecoderFactory.get().binaryDecoder(buffer.array(), 0, bytes.length, null));
    } catch (IOException exception) {
      throw new ApiException("Avro Deserialize exception. " + exception);
    }

    return object;
  }

  public GeneralResponse resetOffset(
      @TopicExistConstraint String topic,
      int partition,
      String consumerGroup,
      ConsumerType type,
      String offset) {
    KafkaConsumer consumer = null;
    if (type != null && type == ConsumerType.NEW) {
      if (!isNewConsumerGroup(consumerGroup)) {
        return GeneralResponse.builder()
            .state(GeneralResponseState.failure)
            .msg("New consumer group:" + consumerGroup + " non-exists!")
            .build();
      }
    }

    if (type != null && type == ConsumerType.OLD) {
      if (!isOldConsumerGroup(consumerGroup)) {
        return GeneralResponse.builder()
            .state(GeneralResponseState.failure)
            .msg("Old consumer group:" + consumerGroup + " non-exists!")
            .build();
      }
    }

    if (!isTopicPartitionValid(topic, partition)) {
      return GeneralResponse.builder()
          .state(GeneralResponseState.failure)
          .msg("Topic:" + topic + " has no partition:" + partition)
          .build();
    }

    long offsetToBeReset = -1;
    long beginningOffset = getBeginningOffset(topic, partition);
    long endOffset = getEndOffset(topic, partition);

    log.info("To tell the consumergroup " + consumerGroup + " is active now");
    if (isConsumerGroupActive(consumerGroup, type)) {
      return GeneralResponse.builder()
          .state(GeneralResponseState.failure)
          .msg("Offsets can only be reset if the group " + consumerGroup + " is inactive")
          .build();
    }

    if (type != null && type == ConsumerType.NEW && isNewConsumerGroup(consumerGroup)) {
      try {
        log.info("The consumergroup " + consumerGroup + " is new. Reset offset now");
        consumer = kafkaUtils.createNewConsumer(consumerGroup);
        // if type is new or the consumergroup itself is new
        TopicPartition tp = new TopicPartition(topic, partition);
        consumer.assign(Arrays.asList(tp));
        consumer.poll(channelSocketTimeoutMs);
        if (offset.equals("earliest")) {
          consumer.seekToBeginning(Arrays.asList(tp));
          offsetToBeReset = beginningOffset;
          log.info(
              "Reset offset for consumer group:"
                  + consumerGroup
                  + " on "
                  + topic
                  + "-"
                  + partition
                  + " to "
                  + consumer.position(tp));
        } else if (offset.equals("latest")) {
          consumer.seekToEnd(Arrays.asList(tp));
          offsetToBeReset = endOffset;
          log.info(
              "Reset offset for consumer group:"
                  + consumerGroup
                  + " on "
                  + topic
                  + "-"
                  + partition
                  + " to "
                  + consumer.position(tp));
        } else if (isDateTime(offset)) {
          // Reset offset by time
          SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
          Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
          try {
            timestampsToSearch.put(tp, sdf.parse(offset).getTime());
            Map<TopicPartition, OffsetAndTimestamp> results =
                consumer.offsetsForTimes(timestampsToSearch);
            OffsetAndTimestamp offsetAndTimestamp = results.get(tp);
            if (offsetAndTimestamp != null) {
              offsetToBeReset = offsetAndTimestamp.offset();
              log.info(
                  "Reset consumer group:"
                      + consumerGroup
                      + " offset by time. Reset to offset:"
                      + offsetAndTimestamp.offset()
                      + ", timestamp:"
                      + offsetAndTimestamp.timestamp()
                      + ", timestampToDate:"
                      + sdf.format(new Date(offsetAndTimestamp.timestamp())));
              consumer.seek(tp, offsetToBeReset);
            } else {
              return GeneralResponse.builder()
                  .state(GeneralResponseState.failure)
                  .msg(
                      "No offset's timestamp is greater than or equal to the given timestamp:"
                          + offset)
                  .build();
            }
          } catch (ParseException parseException) {
            return GeneralResponse.builder()
                .state(GeneralResponseState.failure)
                .msg("Invalid offset format. Date format should be yyyy-MM-dd HH:mm:ss.SSS .")
                .build();
          }
        } else {
          if (Long.parseLong(offset) < beginningOffset || Long.parseLong(offset) > endOffset) {
            log.warn(offset + " error");
            return GeneralResponse.builder()
                .state(GeneralResponseState.failure)
                .msg(
                    "Invalid request offset:"
                        + offset
                        + ". Topic "
                        + topic
                        + "'s beginning offset:"
                        + beginningOffset
                        + ", endoffset:"
                        + endOffset)
                .build();
          }
          offsetToBeReset = Long.parseLong(offset);
          consumer.seek(tp, offsetToBeReset);
        }
        consumer.commitSync();
      } catch (IllegalStateException e) {
        storage.getMap().remove(consumerGroup);
        return GeneralResponse.builder()
            .state(GeneralResponseState.failure)
            .msg(e.getLocalizedMessage())
            .build();
      } finally {
        if (consumer != null) {
          consumer.close();
        }
      }
    }

    // if type is old or the consumer group itself is old
    if (type != null && type == ConsumerType.OLD && isOldConsumerGroup(consumerGroup)) {
      log.info("The consumergroup " + consumerGroup + " is old. Reset offset now");
      if (offset.equals("earliest")) {
        offsetToBeReset = beginningOffset;
      } else if (offset.equals("latest")) {
        offsetToBeReset = endOffset;
      } else {
        try {
          if (Long.parseLong(offset) < beginningOffset || Long.parseLong(offset) > endOffset) {
            log.info("Setting offset to " + offset + " error");
            return GeneralResponse.builder()
                .state(GeneralResponseState.failure)
                .msg(
                    "Invalid request offset:"
                        + offset
                        + ". Topic "
                        + topic
                        + "'s beginning offset:"
                        + beginningOffset
                        + ", endoffset:"
                        + endOffset)
                .build();
          }
          log.info("Offset will be reset to " + offset);
          zkUtils = zookeeperUtils.getZkUtils();
          offsetToBeReset = Long.parseLong(offset);
          zkUtils
              .zkClient()
              .writeData(
                  "/consumers/" + consumerGroup + "/offsets/" + topic + "/" + partition, offset);
        } catch (Exception e) {
          return GeneralResponse.builder()
              .state(GeneralResponseState.failure)
              .msg(e.getLocalizedMessage())
              .build();
        }
      }
    }
    return GeneralResponse.builder()
        .state(GeneralResponseState.success)
        .msg("Reset the offset successfully!")
        .data(Long.toString(offsetToBeReset))
        .build();
  }

  private boolean isDateTime(String offset) {
    String patternStr = "\\d\\d\\d\\d-[0-1]\\d-[0-3]\\d\\s+[0-2]\\d:[0-5]\\d:[0-5]\\d\\.\\d\\d\\d";
    Pattern timePattern = Pattern.compile(patternStr);
    return timePattern.matcher(offset).find();
  }

  public Map<String, Map<Integer, Long>> getLastCommitTime(
      @ConsumerGroupExistConstraint String consumerGroup,
      @TopicExistConstraint String topic,
      ConsumerType type) {
    Map<String, Map<Integer, Long>> result = new ConcurrentHashMap<>();

    if (type != null && type == ConsumerType.OLD) {
      CuratorFramework zkClient = zookeeperUtils.createZkClient();
      // Get Old Consumer commit time
      try {
        Map<Integer, Long> oldConsumerOffsetMap = new ConcurrentHashMap<>();
        if (zkClient.checkExists().forPath(CONSUMERPATHPREFIX + consumerGroup) != null
            && zkClient
            .checkExists()
            .forPath(CONSUMERPATHPREFIX + consumerGroup + OFFSETSPATHPREFIX + topic)
            != null) {
          List<String> offsets =
              zkClient
                  .getChildren()
                  .forPath(CONSUMERPATHPREFIX + consumerGroup + OFFSETSPATHPREFIX + topic);
          for (String offset : offsets) {
            Integer id = Integer.valueOf(offset);
            long mtime =
                zkClient
                    .checkExists()
                    .forPath(
                        CONSUMERPATHPREFIX
                            + consumerGroup
                            + OFFSETSPATHPREFIX
                            + topic
                            + "/"
                            + offset)
                    .getMtime();
            oldConsumerOffsetMap.put(id, mtime);
          }

          result.put("old", oldConsumerOffsetMap);
        }
      } catch (Exception e) {
        log.warn(
            "Get last commit time for consumergroup:"
                + consumerGroup
                + " failed. "
                + e.getLocalizedMessage());
      }
    } else {
      //      Get New consumer commit time, from offset storage instance
      // TODO find a solution a replace the storage
      if (storage.get(consumerGroup) != null) {
        Map<GroupTopicPartition, kafka.common.OffsetAndMetadata> storageResult =
            storage.get(consumerGroup);
        result.put(
            "new",
            (storageResult
                .entrySet()
                .parallelStream()
                .filter(s -> s.getKey().topicPartition().topic().equals(topic))
                .collect(
                    Collectors.toMap(
                        s -> s.getKey().topicPartition().partition(),
                        s -> {
                          if (s.getValue() != null) {
                            return s.getValue().commitTimestamp();
                          } else {
                            return -1L;
                          }
                        }))));
      }
    }

    return result;
  }

  public GeneralResponse deleteConsumerGroup(String consumerGroup, ConsumerType type) {
    if (type == ConsumerType.OLD && !isOldConsumerGroup(consumerGroup)) {
      return GeneralResponse.builder()
          .state(GeneralResponseState.failure)
          .msg("Old consumer group:" + consumerGroup + " non-exist")
          .build();
    }
    if (type == ConsumerType.NEW && !isNewConsumerGroup(consumerGroup)) {
      return GeneralResponse.builder()
          .state(GeneralResponseState.failure)
          .msg("New consumer group:" + consumerGroup + " non-exist")
          .build();
    }
    if (type == ConsumerType.OLD) {
      zkUtils = zookeeperUtils.getZkUtils();
      if (!AdminUtils.deleteConsumerGroupInZK(zkUtils, consumerGroup)) {
        return GeneralResponse.builder()
            .state(GeneralResponseState.failure)
            .msg("The consumer " + consumerGroup + " is still active.Please stop it first")
            .build();
      }
    } else if (type == ConsumerType.NEW) {
      AdminClient adminClient = kafkaUtils.createAdminClient();
      List<String> groups = new ArrayList<>();
      groups.add(consumerGroup);

      scala.collection.immutable.List<String> groupsList =
          JavaConverters.asScalaBufferConverter(groups).asScala().toList();
      scala.collection.immutable.Map<String, Errors> stringErrorsMap =
          adminClient.deleteConsumerGroups((scala.collection.immutable.List) (groupsList));

      adminClient.close();
      if (!stringErrorsMap.get(consumerGroup).get().equals(Errors.NONE)) {
        log.info("Consumer group:"
            + consumerGroup
            + " could not be deleted. Error Code:"
            + stringErrorsMap.get(consumerGroup).get()
            + ". Error msg:"
            + stringErrorsMap.get(consumerGroup).get().exception());
        return GeneralResponse.builder()
            .state(GeneralResponseState.failure)
            .msg(
                "Consumer group:"
                    + consumerGroup
                    + " could not be deleted. Error Code:"
                    + stringErrorsMap.get(consumerGroup).get()
                    + ". Error msg:"
                    + stringErrorsMap.get(consumerGroup).get().exception())
            .build();
      }
    }

    log.info("Consumer group:" + consumerGroup + " has been deleted.");

    return GeneralResponse.builder()
        .state(GeneralResponseState.success)
        .msg("Consumer group:" + consumerGroup + " has been deleted.")
        .data(consumerGroup)
        .build();
  }

  private List<TopicAndPartition> getTopicPartitions(String t) {
    List<TopicAndPartition> tpList = new ArrayList<>();
    List<String> l = Arrays.asList(t);
    zkUtils = zookeeperUtils.getZkUtils();
    java.util.Map<String, Seq<Object>> tpMap =
        JavaConverters.mapAsJavaMapConverter(
            zkUtils.getPartitionsForTopics(
                JavaConverters.asScalaIteratorConverter(l.iterator()).asScala().toSeq()))
            .asJava();
    if (tpMap != null) {
      ArrayList<Object> partitionLists =
          new ArrayList<>(JavaConverters.seqAsJavaListConverter(tpMap.get(t)).asJava());
      tpList =
          partitionLists.stream().map(p -> new TopicAndPartition(t, (Integer) p)).collect(toList());
    }
    return tpList;
  }

  private long getOffsets(Node leader, String topic, int partitionId, long time) {
    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partitionId);

    SimpleConsumer consumer =
        new SimpleConsumer(leader.host(), leader.port(), 10000, 1024, "Kafka-zk-simpleconsumer");

    PartitionOffsetRequestInfo partitionOffsetRequestInfo =
        new PartitionOffsetRequestInfo(time, 10000);
    OffsetRequest offsetRequest =
        new OffsetRequest(
            ImmutableMap.of(topicAndPartition, partitionOffsetRequestInfo),
            kafka.api.OffsetRequest.CurrentVersion(),
            consumer.clientId());
    OffsetResponse offsetResponse = consumer.getOffsetsBefore(offsetRequest);

    if (offsetResponse.hasError()) {
      short errorCode = offsetResponse.errorCode(topic, partitionId);
      log.warn(format("Offset response has error: %d", errorCode));
      throw new ApiException(
          "could not fetch data from Kafka, error code is '"
              + errorCode
              + "'Exception Message:"
              + offsetResponse.toString());
    }

    long[] offsets = offsetResponse.offsets(topic, partitionId);
    consumer.close();
    return offsets[0];
  }

  boolean partitionLeaderExist(String topic, int partitionId) {
    TopicDescription topicDes = getTopicDescription(topic);
    if (topicDes != null) {
      for(TopicPartitionInfo tpi:topicDes.partitions()) {
        if (tpi.partition() == partitionId) {
          return tpi.leader() != null;
        }
      }
    }
    return false;
  }

  public long getBeginningOffset(String topic, int partitionId) {
    if (partitionLeaderExist(topic, partitionId)) {
      log.info("Getting beginning offset for topic:" + topic + ", partition:" + partitionId);
      KafkaConsumer consumer = kafkaUtils.createNewConsumer(KafkaUtils.DEFAULTCP);
      TopicPartition tp = new TopicPartition(topic, partitionId);
      consumer.assign(Arrays.asList(tp));
      Map<TopicPartition, Long> beginningOffsets =
          consumer.beginningOffsets(Collections.singletonList(tp));
      consumer.close();

      if (beginningOffsets != null) {
        return beginningOffsets.get(tp);
      }

      log.info("End Get beginning offset for topic:" + topic + ", partition:" + partitionId);
    }
    return -1;
  }

  public long getEndOffset(String topic, int partitionId) {
    if (partitionLeaderExist(topic, partitionId)) {
      log.info("Getting end offset for topic:" + topic + ", partition:" + partitionId);
      KafkaConsumer consumer = kafkaUtils.createNewConsumer(KafkaUtils.DEFAULTCP);
      TopicPartition tp = new TopicPartition(topic, partitionId);
      consumer.assign(Arrays.asList(tp));
      Map<TopicPartition, Long> endOffsets = consumer.endOffsets(Collections.singletonList(tp));
      consumer.close();

      log.info("End Get end offset for topic:" + topic + ", partition:" + partitionId);
      if (endOffsets != null) {
        return endOffsets.get(tp);
      }
    }
    return -1;
  }

  private long getEndOffset(Node leader, String topic, int partitionId) {
    return getOffsets(leader, topic, partitionId, kafka.api.OffsetRequest.LatestTime());
  }

  private boolean isConsumerGroupActive(String consumerGroup, ConsumerType type) {
    if (type == ConsumerType.NEW) {
      ConsumerGroupMeta groupMeta = getConsumerGroupMeta(consumerGroup);
      ConsumerGroupState groupState = groupMeta.getState();
      if (groupState.equals(ConsumerGroupState.STABLE)
          || groupState.equals(ConsumerGroupState.PREPARING_REBALANCE)
          || groupState.equals(ConsumerGroupState.COMPLETING_REBALANCE)) {
        return true;
      }
      if (groupState.equals(ConsumerGroupState.EMPTY)
          || groupState.equals(ConsumerGroupState.DEAD)) {
        return false;
      }
      throw new ApiException(
          "Consumer group:" + consumerGroup + " state:" + groupState + " unkown.");
    } else if (type == ConsumerType.OLD) {
      zkUtils = zookeeperUtils.getZkUtils();
      return zkUtils.getConsumersInGroup(consumerGroup).nonEmpty();
    } else {
      throw new ApiException("Unknown type " + type);
    }
  }

  public HealthCheckResult healthCheck() {
    String healthCheckTopic = kafkaConfig.getHealthCheckTopic();
    HealthCheckResult healthCheckResult = new HealthCheckResult();
    KafkaProducer producer = kafkaUtils.createProducer();
    int partitionId;
    long offset;

    boolean healthCheckTopicExist = existTopic(healthCheckTopic);
    log.info("HealthCheckTopic:" + healthCheckTopic + " existed:" + healthCheckTopicExist);
    if (!healthCheckTopicExist) {
      healthCheckResult.setStatus("unknown");
      healthCheckResult.setMsg(
          "HealthCheckTopic: "
              + healthCheckTopic
              + " Non-Exist. Please create it before doing health check.");
      return healthCheckResult;
    }

    String message = "health check_" + System.currentTimeMillis();
    ProducerRecord<String, String> record = new ProducerRecord(healthCheckTopic, null, message);
    log.info("Generate message:" + message);
    try {
      RecordMetadata recordMetadata = (RecordMetadata) producer.send(record).get();
      partitionId = recordMetadata.partition();
      offset = recordMetadata.offset();
      log.info(
          "Message:"
              + message
              + " has been sent to Partition:"
              + partitionId
              + ", offset:"
              + offset);
    } catch (Exception e) {
      healthCheckResult.setStatus("error");
      healthCheckResult.setMsg(
          "Health Check: Produce Message Failure. Exception: " + e.getMessage());
      log.error("Health Check: Produce Message Failure.", e);
      return healthCheckResult;
    } finally {
      producer.close();
    }

    KafkaConsumer consumer = kafkaUtils.createNewConsumer();
    TopicPartition topicPartition = new TopicPartition(healthCheckTopic, partitionId);
    consumer.assign(Arrays.asList(topicPartition));
    consumer.seek(topicPartition, offset);
    int retries = 30;
    int noRecordsCount = 0;
    while (true) {
      final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
      if (consumerRecords.count() == 0) {
        noRecordsCount++;
        if (noRecordsCount > retries) {
          break;
        } else {
          continue;
        }
      }
      Iterator<ConsumerRecord<Long, String>> iterator = consumerRecords.iterator();
      while (iterator.hasNext()) {
        ConsumerRecord msg = iterator.next();
        log.info("Health Check: Fetch Message " + msg.value() + ", offset:" + msg.offset());
        if (msg.value().equals(message)) {
          healthCheckResult.setStatus("ok");
          healthCheckResult.setMsg(message);
          return healthCheckResult;
        }
      }
      consumer.commitAsync();
    }
    consumer.close();

    if (healthCheckResult.getStatus() == null) {
      healthCheckResult.setStatus("error");
      healthCheckResult.setMsg(
          "Health Check: Consume Message Failure. Consumer can't fetch the message.");
    }
    return healthCheckResult;
  }
}
