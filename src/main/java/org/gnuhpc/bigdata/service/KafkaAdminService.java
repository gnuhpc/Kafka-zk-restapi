package org.gnuhpc.bigdata.service;

import static java.lang.String.format;
import static java.sql.DriverManager.println;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.apache.coyote.http11.Constants.A;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import io.swagger.annotations.Api;
import io.swagger.models.auth.In;
import java.io.IOException;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import kafka.admin.AdminClient;
import kafka.admin.AdminClient.ConsumerGroupSummary;
import kafka.admin.AdminClient.ConsumerSummary;
import kafka.admin.AdminUtils;
import kafka.admin.ConsumerGroupCommand;
import kafka.admin.RackAwareMode;
import kafka.admin.ReassignPartitionsCommand;
import kafka.admin.ReassignPartitionsCommand.Throttle;
import kafka.admin.ReassignmentStatus;
import kafka.admin.TopicCommand;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.coordinator.group.GroupOverview;
import kafka.coordinator.group.GroupTopicPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.utils.ZkUtils;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import lombok.extern.log4j.Log4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.protocol.Errors;
import org.gnuhpc.bigdata.CollectionConvertor;
import org.gnuhpc.bigdata.componet.OffsetStorage;
import org.gnuhpc.bigdata.config.KafkaConfig;
import org.gnuhpc.bigdata.constant.ConsumerGroupState;
import org.gnuhpc.bigdata.constant.ConsumerType;
import org.gnuhpc.bigdata.constant.GeneralResponseState;
import org.gnuhpc.bigdata.model.AddPartition;
import org.gnuhpc.bigdata.model.BrokerInfo;
import org.gnuhpc.bigdata.model.ConsumerGroupDesc;
import org.gnuhpc.bigdata.model.ConsumerGroupMeta;
import org.gnuhpc.bigdata.model.CustomTopicPartitionInfo;
import org.gnuhpc.bigdata.model.GeneralResponse;
import org.gnuhpc.bigdata.model.HealthCheckResult;
import org.gnuhpc.bigdata.model.MemberDescription;
import org.gnuhpc.bigdata.model.PartitionAssignmentState;
import org.gnuhpc.bigdata.model.ReassignWrapper;
import org.gnuhpc.bigdata.model.Record;
import org.gnuhpc.bigdata.model.TopicBrief;
import org.gnuhpc.bigdata.model.TopicDetail;
import org.gnuhpc.bigdata.model.TopicMeta;
import org.gnuhpc.bigdata.utils.KafkaUtils;
import org.gnuhpc.bigdata.utils.ZookeeperUtils;
import org.gnuhpc.bigdata.validator.ConsumerGroupExistConstraint;
import org.gnuhpc.bigdata.validator.TopicExistConstraint;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.method.support.HandlerMethodReturnValueHandler;
import scala.Function0;
import scala.Int;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.mutable.ListBuffer;
import scala.reflect.internal.Trees.New;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

/**
 * Created by gnuhpc on 2017/7/17.
 */

@Service
@Log4j
@Validated
public class KafkaAdminService {

  private static final int channelSocketTimeoutMs = 600;
  private static final int channelRetryBackoffMs = 600;
  private static final long kafkaAdminClientGetTimeoutMs = 30000;
  private static final long kafkaAdminClientAlterTimeoutMs = 60000;
  private static final String CONSUMERPATHPREFIX = "/consumers/";
  private static final String OFFSETSPATHPREFIX = "/offsets/";
  @Autowired
  private ZookeeperUtils zookeeperUtils;

  @Autowired
  private KafkaUtils kafkaUtils;

  @Autowired
  private KafkaConfig kafkaConfig;

  @Autowired
  private OffsetStorage storage;

  //For AdminUtils use
  private ZkUtils zkUtils;

  //For zookeeper connection
  private CuratorFramework zkClient;

  private KafkaZkClient kafkaZkClient;

  //  private org.apache.kafka.clients.admin.AdminClient kafkaAdminClient;
  //For Json serialized
  private Gson gson;

  private scala.Option<String> NONE = scala.Option.apply(null);

  //TODO use autocloseable interface
    /*
    public KafkaAdminService(final KafkaZkClient kafkaZkClient) {
        this.kafkaZkClient = kafkaZkClient;
    }

    public KafkaAdminService(final org.apache.kafka.clients.admin.AdminClient kafkaAdminClient) {
        this.kafkaAdminClient = kafkaAdminClient;
    }

    public KafkaAdminService(final KafkaZkClient kafkaZkClient, final org.apache.kafka.clients.admin.AdminClient kafkaAdminClient) {
        this.kafkaZkClient = kafkaZkClient;
        this.kafkaAdminClient = kafkaAdminClient;
    } */

  @PostConstruct
  private void init() {
    this.zkUtils = zookeeperUtils.getZkUtils();
    this.zkClient = zookeeperUtils.getCuratorClient();
    this.kafkaZkClient = zookeeperUtils.getKafkaZkClient();
//    Properties adminClientProp = new Properties();
//    adminClientProp.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBrokers());
//    //TODO eliminate the init connection
//    kafkaAdminClient = KafkaAdminClient.create(adminClientProp);
    GsonBuilder builder = new GsonBuilder();
    builder.registerTypeAdapter(DateTime.class,
        (JsonDeserializer<DateTime>) (jsonElement, type, jsonDeserializationContext) -> new DateTime(
            jsonElement.getAsJsonPrimitive().getAsLong()));

    this.gson = builder.create();
  }

  private org.apache.kafka.clients.admin.AdminClient createKafkaAdminClient() {
    Properties adminClientProp = new Properties();
    adminClientProp.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBrokers());
    return KafkaAdminClient.create(adminClientProp);
  }

  //TODO use autocloseable interface
    /*
    @Override
    public void close() {
        System.out.println("++++++++Kafka Admin Service close ++++++++");
        kafkaAdminClient.close();
        kafkaZkClient.close();
        zkClient.close();
    }
    */
  public TopicMeta createTopic(TopicDetail topic, String reassignStr) {
    if (Topic.hasCollisionChars(topic.getName())) {
      throw new InvalidTopicException("Invalid topic name, it contains '.' or '_'.");
    }

    if (Strings.isNullOrEmpty(reassignStr) && topic.getPartitions() <= 0) {
      throw new InvalidTopicException("Number of partitions must be larger than 0");
    }
    Topic.validate(topic.getName());

    if (Strings.isNullOrEmpty(reassignStr)) {
      AdminUtils.createTopic(zkUtils,
          topic.getName(), topic.getPartitions(), topic.getFactor(),
          topic.getProp(), RackAwareMode.Enforced$.MODULE$);
    } else {
      List<String> argsList = new ArrayList<>();
      argsList.add("--topic");
      argsList.add(topic.getName());

      if (topic.getProp().stringPropertyNames().size() != 0) {
        argsList.add("--config");

        for (String key : topic.getProp().stringPropertyNames()) {
          argsList.add(key + "=" + topic.getProp().get(key));
        }
      }
      argsList.add("--replica-assignment");
      argsList.add(reassignStr);

      TopicCommand.createTopic(kafkaZkClient,
          new TopicCommand.TopicCommandOptions(argsList.stream().toArray(String[]::new)));
    }

    try {
      //Wait for a second for metadata propergating
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return describeTopic(topic.getName());
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

    kafkaAdminClient.close();
    return topicNames;
  }

  public List<TopicBrief> listTopicBrief() {
    org.apache.kafka.clients.admin.AdminClient kafkaAdminClient = createKafkaAdminClient();

    DescribeTopicsResult describeTopicsResult = kafkaAdminClient.describeTopics(listTopics());
    Map<String, TopicDescription> topicMap;
    List<TopicBrief> result;
    try {
      topicMap = describeTopicsResult.all()
          .get(kafkaAdminClientGetTimeoutMs, TimeUnit.MILLISECONDS);
      result = topicMap.entrySet().parallelStream().map(e -> {
        String topic = e.getKey();
        TopicDescription topicDescription = e.getValue();
        List<org.apache.kafka.common.TopicPartitionInfo> topicPartitionInfoList = topicDescription
            .partitions();
        int replicateCount = 0;
        int isrCount = 0;
        for (org.apache.kafka.common.TopicPartitionInfo topicPartitionInfo : topicPartitionInfoList) {
          replicateCount += topicPartitionInfo.replicas().size();
          isrCount += topicPartitionInfo.isr().size();
        }
        if (replicateCount == 0) {
          return new TopicBrief(topic, topicDescription.partitions().size(), 0);
        } else {
          return new TopicBrief(topic, topicDescription.partitions().size(),
              ((double) isrCount / replicateCount));
        }
      }).collect(toList());
    } catch (Exception exception) {
      log.warn("Describe all topics exception:" + exception);
      throw new ApiException("Describe all topics exception:" + exception);
    }

    kafkaAdminClient.close();
    return result;
  }

  public boolean existTopic(String topicName) {
    return kafkaZkClient.topicExists(topicName);
  }

  public List<BrokerInfo> listBrokers() {
    List<Broker> brokerList = CollectionConvertor
        .seqConvertJavaList(kafkaZkClient.getAllBrokersInCluster());

    return brokerList.parallelStream().collect(Collectors.toMap(Broker::id, Broker::rack))
        .entrySet().parallelStream()
        .map(entry -> {
          String brokerInfoStr = null;
          try {
            //TODO replace zkClient with kafkaZKClient
            brokerInfoStr = new String(
                zkClient.getData().forPath(ZkUtils.BrokerIdsPath() + "/" + entry.getKey())
            );
          } catch (Exception e) {
            e.printStackTrace();
          }
          BrokerInfo brokerInfo = gson.fromJson(brokerInfoStr, BrokerInfo.class);
          if (entry.getValue().isEmpty()) {
            brokerInfo.setRack("");
          } else {
            brokerInfo.setRack(entry.getValue().get());
          }
          brokerInfo.setId(entry.getKey());
          return brokerInfo;
        }).collect(toList());
  }

  public int getControllerId() {
    int controllerId = -1;
    if (!kafkaZkClient.getControllerId().equals(NONE)) {
      controllerId = (int) kafkaZkClient.getControllerId().get();
    }

    return controllerId;
  }

  public TopicMeta describeTopic(@TopicExistConstraint String topicName) {
    org.apache.kafka.clients.admin.AdminClient kafkaAdminClient = createKafkaAdminClient();

    DescribeTopicsResult describeTopicsResult = kafkaAdminClient
        .describeTopics(Collections.singletonList(topicName));
    TopicMeta topicMeta = new TopicMeta(topicName);
    try {
      Map<String, TopicDescription> topicMap = describeTopicsResult.all()
          .get(kafkaAdminClientGetTimeoutMs, TimeUnit.MILLISECONDS);
      if (topicMap.containsKey(topicName)) {
        TopicDescription topicDescription = topicMap.get(topicName);
        List<TopicPartitionInfo> tmList = topicDescription.partitions();
        topicMeta.setInternal(topicDescription.isInternal());
        topicMeta.setPartitionCount(topicDescription.partitions().size());
        topicMeta.setReplicationFactor(tmList.get(0).replicas().size());
        topicMeta.setTopicPartitionInfos(tmList.parallelStream().map(
            tm -> {
              CustomTopicPartitionInfo customTopicPartitionInfo = new CustomTopicPartitionInfo();
              customTopicPartitionInfo.setTopicPartitionInfo(tm);
              customTopicPartitionInfo.setIn_sync();
              customTopicPartitionInfo
                  .setStartOffset(getBeginningOffset(topicName, tm.partition()));
              customTopicPartitionInfo
                  .setEndOffset(getEndOffset(tm.leader(), topicName, tm.partition()));
              customTopicPartitionInfo.setMessageAvailable();
              return customTopicPartitionInfo;

            }).collect(toList())
        );
        Collections.sort(topicMeta.getTopicPartitionInfos());
      }
    } catch (Exception exception) {
      exception.printStackTrace();
      throw new ApiException("Describe topic exception." + exception);
    }

    kafkaAdminClient.close();
    return topicMeta;
  }

  public Map<String, GeneralResponse> deleteTopicList(List<String> topicList) {
    org.apache.kafka.clients.admin.AdminClient kafkaAdminClient = createKafkaAdminClient();
    HashMap<String, GeneralResponse> deleteResults = new HashMap<>();

    log.warn("Delete topic " + topicList);
    DeleteTopicsResult deleteTopicsResult = kafkaAdminClient.deleteTopics(topicList);
    try {
      deleteTopicsResult.all().get(kafkaAdminClientGetTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception exception) {
      log.warn("Delete topic exception:" + exception);
    } finally {
      deleteTopicsResult.values().forEach((topic, result) -> {
        GeneralResponse generalResponse;
        if (result.isDone() && !result.isCompletedExceptionally()) {
          generalResponse = GeneralResponse.builder().state(GeneralResponseState.success).build();
        } else {
          generalResponse = GeneralResponse.builder().state(GeneralResponseState.failure)
              .msg(result.toString()).build();
        }
        deleteResults.put(topic, generalResponse);
      });
    }

    kafkaAdminClient.close();
    return deleteResults;
  }

  public Collection<ConfigEntry> describeConfig(ConfigResource.Type type, String name) {
    Map<ConfigResource, Config> configs;
    org.apache.kafka.clients.admin.AdminClient kafkaAdminClient = createKafkaAdminClient();
    ConfigResource configResource = new ConfigResource(type, name);

    DescribeConfigsResult ret = kafkaAdminClient
        .describeConfigs(Collections.singleton(configResource));
    try {
      configs = ret.all().get(kafkaAdminClientGetTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception exception) {
      log.warn("Describe config type:" + type + ", name:" + name + " exception:" + exception);
      throw new ApiException("Describe config exception:" + exception.getLocalizedMessage());
    }

    kafkaAdminClient.close();
    return configs.get(configResource).entries();
  }

  public boolean alterConfig(ConfigResource.Type type, String name,
      Collection<ConfigEntry> configEntries) {
    org.apache.kafka.clients.admin.AdminClient kafkaAdminClient = createKafkaAdminClient();
    Config config = new Config(configEntries);
    AlterConfigsResult alterConfigsResult = kafkaAdminClient
        .alterConfigs(Collections.singletonMap(new ConfigResource(type, name), config));

    try {
      alterConfigsResult.all().get(kafkaAdminClientAlterTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception exception) {
      log.warn("Alter config type:" + type + ", name:" + name + " exception:" + exception);
      return false;
    }

    kafkaAdminClient.close();

    return true;
  }

  public Collection<ConfigEntry> updateTopicConf(@TopicExistConstraint String topic,
      Properties props) {
    Collection<ConfigEntry> configEntries = props.entrySet().stream().map(
        e -> new ConfigEntry(e.getKey().toString(), e.getValue().toString()))
        .collect(Collectors.toList());
    alterConfig(ConfigResource.Type.TOPIC, topic, configEntries);

    return describeConfig(ConfigResource.Type.TOPIC, topic);
  }

  public Collection<ConfigEntry> getTopicConf(@TopicExistConstraint String topic) {
    return describeConfig(ConfigResource.Type.TOPIC, topic);
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

  public Collection<ConfigEntry> updateTopicConfByKey(@TopicExistConstraint String topic,
      String key, String value) {
    alterConfig(ConfigResource.Type.TOPIC, topic,
        Collections.singletonList(new ConfigEntry(key, value)));

    return describeConfig(ConfigResource.Type.TOPIC, topic);
  }

  public Map<String, Set<String>> listAllConsumerGroups(ConsumerType type) {
    Map<String, Set<String>> result = new HashMap<>();

    if (type == null || type == ConsumerType.OLD) {
      Set<String> oldCGList = listAllOldConsumerGroups();
      if (oldCGList.size() != 0) {
        result.put("old", oldCGList);
      }
    }

    if (type == null || type == ConsumerType.NEW) {
      Set<String> newCGList = listAllNewConsumerGroups();
      if (newCGList.size() != 0) {
        result.put("new", newCGList);
      }
    }

    return result;
  }

  private Set<String> listAllNewConsumerGroups() {
    AdminClient adminClient = kafkaUtils.createAdminClient();
    log.info("Calling the listAllConsumerGroupsFlattened");
    Set activeGroups = CollectionConvertor
        .seqConvertJavaList(adminClient.listAllConsumerGroupsFlattened()).stream()
        .map(GroupOverview::groupId).collect(toSet());

    log.info("Finish getting new consumers");
    adminClient.close();
    return activeGroups;
  }

  private Set<String> listAllOldConsumerGroups() {
    log.info("Finish getting old consumers");
    return CollectionConvertor
        .seqConvertJavaList(kafkaZkClient.getChildren(ZkUtils.ConsumersPath())).stream()
        .collect(toSet());
  }

  public Map<String, Set<String>> listConsumerGroupsByTopic(
      @TopicExistConstraint String topic,
      ConsumerType type) {
    Map<String, Set<String>> result = new HashMap<>();

    if (type == null || type == ConsumerType.OLD) {
      Set<String> oldCGList = null;
      try {
        oldCGList = listOldConsumerGroupsByTopic(topic);
      } catch (Exception e) {
        e.printStackTrace();
      }
      if (oldCGList.size() != 0) {
        result.put("old", oldCGList);
      }
    }

    if (type == null || type == ConsumerType.NEW) {
      Set<String> newCGList = listNewConsumerGroupsByTopic(topic);

      if (newCGList.size() != 0) {
        result.put("new", newCGList);
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
    List<String> consumersFromZk = CollectionConvertor
        .seqConvertJavaList(kafkaZkClient.getChildren(ZkUtils.ConsumersPath()));
    Set<String> cList = new HashSet<>();

    for (String consumer : consumersFromZk) {
      String path = ZkUtils.ConsumersPath() + "/" + consumer + "/offsets";
      List<String> topics = CollectionConvertor.seqConvertJavaList(kafkaZkClient.getChildren(path));
      if (topics != null && topics.contains(topic)) {
        cList.add(consumer);
      }
    }

    return cList;
  }

  public Set<String> listTopicsByCG(String consumerGroup, ConsumerType type) {
    Set<String> topicList = new HashSet<>();

    if (type == null) {
      throw new ApiException("Unknown Type " + type);
    }

    if (type == ConsumerType.OLD) {
      if (!isOldConsumerGroup(consumerGroup)) {
        throw new RuntimeException(consumerGroup + " non-exist");
      }
      String path = ZkUtils.ConsumersPath() + "/" + consumerGroup + "/offsets";
      topicList = CollectionConvertor.seqConvertJavaList(kafkaZkClient.getChildren(path)).stream()
          .collect(toSet());
    } else if (type == ConsumerType.NEW) {
      if (!isNewConsumerGroup(consumerGroup)) {
        throw new RuntimeException(consumerGroup + " non-exist!");
      }
      topicList.addAll(getTopicListByConsumerGroup(consumerGroup));
    } else {
      throw new ApiException("Unknown Type " + type);
    }

    return topicList;

  }

  private List<String> getTopicListByConsumerGroup(String consumerGroup) {
    AdminClient adminClient = kafkaUtils.createAdminClient();
    Map<TopicPartition, Object> groupOffsets = CollectionConvertor
        .mapConvertJavaMap(adminClient.listGroupOffsets(consumerGroup));
    adminClient.close();

    return groupOffsets.entrySet().stream()
        .map(topicPartitionObjectEntry -> topicPartitionObjectEntry.getKey().topic())
        .collect(toList());
  }

  public ConsumerGroupMeta getConsumerGroupMeta(String consumerGroup) {
    List<MemberDescription> members = new ArrayList<>();
    AdminClient adminClient = kafkaUtils.createAdminClient();

    ConsumerGroupSummary consumerGroupSummary = adminClient.describeConsumerGroup(consumerGroup, 0);
    List<ConsumerSummary> consumerSummaryList = CollectionConvertor
        .optionListConvertJavaList(consumerGroupSummary.consumers().get());

    ConsumerGroupMeta consumerGroupMeta = ConsumerGroupMeta.builder().groupId(consumerGroup)
        .state(ConsumerGroupState.parse(consumerGroupSummary.state()))
        .assignmentStrategy(consumerGroupSummary.assignmentStrategy())
        .coordinator(consumerGroupSummary.coordinator()).build();

    consumerSummaryList.forEach(consumerSummary -> {
      List<TopicPartition> topicPartitions = CollectionConvertor
          .listConvertJavaList(consumerSummary.assignment());
      members.add(new MemberDescription(consumerSummary.consumerId(),
          consumerSummary.clientId(), consumerSummary.host(), topicPartitions));
    });

    consumerGroupMeta.setMembers(members);
    adminClient.close();

    return consumerGroupMeta;
  }

  public boolean isOldConsumerGroup(String consumerGroup) {
    return listAllOldConsumerGroups().contains(consumerGroup);
  }

  public boolean isNewConsumerGroup(String consumerGroup) {
    //Active Consumergroup or Dead ConsumerGroup is OK
    return (listAllNewConsumerGroups().contains(consumerGroup));
  }

  public Map<String, List<ConsumerGroupDesc>> describeConsumerGroup(String consumerGroup,
      ConsumerType type) {
    Map<String, List<ConsumerGroupDesc>> result = new HashMap<>();
    Set<String> topicList = listTopicsByCG(consumerGroup, type);
    if (topicList == null) {
      //Return empty result
      return result;
    }
    if (type == ConsumerType.NEW) {
      if (!isNewConsumerGroup(consumerGroup)) {
        throw new RuntimeException(consumerGroup + " non-exist!");
      }
      List<PartitionAssignmentState> partitionAssignmentStateList = describeNewConsumerGroup(
          consumerGroup, false, null);
      result = transPasListToMap(consumerGroup, partitionAssignmentStateList, ConsumerType.NEW);
    } else if (type == ConsumerType.OLD) {
      if (!isOldConsumerGroup(consumerGroup)) {
        throw new RuntimeException(consumerGroup + " non-exist");
      }
      List<PartitionAssignmentState> partitionAssignmentStateList = describeOldConsumerGroup(
          consumerGroup, false, null);
      result = transPasListToMap(consumerGroup, partitionAssignmentStateList, ConsumerType.OLD);
    }

    return result;
  }

  private Map<String, List<ConsumerGroupDesc>> transPasListToMap(String consumerGroup,
      List<PartitionAssignmentState> pasList, ConsumerType type) {
    Map<String, List<ConsumerGroupDesc>> result = new HashMap<>();
    ConsumerGroupSummary consumerGroupSummary;

    if (type.equals(ConsumerType.NEW)) {
      AdminClient adminClient = kafkaUtils.createAdminClient();
      consumerGroupSummary = adminClient
          .describeConsumerGroup(consumerGroup, 0);
      adminClient.close();
    } else {
      //Old consumer group has no state, coordinator, assignmentStrategy info
      consumerGroupSummary = null;
    }

    pasList.forEach(partitionAssignmentState -> {
      String topic = partitionAssignmentState.getTopic();
      List<ConsumerGroupDesc> consumerGroupDescs;
      if (result.containsKey(topic)) {
        consumerGroupDescs = result.get(topic);
      } else {
        consumerGroupDescs = new ArrayList<>();
      }
      consumerGroupDescs.add(
          convertParitionAssignmentStateToGroupDesc(consumerGroup, consumerGroupSummary,
              partitionAssignmentState, type));
      result.put(topic, consumerGroupDescs);
    });

    return result;
  }

  private ConsumerGroupDesc convertParitionAssignmentStateToGroupDesc(String consumerGroup,
      ConsumerGroupSummary consumerGroupSummary, PartitionAssignmentState pas, ConsumerType type) {
    ConsumerGroupDesc.ConsumerGroupDescBuilder consumerGroupDescBuilder = ConsumerGroupDesc
        .builder().groupName(consumerGroup)
        .topic(pas.getTopic())
        .partitionId(pas.getPartition()).currentOffset(pas.getOffset())
        .logEndOffset(pas.getLogEndOffset()).lag(pas.getLag()).consumerId(pas.getConsumerId())
        .consumerId(pas.getConsumerId()).host(pas.getHost()).type(type);

    if (consumerGroupSummary != null) {
      consumerGroupDescBuilder = consumerGroupDescBuilder
          .state(ConsumerGroupState.parse(consumerGroupSummary.state()))
          .assignmentStrategy(consumerGroupSummary.assignmentStrategy()).
              coordinator(consumerGroupSummary.coordinator());
    }

    return consumerGroupDescBuilder.build();
  }

  public List<PartitionAssignmentState> describeNewConsumerGroup(String consumerGroup,
      boolean filtered, String topic) {
    List<PartitionAssignmentState> partitionAssignmentStateList = new ArrayList<>();

    if (filtered && !existTopic(topic)) {
      return partitionAssignmentStateList;
    }

    AdminClient adminClient = kafkaUtils.createAdminClient();
    ConsumerGroupSummary consumerGroupSummary = adminClient.describeConsumerGroup(consumerGroup, 0);
    List<ConsumerSummary> consumerSummaryList = CollectionConvertor
        .listConvertJavaList(consumerGroupSummary.consumers().get());
    if (consumerSummaryList != null) {
      Map<TopicPartition, Object> offsets = CollectionConvertor
          .mapConvertJavaMap(adminClient.listGroupOffsets(consumerGroup));
      Map<TopicPartition, Object> offsetsFiltered;
      if (filtered && existTopic(topic)) {
        offsetsFiltered = offsets.entrySet().stream().filter(
            topicPartitionObjectEntry -> topicPartitionObjectEntry.getKey().topic().equals(topic))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      } else {
        offsetsFiltered = offsets;
      }
      if (offsetsFiltered.isEmpty()) {
        return partitionAssignmentStateList;
      } else {
        ArrayList<TopicPartition> assignedTopicPartitions = new ArrayList<>();
        List<PartitionAssignmentState> rowsWithConsumer = consumerSummaryList.stream()
            .flatMap(consumerSummary -> {
              List<TopicPartition> topicPartitions = CollectionConvertor
                  .listConvertJavaList(consumerSummary.assignment());
              List<TopicPartition> topicPartitionsFiltered = topicPartitions;
              if (filtered) {
                topicPartitionsFiltered = topicPartitions.stream()
                    .filter(topicPartition -> topicPartition.topic().equals(topic))
                    .collect(toList());
              }
              assignedTopicPartitions.addAll(topicPartitionsFiltered);
              Map<TopicPartition, Object> partitionOffsets = topicPartitionsFiltered.stream()
                  .collect(
                      toMap(Function.identity(),
                          topicPartition -> offsetsFiltered.get(topicPartition)));
              return collectConsumerAssignment(consumerGroup, consumerGroupSummary.coordinator(),
                  topicPartitionsFiltered, partitionOffsets, consumerSummary.consumerId(),
                  consumerSummary.host(), consumerSummary.clientId()).stream();
            }).collect(toList());

        List<TopicPartition> topicPartitionList = new ArrayList<>();
        Map<TopicPartition, Object> partitionOffsets = new HashMap<>();

        offsetsFiltered.entrySet().forEach(topicPartitionObjectEntry -> {
          if (!assignedTopicPartitions.contains(topicPartitionObjectEntry.getKey())) {
            topicPartitionList.add(topicPartitionObjectEntry.getKey());
            partitionOffsets
                .put(topicPartitionObjectEntry.getKey(), topicPartitionObjectEntry.getValue());
          }
        });
        List<PartitionAssignmentState> rowsWithoutConsumer = collectConsumerAssignment(
            consumerGroup, consumerGroupSummary.coordinator(),
            topicPartitionList, partitionOffsets, "-", "-", "-");

        partitionAssignmentStateList.addAll(rowsWithConsumer);
        partitionAssignmentStateList.addAll(rowsWithoutConsumer);
      }
    }

    adminClient.close();
    return partitionAssignmentStateList;
  }

  private List<PartitionAssignmentState> collectConsumerAssignment(String group, Node coordinator,
      List<TopicPartition> topicPartitions, Map<TopicPartition, Object> partitionOffsets,
      String consumerId, String host, String clientId) {
    if (topicPartitions.size() == 0) {
      return new ArrayList<PartitionAssignmentState>();
    } else {
      List<PartitionAssignmentState> list = new ArrayList<>();
      topicPartitions.forEach(topicPartition -> {
        long logEndOffset = getEndOffset(topicPartition.topic(), topicPartition.partition());
        long offset = (Long) partitionOffsets.get(topicPartition);
        long lag = logEndOffset - offset;
        list.add(new PartitionAssignmentState(group, coordinator, topicPartition.topic(),
            topicPartition.partition(), offset, lag, consumerId, host, clientId, logEndOffset));
      });
      return list;
    }
  }

  public List<PartitionAssignmentState> describeOldConsumerGroup(String consumerGroup,
      boolean filtered, String topic) {
    List<PartitionAssignmentState> partitionAssignmentStateList = new ArrayList<>();

    if (filtered && !existTopic(topic)) {
      return partitionAssignmentStateList;
    }

    String[] agrs = {"--describe", "--zookeeper", zookeeperUtils.getZookeeperConfig().getUris(),
        "--group", consumerGroup};
    ConsumerGroupCommand.ConsumerGroupCommandOptions options =
        new ConsumerGroupCommand.ConsumerGroupCommandOptions(agrs);
    ConsumerGroupCommand.ZkConsumerGroupService zkConsumerGroupService =
        new ConsumerGroupCommand.ZkConsumerGroupService(options);

    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new DefaultScalaModule());
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    try {
      String source = mapper
          .writeValueAsString(zkConsumerGroupService.collectGroupOffsets()._2().get());
      System.out.println("source:" + source);
      partitionAssignmentStateList = mapper.readValue(source,
          getCollectionType(mapper, List.class, PartitionAssignmentState.class));
      List<PartitionAssignmentState> partitionAssignmentStateListFiltered;
      if (filtered && existTopic(topic)) {
        partitionAssignmentStateListFiltered = partitionAssignmentStateList.stream()
            .filter(partitionAssignmentState -> partitionAssignmentState.getTopic().equals(topic))
            .collect(Collectors.toList());
      } else {
        partitionAssignmentStateListFiltered = partitionAssignmentStateList;
      }
      partitionAssignmentStateListFiltered
          .sort(Comparator.comparing(PartitionAssignmentState::getTopic)
              .thenComparing(PartitionAssignmentState::getPartition));
    } catch (JsonProcessingException jsonProcessingException) {
      log.error("Describe old consumer group exception.", jsonProcessingException);
    } catch (IOException ioexception) {
      log.error("Describe old consumer group exception.", ioexception);
    }

    return partitionAssignmentStateList;
  }

  private JavaType getCollectionType(ObjectMapper mapper,
      Class<?> collectionClass,
      Class<?>... elementClasses) {
    return mapper.getTypeFactory().constructParametricType(collectionClass, elementClasses);
  }

  public List<ConsumerGroupDesc> describeNewCGByTopic(String consumerGroup,
      String topic) {
    if (!isNewConsumerGroup(consumerGroup)) {
      throw new RuntimeException(consumerGroup + " non-exist!");
    }

    List<PartitionAssignmentState> partitionAssignmentStateList = describeNewConsumerGroup(
        consumerGroup, true, topic);

    AdminClient adminClient = kafkaUtils.createAdminClient();
    ConsumerGroupSummary consumerGroupSummary = adminClient
        .describeConsumerGroup(consumerGroup, 0);
    adminClient.close();

    return partitionAssignmentStateList.stream().map(
        partitionAssignmentState -> convertParitionAssignmentStateToGroupDesc(consumerGroup,
            consumerGroupSummary, partitionAssignmentState, ConsumerType.NEW))
        .collect(Collectors.toList());
  }

  public List<ConsumerGroupDesc> describeOldCGByTopic(String consumerGroup,
      @TopicExistConstraint String topic) throws InterruptedException, ExecutionException {
    if (!isOldConsumerGroup(consumerGroup)) {
      throw new RuntimeException(consumerGroup + " non-exist");
    }

    List<PartitionAssignmentState> partitionAssignmentStateList = describeOldConsumerGroup(
        consumerGroup, true, topic);
    ConsumerGroupSummary consumerGroupSummary = null;

    return partitionAssignmentStateList.stream().map(
        partitionAssignmentState -> convertParitionAssignmentStateToGroupDesc(consumerGroup,
            consumerGroupSummary, partitionAssignmentState, ConsumerType.NEW))
        .collect(Collectors.toList());
  }

  public Map<String, GeneralResponse> addPartitions(List<AddPartition> addPartitions) {
    Map<String, GeneralResponse> addPartitionsResult = new HashMap<>();
    org.apache.kafka.clients.admin.AdminClient kafkaAdminClient = createKafkaAdminClient();

    Map<String, NewPartitions> newPartitionsMap = new HashMap<>();
    addPartitions.forEach(addPartition -> {
      String topic = addPartition.getTopic();
      if (!existTopic(topic)) {
        addPartitionsResult.put(topic, GeneralResponse.builder().state(GeneralResponseState.failure)
            .msg("Topic:" + topic + " non-exist.").build());
      } else {
        TopicMeta topicMeta = describeTopic(topic);
        int currentPartionCount = topicMeta.getPartitionCount();
        int numPartitionsAdded = addPartition.getNumPartitionsAdded();
        int totalCount = currentPartionCount + numPartitionsAdded;
        String assignmentStr = addPartition.getReplicaAssignment();
        List<List<Integer>> newAssignments = new ArrayList<>();
        if (assignmentStr != null && !assignmentStr.equals("")) {
          String[] ids = assignmentStr.split(",");
          for (int i = 0; i < ids.length; i++) {
            String[] replicas = ids[i].split(":");
            List<Integer> replicasInt = new ArrayList<>();
            for (int j = 0; j < replicas.length; j++) {
              replicasInt.add(Integer.parseInt(replicas[j]));
            }
            newAssignments.add(replicasInt);
          }
        }
        System.out.println("++++++++newAssignments:" + newAssignments);
        NewPartitions newPartitions;
        if (newAssignments.isEmpty()) {
          //The assignment of new replicas to brokers will be decided by the broker.
          newPartitions = NewPartitions.increaseTo(totalCount);
        } else {
          newPartitions = NewPartitions.increaseTo(totalCount, newAssignments);
        }
        newPartitionsMap.put(topic, newPartitions);
      }
    });
    CreatePartitionsResult createPartitionsResult = kafkaAdminClient
        .createPartitions(newPartitionsMap);
    try {
      createPartitionsResult.all().get(kafkaAdminClientAlterTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception exception) {
      log.warn("Add partitions exception: " + exception);
    } finally {
      Map<String, KafkaFuture<Void>> result = createPartitionsResult.values();
      result.forEach((topic, createResult) -> {
        GeneralResponse generalResponse;
        if (!createResult.isCompletedExceptionally() && createResult.isDone()) {
          TopicMeta topicMeta = describeTopic(topic);
          generalResponse = GeneralResponse.builder().state(GeneralResponseState.success)
              .data(topicMeta).build();
        } else {
          generalResponse = GeneralResponse.builder().state(GeneralResponseState.failure)
              .msg(createResult.toString()).build();
        }
        addPartitionsResult.put(topic, generalResponse);
      });

      kafkaAdminClient.close();
      return addPartitionsResult;
    }
  }

  //Return <Current partition replica assignment, Proposed partition reassignment>
  public List<String> generateReassignPartition(ReassignWrapper reassignWrapper) {
    Seq brokerSeq = JavaConverters.asScalaBufferConverter(reassignWrapper.getBrokers()).asScala()
        .toSeq();
    //<Proposed partition reassignment，Current partition replica assignment>
    Tuple2 resultTuple2 = ReassignPartitionsCommand
        .generateAssignment(kafkaZkClient, brokerSeq, reassignWrapper.generateReassignJsonString(),
            false);
    List<String> result = new ArrayList<>();
    HashMap<TopicPartitionReplica, String> emptyMap = new HashMap<>();
    result.add(ReassignPartitionsCommand
        .formatAsReassignmentJson(
            (scala.collection.Map<TopicPartition, Seq<Object>>) resultTuple2._1(),
            JavaConverters.mapAsScalaMapConverter(emptyMap).asScala()));
    result.add(ReassignPartitionsCommand
        .formatAsReassignmentJson(
            (scala.collection.Map<TopicPartition, Seq<Object>>) resultTuple2._2(),
            JavaConverters.mapAsScalaMapConverter(emptyMap).asScala()));

    return result;
  }

  public Map<TopicAndPartition, Integer> executeReassignPartition(String reassignStr,
      Long interBrokerThrottle, Long replicaAlterLogDirsThrottle, Long timeoutMs) {
    //Set default value
    interBrokerThrottle = (interBrokerThrottle == null) ? -1 : interBrokerThrottle;
    replicaAlterLogDirsThrottle =
        (replicaAlterLogDirsThrottle == null) ? -1 : replicaAlterLogDirsThrottle;
    timeoutMs = (timeoutMs == null) ? 10000 : timeoutMs;

    Tuple2 resultTuple2;
    try {
      //Parse and validate reassignment json string, return (partitionsToBeReassigned, replicaAssignment)
      resultTuple2 = ReassignPartitionsCommand
          .parseAndValidate(kafkaZkClient, reassignStr);
    } catch (Exception exception) {
      throw new ApiException("Bad Request. " + exception.getMessage());
    }
    //Change list buffer to map
    ListBuffer partitionsToBeReassignedList = (scala.collection.mutable.ListBuffer) resultTuple2
        ._1();
    scala.collection.mutable.HashMap<TopicPartition, Seq<Object>> partitionsToBeReassignedMap = new scala.collection.mutable.HashMap<>();
    for (int i = 0; i < partitionsToBeReassignedList.size(); ++i) {
      Tuple2 tup = (Tuple2) partitionsToBeReassignedList.apply(i);
      partitionsToBeReassignedMap.put((TopicPartition) tup._1(), (Seq<Object>) tup._2());
    }

    org.apache.kafka.clients.admin.AdminClient kafkaAdminClient = createKafkaAdminClient();
    AdminZkClient adminZkClient = new AdminZkClient(kafkaZkClient);

    ReassignPartitionsCommand reassignPartitionsCommand = new ReassignPartitionsCommand(
        kafkaZkClient, scala.Option.apply(null),
        partitionsToBeReassignedMap,
        (scala.collection.Map<TopicPartitionReplica, String>) resultTuple2._2(), adminZkClient);
    Function0<BoxedUnit> postUpdateAction = new AbstractFunction0<BoxedUnit>() {
      @Override
      public BoxedUnit apply() {
        return null;
      }
    };
    Throttle throttle = new Throttle(interBrokerThrottle, replicaAlterLogDirsThrottle,
        postUpdateAction);

    if (kafkaZkClient.reassignPartitionsInProgress()) {
      reassignPartitionsCommand.maybeLimit(throttle);
      throw new ApiException(
          "Failed to reassign partitions because there is an existing assignment running.");
    } else {
      if (reassignPartitionsCommand.reassignPartitions(throttle, timeoutMs)) {
        System.out.println("Successfully started reassignment of partitions.");
      } else {
        throw new ApiException("Failed to reassign partitions " + resultTuple2._1());
      }
    }
//      return checkReassignStatus(reassignStr);

    kafkaAdminClient.close();
    return new HashMap<>();
  }

  private void checkReassignStatus(
      scala.collection.Map<TopicPartition, Seq<Object>> partitionsToBeReassigned,
      scala.collection.Map<TopicPartition, String> replicaAssignement) {
    Map<TopicPartition, ReassignmentStatus> reassignedPartitionsStatus = checkIfPartitionReassignmentSucceeded(
        partitionsToBeReassigned);
    Map<TopicPartitionReplica, ReassignmentStatus> replicasReassignmentStatus = checkIfReplicaReassignmentSucceeded(
        replicaAssignement);
  }

  private Map<TopicPartition, ReassignmentStatus> checkIfPartitionReassignmentSucceeded(
      scala.collection.Map<TopicPartition, Seq<Object>> partitionsToBeReassigned) {
    Map<TopicPartition, ReassignmentStatus> reassignedPartitionsStatus = new HashMap<>();

    return reassignedPartitionsStatus;
  }

  private Map<TopicPartitionReplica, ReassignmentStatus> checkIfReplicaReassignmentSucceeded(
      scala.collection.Map<TopicPartition, String> replicaAssignement) {
    Map<TopicPartitionReplica, ReassignmentStatus> replicasReassignmentStatus = new HashMap<>();

    return replicasReassignmentStatus;
  }

  /*
      public Map<TopicAndPartition, Integer> checkReassignStatus(String reassignStr) {
          Map<TopicAndPartition, Seq<Object>> partitionsToBeReassigned = JavaConverters.mapAsJavaMapConverter(
                  zkUtils.parsePartitionReassignmentData(reassignStr)).asJava();

          Map<TopicAndPartition, Seq<Object>> partitionsBeingReassigned = JavaConverters.mapAsJavaMapConverter(
                  zkUtils.getPartitionsBeingReassigned()).asJava().entrySet().stream().collect(
                  Collectors.toMap(
                          Map.Entry::getKey,
                          data -> data.getValue().newReplicas()
                  ));


          java.util.Map<TopicAndPartition, ReassignmentStatus> reassignedPartitionsStatus =
                  partitionsToBeReassigned.entrySet().stream().collect(Collectors.toMap(
                          Map.Entry::getKey,
                          pbr -> ReassignPartitionsCommand.checkIfPartitionReassignmentSucceeded(
                                  zkUtils,
                                  pbr.getKey(),
                                  pbr.getValue(),
                                  JavaConverters.mapAsScalaMapConverter(partitionsToBeReassigned).asScala(),
                                  JavaConverters.mapAsScalaMapConverter(partitionsBeingReassigned).asScala()
                          )
                  ));


          return reassignedPartitionsStatus.entrySet().stream().collect(Collectors.toMap(
                  Map.Entry::getKey,
                  r -> r.getValue().status()
          ));
      }
      */
  public String getMessage(@TopicExistConstraint String topic, int partition, long offset,
      String decoder, String avroSchema) {
    KafkaConsumer consumer = kafkaUtils
        .createNewConsumer(String.valueOf(System.currentTimeMillis()));
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
          "offsets must be between " + String.valueOf(beginningOffset
              + " and " + (endOffset - 1)
          )
      );
    }
    consumer.assign(Collections.singletonList(tp));
    consumer.seek(tp, offset);

    String last = null;

    //ConsumerRecords<String, String> crs = consumer.poll(channelRetryBackoffMs);
    ConsumerRecords<String, String> crs = consumer.poll(3000);
    log.info("Seek to offset:" + offset + ", topic:" + topic + ", partition:" + partition
        + ", crs.count:" + crs.count());
    if (crs.count() != 0) {
      Iterator<ConsumerRecord<String, String>> it = crs.iterator();
      while (it.hasNext()) {
        ConsumerRecord<String, String> initCr = it.next();
        last = "Value: " + initCr.value() + ", Offset: " + String.valueOf(initCr.offset())
            + ", timestamp:" + initCr.timestamp();
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

  public Record getRecordByOffset(@TopicExistConstraint String topic, int partition,
      long offset,
      String decoder, String avroSchema) {
    if (!isTopicPartitionValid(topic, partition)) {
      throw new ApiException("Bad request. Topic:" + topic + " has no partition:" + partition);
    }
    KafkaConsumer consumer;
    try {
      consumer = kafkaUtils.createNewConsumer(String.valueOf(System.currentTimeMillis()), decoder);
    } catch (ClassNotFoundException classNotFoundException) {
      throw new ApiException("Bad request. Decoder class:" + decoder
          + " not found. ByteArrayDeserializer, ByteBufferDeserializer, BytesDeserializer, "
          + "DoubleDeserializer, ExtendedDeserializer.Wrapper, FloatDeserializer, "
          + "IntegerDeserializer, LongDeserializer, ShortDeserializer, StringDeserializer "
          + "are supported.");
    }
    TopicPartition tp = new TopicPartition(topic, partition);
    long beginningOffset = getBeginningOffset(topic, partition);
    long endOffset = getEndOffset(topic, partition);
    System.out.println("++++++beginning offset:" + beginningOffset + ", endoffset:" + endOffset);
    if (beginningOffset == endOffset) {
      throw new ApiException("There is no message in this partition of this topic");
    }
    if (offset < beginningOffset || offset >= endOffset) {
      log.error(offset + " error");
      consumer.close();
      throw new ApiException(
          "offsets must be between " + String.valueOf(beginningOffset
              + " and " + (endOffset - 1)
          )
      );
    }
    consumer.assign(Collections.singletonList(tp));
    consumer.seek(tp, offset);

    Record record = new Record();

    //ConsumerRecords<String, String> crs = consumer.poll(channelRetryBackoffMs);
    ConsumerRecords<String, String> crs = consumer.poll(3000);
    log.info("Seek to offset:" + offset + ", topic:" + topic + ", partition:" + partition
        + ", crs.count:" + crs.count());
    System.out.println("Seek to offset:" + offset + ", topic:" + topic + ", partition:" + partition
        + ", crs.count:" + crs.count());
    if (crs.count() != 0) {
      Iterator<ConsumerRecord<String, String>> it = crs.iterator();
      while (it.hasNext()) {
        ConsumerRecord<String, String> initCr = it.next();
        if (initCr.offset() == offset) {
          record.setOffset(offset);
          record.setTimestamp(initCr.timestamp());
          record.setKey(initCr.key());
          record.setValue(initCr.value());
          break;
        }
        log.info(
            "Value: " + initCr.value() + ", initCr.Offset: " + String.valueOf(initCr.offset())
                + ", timestamp:" + initCr.timestamp());
        System.out.println(
            "Value: " + initCr.value() + ", initCr.Offset: " + String.valueOf(initCr.offset())
                + ", timestamp:" + initCr.timestamp());
      }
    }
    consumer.close();
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

  public GeneralResponse resetOffset(@TopicExistConstraint String topic, int partition,
      String consumerGroup,
      ConsumerType type, String offset) {
    KafkaConsumer consumer = null;
    log.info("To tell the consumergroup " + consumerGroup + " is new");
    if (type != null && type == ConsumerType.NEW) {
      if (!isNewConsumerGroup(consumerGroup)) {
        throw new ApiException("Consumer group " + consumerGroup + " is non-exist!");
      }
    }

    log.info("To tell the consumergroup " + consumerGroup + " is old");
    if (type != null && type == ConsumerType.OLD) {
      if (!isOldConsumerGroup(consumerGroup)) {
        throw new ApiException("Consumer group " + consumerGroup + " is non-exist!");
      }
    }

    long offsetToBeReset = -1;
    long beginningOffset = getBeginningOffset(topic, partition);
    long endOffset = getEndOffset(topic, partition);

    log.info("To tell the consumergroup " + consumerGroup + " is active now");
    if (isConsumerGroupActive(consumerGroup, type)) {
      throw new ApiException(
          "Offsets can only be reset if the group " + consumerGroup + " is inactive");
    }

    if (type != null && type == ConsumerType.NEW && isNewConsumerGroup(consumerGroup)) {
      try {
        log.info("The consumergroup " + consumerGroup + " is new. Reset offset now");
        consumer = kafkaUtils.createNewConsumer(consumerGroup);
        //if type is new or the consumergroup itself is new
        TopicPartition tp = new TopicPartition(topic, partition);
        consumer.assign(Arrays.asList(tp));
        consumer.poll(channelSocketTimeoutMs);
        if (offset.equals("earliest")) {
          consumer.seekToBeginning(Arrays.asList(tp));
          offsetToBeReset = beginningOffset;
          log.info("Reset to" + consumer.position(tp));
        } else if (offset.equals("latest")) {
          consumer.seekToEnd(Arrays.asList(tp));
          offsetToBeReset = endOffset;
          log.info("Reset to" + consumer.position(tp));
        } else if (isDateTime(offset)) {
          //Reset offset by time
          SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
          Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
          try {
            timestampsToSearch.put(tp, sdf.parse(offset).getTime());
            Map<TopicPartition, OffsetAndTimestamp> results = consumer
                .offsetsForTimes(timestampsToSearch);
            OffsetAndTimestamp offsetAndTimestamp = results.get(tp);
            if (offsetAndTimestamp != null) {
              offsetToBeReset = offsetAndTimestamp.offset();
              log.info("Reset offset by time" + offsetAndTimestamp.offset() + ", timestamp:"
                  + offsetAndTimestamp.timestamp() + ", timestampToDate:" + sdf
                  .format(new Date(offsetAndTimestamp.timestamp())));
              consumer.seek(tp, offsetToBeReset);
              consumer.commitSync();
            } else {
              throw new ApiException(
                  "No offset whose timestamp is greater than or equal to the given timestamp:"
                      + offset);
            }
          } catch (ParseException parseException) {
            throw new ApiException(
                "Invalid offset format. Date format should be yyyy-MM-dd HH:mm:ss.");
          }
        } else {
          if (Long.parseLong(offset) < beginningOffset || Long.parseLong(offset) > endOffset) {
            log.error(offset + " error");
            throw new ApiException(
                "Invalid request offset:" + offset + ". Topic " + topic + "'s beginning offset:"
                    + beginningOffset + ", endoffset:" + endOffset);
          }
          offsetToBeReset = Long.parseLong(offset);
          consumer.seek(tp, offsetToBeReset);
          consumer.commitSync();
        }
      } catch (IllegalStateException e) {
        storage.getMap().remove(consumerGroup);
        throw new ApiException(e);
      } finally {
        consumer.close();
      }
    }

    //if type is old or the consumer group itself is old
    if (type != null && type == ConsumerType.OLD && isOldConsumerGroup(consumerGroup)) {
      log.info("The consumergroup " + consumerGroup + " is old. Reset offset now");
      if (offset.equals("earliest")) {
        offset = String.valueOf(beginningOffset);
        offsetToBeReset = beginningOffset;
      } else if (offset.equals("latest")) {
        offset = String.valueOf(endOffset);
        offsetToBeReset = endOffset;
      } else {
        try {
          if (Long.parseLong(offset) < beginningOffset || Long.parseLong(offset) > endOffset) {
            log.info("Setting offset to " + offset + " error");
            throw new ApiException(
                "Invalid request offset:" + offset + ". Topic " + topic + "'s beginning offset:"
                    + beginningOffset + ", endoffset:" + endOffset);
          }
          log.info("Offset will be reset to " + offset);
          offsetToBeReset = Long.parseLong(offset);
          zkUtils.zkClient().writeData(
              "/consumers/" + consumerGroup + "/offsets/" + topic + "/" + partition,
              offset);
        } catch (Exception e) {
          throw new ApiException(e);
        }
      }
    }
    return GeneralResponse.builder().state(GeneralResponseState.success)
        .msg("Reset the offset successfully!").data(Long.toString(offsetToBeReset)).build();
  }

  private boolean isDateTime(String offset) {
    String patternStr = "\\d\\d\\d\\d-[0-1]\\d-[0-3]\\d\\s+[0-2]\\d:[0-5]\\d:[0-5]\\d";
    Pattern timePattern = Pattern.compile(patternStr);
    return timePattern.matcher(offset).find();
  }

  public Map<String, Map<Integer, Long>> getLastCommitTime(
      @ConsumerGroupExistConstraint String consumerGroup,
      @TopicExistConstraint String topic,
      ConsumerType type) {
    Map<String, Map<Integer, Long>> result = new ConcurrentHashMap<>();

    if (type != null && type == ConsumerType.OLD) {
      //Get Old Consumer commit time
      try {
        Map<Integer, Long> oldConsumerOffsetMap = new ConcurrentHashMap<>();
        if (zkClient.checkExists().forPath(CONSUMERPATHPREFIX + consumerGroup) != null
            && zkClient.checkExists()
            .forPath(CONSUMERPATHPREFIX + consumerGroup + OFFSETSPATHPREFIX + topic) != null) {
          List<String> offsets = zkClient.getChildren()
              .forPath(CONSUMERPATHPREFIX + consumerGroup + OFFSETSPATHPREFIX + topic);
          for (String offset : offsets) {
            Integer id = Integer.valueOf(offset);
            long mtime = zkClient.checkExists().forPath(
                CONSUMERPATHPREFIX + consumerGroup + OFFSETSPATHPREFIX + topic + "/" + offset)
                .getMtime();
            oldConsumerOffsetMap.put(id, mtime);
          }

          result.put("old", oldConsumerOffsetMap);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }

    } else {
//      Get New consumer commit time, from offset storage instance

      if (storage.get(consumerGroup) != null) {
        Map<GroupTopicPartition, kafka.common.OffsetAndMetadata> storageResult = storage
            .get(consumerGroup);
        result.put("new", (storageResult.entrySet().parallelStream()
                .filter(s -> s.getKey().topicPartition().topic().equals(topic))
                .collect(
                    Collectors.toMap(
                        s -> s.getKey().topicPartition().partition(),
                        s -> {
                          if (s.getValue() != null) {
                            return s.getValue().commitTimestamp();
                          } else {
                            return -1l;
                          }
                        }
                    )
                )
            )
        );
      }

    }

    return result;
  }

  public GeneralResponse deleteConsumerGroup(String consumerGroup, ConsumerType type) {
    if (type == ConsumerType.OLD && !isOldConsumerGroup(consumerGroup)) {
      throw new RuntimeException("Old consumer group:" + consumerGroup + " non-exist");
    }
    if (type == ConsumerType.NEW && !isNewConsumerGroup(consumerGroup)) {
      throw new RuntimeException("New consumer group:" + consumerGroup + " non-exist");
    }
    if (type == ConsumerType.OLD) {
      if (!AdminUtils.deleteConsumerGroupInZK(zkUtils, consumerGroup)) {
        throw new ApiException(
            "The consumer " + consumerGroup + " is still active.Please stop it first");
      }
    } else if (type == ConsumerType.NEW) {
      AdminClient adminClient = kafkaUtils.createAdminClient();
      List<String> groups = new ArrayList<>();
      groups.add(consumerGroup);

      scala.collection.immutable.List<String> sList = JavaConverters.asScalaBufferConverter(groups)
          .asScala().toList();
      scala.collection.immutable.Map<String, Errors> stringErrorsMap = adminClient
          .deleteConsumerGroups((scala.collection.immutable.List) (sList));

      if (!stringErrorsMap.get(consumerGroup).get().equals(Errors.NONE)) {
        throw new ApiException(
            "Consumer group:" + consumerGroup + " could not be deleted. Error Code:"
                + stringErrorsMap.get(consumerGroup).get() + ". Error msg:" + stringErrorsMap
                .get(consumerGroup).get().exception());
      }
      adminClient.close();
    }

    return GeneralResponse.builder().state(GeneralResponseState.success)
        .msg("Consumer group:" + consumerGroup + " has been deleted.").build();
  }

  private List<TopicAndPartition> getTopicPartitions(String t) {
    List<TopicAndPartition> tpList = new ArrayList<>();
    List<String> l = Arrays.asList(t);
    java.util.Map<String, Seq<Object>> tpMap = JavaConverters.mapAsJavaMapConverter(zkUtils
        .getPartitionsForTopics(
            JavaConverters.asScalaIteratorConverter(l.iterator()).asScala().toSeq())).asJava();
    if (tpMap != null) {
      ArrayList<Object> partitionLists = new ArrayList<>(
          JavaConverters.seqAsJavaListConverter(tpMap.get(t)).asJava());
      tpList = partitionLists.stream().map(p -> new TopicAndPartition(t, (Integer) p))
          .collect(toList());
    }
    return tpList;
  }

  private long getOffsets(Node leader, String topic, int partitionId, long time) {
    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partitionId);

    SimpleConsumer consumer = new SimpleConsumer(
        leader.host(),
        leader.port(),
        10000,
        1024,
        "Kafka-zk-simpleconsumer"
    );

    PartitionOffsetRequestInfo partitionOffsetRequestInfo = new PartitionOffsetRequestInfo(time,
        10000);
    OffsetRequest offsetRequest = new OffsetRequest(
        ImmutableMap.of(topicAndPartition, partitionOffsetRequestInfo),
        kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());
    OffsetResponse offsetResponse = consumer.getOffsetsBefore(offsetRequest);

    if (offsetResponse.hasError()) {
      short errorCode = offsetResponse.errorCode(topic, partitionId);
      log.warn(format("Offset response has error: %d", errorCode));
      throw new ApiException(
          "could not fetch data from Kafka, error code is '" + errorCode + "'Exception Message:"
              + offsetResponse.toString());
    }

    long[] offsets = offsetResponse.offsets(topic, partitionId);
    consumer.close();
    return offsets[0];
  }

  private long getOffsets(PartitionInfo partitionInfo, long time) {
    return getOffsets(partitionInfo.leader(), partitionInfo.topic(), partitionInfo.partition(),
        time);
  }

  public long getBeginningOffset(String topic, int partitionId) {
    KafkaConsumer consumer = kafkaUtils.createNewConsumer(KafkaUtils.DEFAULTCP);
    TopicPartition tp = new TopicPartition(topic, partitionId);
    consumer.assign(Arrays.asList(tp));
    Map<TopicPartition, Long> beginningOffsets = consumer
        .beginningOffsets(Collections.singletonList(tp));
    consumer.close();

    if (beginningOffsets != null) {
      return beginningOffsets.get(tp);
    }

    return -1;
  }

  public long getEndOffset(String topic, int partitionId) {
    KafkaConsumer consumer = kafkaUtils.createNewConsumer(KafkaUtils.DEFAULTCP);
    TopicPartition tp = new TopicPartition(topic, partitionId);
    consumer.assign(Arrays.asList(tp));
    Map<TopicPartition, Long> endOffsets = consumer.endOffsets(Collections.singletonList(tp));
    consumer.close();

    if (endOffsets != null) {
      return endOffsets.get(tp);
    }

    return -1;
  }

  private long getBeginningOffset(Node leader, String topic, int partitionId) {
    return getOffsets(leader, topic, partitionId, kafka.api.OffsetRequest.EarliestTime());
  }

  private long getEndOffset(Node leader, String topic, int partitionId) {
    return getOffsets(leader, topic, partitionId, kafka.api.OffsetRequest.LatestTime());
  }


  public Map<Integer, Long> countPartition(String topic) {
    KafkaConsumer consumer = kafkaUtils.createNewConsumer();
    List<PartitionInfo> piList = consumer.partitionsFor(topic);
    Map<Integer, Long> result = piList.stream().flatMap(pi -> Arrays.stream(pi.replicas()))
        .map(node -> node.id()).collect(Collectors.groupingBy(
            Function.identity(), Collectors.counting()
        ));

    consumer.close();

    return result;
  }

  private boolean isConsumerGroupActive(String consumerGroup, ConsumerType type) {
    if (type == ConsumerType.NEW) {
      ConsumerGroupMeta groupMeta = getConsumerGroupMeta(consumerGroup);
      ConsumerGroupState groupState = groupMeta.getState();
      if (groupState.equals(ConsumerGroupState.STABLE) || groupState
          .equals(ConsumerGroupState.PREPARING_REBALANCE) || groupState
          .equals(ConsumerGroupState.COMPLETING_REBALANCE)) {
        return true;
      }
      if (groupState.equals(ConsumerGroupState.EMPTY) || groupState
          .equals(ConsumerGroupState.DEAD)) {
        return false;
      }
      throw new ApiException(
          "Consumer group:" + consumerGroup + " state:" + groupState + " unkown.");
    } else if (type == ConsumerType.OLD) {
      return zkUtils.getConsumersInGroup(consumerGroup).nonEmpty();
    } else {
      throw new ApiException("Unknown type " + type);
    }
  }

  public HealthCheckResult healthCheck() {
    String healthCheckTopic = kafkaConfig.getHealthCheckTopic();
    HealthCheckResult healthCheckResult = new HealthCheckResult();
    KafkaProducer producer = kafkaUtils.createProducer();
    KafkaConsumer consumer = kafkaUtils.createNewConsumerByTopic(healthCheckTopic);

    boolean healthCheckTopicExist = existTopic(healthCheckTopic);
    log.info("HealthCheckTopic:" + healthCheckTopic + " existed:" + healthCheckTopicExist);
    if (!healthCheckTopicExist) {
      healthCheckResult.setStatus("unknown");
      healthCheckResult.setMsg("HealthCheckTopic: " + healthCheckTopic
          + " Non-Exist. Please create it before doing health check.");
      return healthCheckResult;
    }

    String message = "healthcheck_" + System.currentTimeMillis();
    ProducerRecord<String, String> record = new ProducerRecord(healthCheckTopic, null, message);
    log.info("Generate message:" + message);
    try {
      RecordMetadata recordMetadata = (RecordMetadata) producer.send(record).get();
      log.info("Message:" + message + " has been sent to Partition:" + recordMetadata.partition());
    } catch (Exception e) {
      healthCheckResult.setStatus("error");
      healthCheckResult
          .setMsg("Health Check: Produce Message Failure. Exception: " + e.getMessage());
      log.error("Health Check: Produce Message Failure.", e);
      return healthCheckResult;
    } finally {
      producer.close();
    }

    int retries = 30;
    int noRecordsCount = 0;
    while (true) {
      final ConsumerRecords<Long, String> consumerRecords =
          consumer.poll(1000);
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
      healthCheckResult
          .setMsg("Health Check: Consume Message Failure. Consumer can't fetch the message.");
    }
    return healthCheckResult;
  }

  public void printObjectInJson(Object object) {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      //objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
      System.out.println(
          "object:" + objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(object));
    } catch (JsonProcessingException jsonProcessingException) {
      jsonProcessingException.printStackTrace();
    }
  }
}
