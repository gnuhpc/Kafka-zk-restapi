package org.gnuhpc.bigdata.service;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import kafka.admin.*;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.OffsetAndMetadata;
import kafka.common.Topic;
import kafka.common.TopicAndPartition;
import kafka.coordinator.GroupOverview;
import kafka.coordinator.GroupTopicPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.server.ConfigType;
import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZkUtils;
import lombok.extern.log4j.Log4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.gnuhpc.bigdata.CollectionConvertor;
import org.gnuhpc.bigdata.componet.OffsetStorage;
import org.gnuhpc.bigdata.constant.ConsumerState;
import org.gnuhpc.bigdata.constant.ConsumerType;
import org.gnuhpc.bigdata.constant.GeneralResponseState;
import org.gnuhpc.bigdata.model.*;
import org.gnuhpc.bigdata.task.FetchOffSetFromZKResult;
import org.gnuhpc.bigdata.task.FetchOffsetFromZKTask;
import org.gnuhpc.bigdata.utils.KafkaUtils;
import org.gnuhpc.bigdata.utils.ZookeeperUtils;
import org.gnuhpc.bigdata.validator.ConsumerGroupExistConstraint;
import org.gnuhpc.bigdata.validator.TopicExistConstraint;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

/**
 * Created by gnuhpc on 2017/7/17.
 */

@Service
@Log4j
@Validated
public class KafkaAdminService {

    private static final int channelSocketTimeoutMs = 600;
    private static final int channelRetryBackoffMs = 600;
    private static final String DEFAULTCP = "kafka-rest-consumergroup";
    private static final String CONSUMERPATHPREFIX = "/consumers/";
    private static final String OFFSETSPATHPREFIX = "/offsets/";
    @Autowired
    private ZookeeperUtils zookeeperUtils;

    @Autowired
    private KafkaUtils kafkaUtils;

    @Autowired
    private OffsetStorage storage;

    private AdminClient kafkaAdminClient;

    //For AdminUtils use
    private ZkUtils zkUtils;

    //For zookeeper connection
    private CuratorFramework zkClient;

    //For Json serialized
    private Gson gson;

    private Option<String> NONE = Option.apply(null);

    @PostConstruct
    private void init() {
        this.zkUtils = zookeeperUtils.getZkUtils();
        this.zkClient = zookeeperUtils.getCuratorClient();
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(DateTime.class, (JsonDeserializer<DateTime>) (jsonElement, type, jsonDeserializationContext) -> new DateTime(jsonElement.getAsJsonPrimitive().getAsLong()));

        this.gson = builder.create();
        this.kafkaAdminClient = kafkaUtils.getKafkaAdminClient();
    }

    public TopicMeta createTopic(TopicDetail topic, String reassignStr) {
        if (StringUtils.isEmpty(topic.getName())) {
            throw new InvalidTopicException("Empty topic name");
        }

        if (Topic.hasCollisionChars(topic.getName())) {
            throw new InvalidTopicException("Invalid topic name");
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

            TopicCommand.createTopic(zkUtils, new TopicCommand.TopicCommandOptions(argsList.stream().toArray(String[]::new)));
        }

        return describeTopic(topic.getName());
    }

    public List<String> listTopics() {
        return CollectionConvertor.seqConvertJavaList(zkUtils.getAllTopics());
    }

    public List<TopicBrief> listTopicBrief() {
        KafkaConsumer consumer = createNewConsumer(DEFAULTCP);
        Map<String, List<PartitionInfo>> topicMap = consumer.listTopics();
        List<TopicBrief> result = topicMap.entrySet().parallelStream().map(e -> {
                    String topic = e.getKey();
                    long isrCount = e.getValue().parallelStream().flatMap(pi -> Arrays.stream(pi.replicas())).count();
                    long replicateCount = e.getValue().parallelStream().flatMap(pi -> Arrays.stream(pi.inSyncReplicas())).count();
                    if (replicateCount == 0) {
                        return new TopicBrief(topic, e.getValue().size(), 0);
                    } else {
                        return new TopicBrief(topic, e.getValue().size(), isrCount / replicateCount);
                    }
                }
        ).collect(toList());

        consumer.close();

        return result;
    }

    public boolean existTopic(String topicName) {
        return AdminUtils.topicExists(zkUtils, topicName);
    }

    public List<BrokerInfo> listBrokers() {
        List<Broker> brokerList = CollectionConvertor.seqConvertJavaList(zkUtils.getAllBrokersInCluster());
        return brokerList.parallelStream().collect(Collectors.toMap(Broker::id, Broker::rack)).entrySet().parallelStream()
                .map(entry -> {
                    String brokerInfoStr = null;
                    try {
                        brokerInfoStr = new String(
                                zkClient.getData().forPath(ZkUtils.BrokerIdsPath() + "/" + entry.getKey())
                        );
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    BrokerInfo brokerInfo = gson.fromJson(brokerInfoStr, BrokerInfo.class);
                    if (entry.getValue().isEmpty())
                        brokerInfo.setRack("");
                    else {
                        brokerInfo.setRack(entry.getValue().get());
                    }
                    brokerInfo.setId(entry.getKey());
                    return brokerInfo;
                }).collect(toList());
    }

    public TopicMeta describeTopic(@TopicExistConstraint String topicName) {
        KafkaConsumer consumer = createNewConsumer(DEFAULTCP);
        TopicMeta topicMeta = new TopicMeta(topicName);
        List<PartitionInfo> tmList = consumer.partitionsFor(topicName);
        topicMeta.setPartitionCount(tmList.size());
        topicMeta.setReplicationFactor(tmList.get(0).replicas().length);
        topicMeta.setTopicCustomConfigs(getTopicPropsFromZk(topicName));
        topicMeta.setTopicPartitionInfos(tmList.parallelStream().map(
                tm -> {
                    TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo();
                    topicPartitionInfo.setLeader(tm.leader().host());
                    topicPartitionInfo.setIsr(Arrays.stream(tm.inSyncReplicas()).map(node -> node.host()).collect(toList()));
                    topicPartitionInfo.setPartitionId(tm.partition());
                    topicPartitionInfo.setReplicas(Arrays.stream(tm.replicas()).map(node -> node.host()).collect(toList()));
                    topicPartitionInfo.setIn_sync();
                    topicPartitionInfo.setStartOffset(getBeginningOffset(tm.leader(), tm.topic(), tm.partition()));
                    topicPartitionInfo.setEndOffset(getEndOffset(tm.leader(), tm.topic(), tm.partition()));
                    topicPartitionInfo.setMessageAvailable();
                    return topicPartitionInfo;

                }).collect(toList())
        );

        Collections.sort(topicMeta.getTopicPartitionInfos());

        consumer.close();

        return topicMeta;
    }

    public GeneralResponse deleteTopic(@TopicExistConstraint String topic) {
        log.warn("Delete topic " + topic);
        AdminUtils.deleteTopic(zkUtils, topic);

        return new GeneralResponse(GeneralResponseState.success, topic + " has been deleted.");
    }

    public Properties createTopicConf(@TopicExistConstraint String topic, Properties prop) {
        Properties configs = getTopicPropsFromZk(topic);
        configs.putAll(prop);
        AdminUtils.changeTopicConfig(zkUtils, topic, configs);
        log.info("Create config for topic: " + topic + "Configs:" + configs);
        return getTopicPropsFromZk(topic);
    }

    public Properties deleteTopicConf(@TopicExistConstraint String topic, List<String> deleteProps) {
        // compile the final set of configs
        Properties configs = getTopicPropsFromZk(topic);
        deleteProps.stream().forEach(config -> configs.remove(config));
        AdminUtils.changeTopicConfig(zkUtils, topic, configs);
        log.info("Delete config for topic: " + topic);
        return getTopicPropsFromZk(topic);
    }

    public Properties updateTopicConf(@TopicExistConstraint String topic, Properties prop) {
        AdminUtils.changeTopicConfig(zkUtils, topic, prop);
        return getTopicPropsFromZk(topic);
    }

    public Properties getTopicConf(@TopicExistConstraint String topic) {
        return getTopicPropsFromZk(topic);
    }

    public Properties getTopicConfByKey(@TopicExistConstraint String topic, String key) {
        String value = String.valueOf(AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topic).get(key));
        Properties returnProps = new Properties();
        if (!value.equals("null")) {
            returnProps.setProperty(key, value);
            return returnProps;
        } else
            return null;
    }

    public boolean deleteTopicConfByKey(@TopicExistConstraint String topic, String key) {
        Properties configs = getTopicPropsFromZk(topic);
        configs.remove(key);
        AdminUtils.changeTopicConfig(zkUtils, topic, configs);
        return getTopicPropsFromZk(topic).get(key) == null;
    }

    public Properties updateTopicConfByKey(@TopicExistConstraint String topic, String key, String value) {
        Properties props = getTopicConf(topic);
        props.setProperty(key, value);
        String validValue = String.valueOf(updateTopicConf(topic, props).get(key));
        if (!validValue.equals("null") && validValue.equals(value)) {
            return props;
        } else {
            throw new ApiException("Update Topic Config failed: " + key + ":" + value);
        }
    }

    public Properties createTopicConfByKey(@TopicExistConstraint String topic, String key, String value) {
        Properties props = new Properties();
        props.setProperty(key, value);
        String validValue = String.valueOf(createTopicConf(topic, props).get(key));
        if (!validValue.equals("null") && validValue.equals(value)) {
            return props;
        } else {
            throw new ApiException("Update Topic Config failed: " + key + ":" + value);
        }

    }

    public TopicMeta addPartition(@TopicExistConstraint String topic, AddPartition addPartition) {
        List<MetadataResponse.PartitionMetadata> partitionMataData = AdminUtils.fetchTopicMetadataFromZk(topic, zkUtils).partitionMetadata();
        int numPartitions = partitionMataData.size();
        int numReplica = partitionMataData.get(0).replicas().size();
        List partitionIdList = partitionMataData.stream().map(p -> String.valueOf(p.partition())).collect(toList());
        String assignmentStr = addPartition.getReplicaAssignment();
        String toBeSetReplicaAssignmentStr = "";

        if (assignmentStr != null && !assignmentStr.equals("")) {
            //Check out of index ids in replica assignment string
            String[] ids = addPartition.getReplicaAssignment().split(",|:");
            if (Arrays.stream(ids).filter(id -> !partitionIdList.contains(id)).count() != 0) {
                throw new InvalidTopicException("Topic " + topic + ": manual reassignment str has wrong id!");
            }

            //Check if any ids duplicated in one partition in replica assignment
            String[] assignPartitions = addPartition.getReplicaAssignment().split(",");
            if (Arrays.stream(assignPartitions).filter(p ->
                    Arrays.stream(p.split(":")).collect(Collectors.toSet()).size()
                            != p.split(":").length).count()
                    != 0) {
                throw new InvalidTopicException("Topic " + topic + ": manual reassignment str has duplicated id in one partition!");
            }

            String replicaStr = Strings.repeat("0:", numReplica).replaceFirst(".$", ",");
            toBeSetReplicaAssignmentStr = Strings.repeat(replicaStr, numPartitions) + addPartition.getReplicaAssignment();
        } else {
            toBeSetReplicaAssignmentStr = "";
        }

        AdminUtils.addPartitions(zkUtils, topic, addPartition.getNumPartitionsAdded() + numPartitions,
                toBeSetReplicaAssignmentStr, true,
                RackAwareMode.Enforced$.MODULE$);

        return describeTopic(topic);
    }

    //Return <Current partition replica assignment, Proposed partition reassignment>
    public List<String> generateReassignPartition(ReassignWrapper reassignWrapper) {
        Seq brokerSeq = JavaConverters.asScalaBufferConverter(reassignWrapper.getBrokers()).asScala().toSeq();
        //<Proposed partition reassignment，Current partition replica assignment>
        Tuple2 resultTuple2 = ReassignPartitionsCommand.generateAssignment(zkUtils, brokerSeq, reassignWrapper.generateReassignJsonString(), false);
        List<String> result = new ArrayList<>();
        result.add(zkUtils.formatAsReassignmentJson((scala.collection.Map<TopicAndPartition, Seq<Object>>) resultTuple2._2()));
        result.add(zkUtils.formatAsReassignmentJson((scala.collection.Map<TopicAndPartition, Seq<Object>>) resultTuple2._1()));

        return result;
    }

    public Map<TopicAndPartition, Integer> executeReassignPartition(String reassignStr) {
        ReassignPartitionsCommand.executeAssignment(
                zkUtils,
                reassignStr
        );
        return checkReassignStatus(reassignStr);
    }

    public Map<TopicAndPartition, Integer> checkReassignStatus(String reassignStr) {
        Map<TopicAndPartition, Seq<Object>> partitionsToBeReassigned = JavaConverters.mapAsJavaMapConverter(
                zkUtils.parsePartitionReassignmentData(reassignStr)).asJava();

        Map<TopicAndPartition, Seq<Object>> partitionsBeingReassigned = JavaConverters.mapAsJavaMapConverter(
                zkUtils.getPartitionsBeingReassigned()).asJava().entrySet().stream().collect(
                Collectors.toMap(
                        Map.Entry::getKey,
                        data -> data.getValue().newReplicas()
                ));


        Map<TopicAndPartition, ReassignmentStatus> reassignedPartitionsStatus =
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

    private List<String> listAllOldConsumerGroups() {
        log.info("Finish getting old consumers");
        return CollectionConvertor.seqConvertJavaList(zkUtils.getConsumerGroups());
    }

    private List<String> listOldConsumerGroupsByTopic(@TopicExistConstraint String topic) throws Exception {

        List<String> consumersFromZk = zkClient.getChildren().forPath(ZkUtils.ConsumersPath());
        List<String> cList = new ArrayList<>();

        for (String consumer : consumersFromZk) {
            String path = ZkUtils.ConsumersPath() + "/" + consumer + "/offsets";
            if (zkClient.checkExists().forPath(path) != null) {
                if (zkClient.getChildren().forPath(path).size() != 0) {
                    if (!Strings.isNullOrEmpty(topic)) {
                        if (zkClient.getChildren().forPath(path).stream().filter(p -> p.equals(topic)).count() != 0)
                            cList.add(consumer);
                    } else {
                        cList.add(consumer);
                    }
                }
            }
        }

        return cList;

        //May cause keeperexception, deprecated
        //return JavaConverters.asJavaCollectionConverter(zkUtils.getAllConsumerGroupsForTopic(topic)).asJavaCollection().stream().collect(toList());
    }

    private List<String> listAllNewConsumerGroups() {
        log.info("Calling the listAllConsumerGroupsFlattened");
        List activeGroups = CollectionConvertor.seqConvertJavaList(kafkaAdminClient.listAllConsumerGroupsFlattened()).stream()
                .map(GroupOverview::groupId).collect(toList());
        log.info("Checking the groups in storage");
        List usedTobeGroups = storage.getMap().entrySet().stream().map(Map.Entry::getKey).collect(toList());

        //Merge two lists
        activeGroups.removeAll(usedTobeGroups);
        activeGroups.addAll(usedTobeGroups);
        Collections.sort(activeGroups);
        log.info("Finish getting new consumers");
        return activeGroups;
    }

    private List<String> listNewConsumerGroupsByTopic(@TopicExistConstraint String topic) {
        String t;
        List<String> consumersList = listAllNewConsumerGroups();

        Map<String, String> consumerTopicMap = new HashMap<>();
        for (String c : consumersList) {
            List<AdminClient.ConsumerSummary> consumerSummaryList = CollectionConvertor.listConvertJavaList(kafkaAdminClient.describeConsumerGroup(c));
            for (AdminClient.ConsumerSummary cs : consumerSummaryList) {
                List<TopicPartition> topicList = CollectionConvertor.listConvertJavaList(cs.assignment()).stream().collect(toList());
                for (TopicPartition tp : topicList) {
                    consumerTopicMap.put(c, tp.topic());
                }
            }
        }
        List<Map.Entry<String, String>> consumerEntryList = consumerTopicMap.entrySet().stream().collect(
                Collectors.groupingBy(
                        Map.Entry::getValue,
                        Collectors.toList()
                )
        ).getOrDefault(topic, new ArrayList<>());

        return consumerEntryList.stream().map(Map.Entry::getKey).collect(Collectors.toList());
    }

    public Map<String, List<ConsumerGroupDesc>> describeOldCG(String consumerGroup, String topic) {
        if (!isOldConsumerGroup(consumerGroup)) {
            throw new RuntimeException(consumerGroup + " non-exist");
        }
        Map<String, List<ConsumerGroupDesc>> result = new HashMap<>();
        List<ConsumerGroupDesc> cgdList = new ArrayList<>();
        Map<Integer, Long> fetchOffSetFromZKResultList = new HashMap<>();

        List<String> topicList = CollectionConvertor.seqConvertJavaList(zkUtils.getTopicsByConsumerGroup(consumerGroup));
        if (topicList.size() == 0) {
            return null;
        }

        if (!Strings.isNullOrEmpty(topic)) {
            topicList = Collections.singletonList(topic);
        }

        for (String t : topicList) {
            List<TopicAndPartition> topicPartitions = getTopicPartitions(t);
            ZKGroupTopicDirs groupDirs = new ZKGroupTopicDirs(consumerGroup, t);
            Map<Integer, String> ownerPartitionMap = topicPartitions.stream().collect(Collectors.toMap(
                    TopicAndPartition::partition,
                    tp -> {
                        Option<String> owner = zkUtils.readDataMaybeNull(groupDirs.consumerOwnerDir() + "/" + tp.partition())._1;
                        if (owner != NONE) {
                            return owner.get();
                        } else {
                            return "none";
                        }
                    }
                    )
            );

            ExecutorService executor = Executors.newCachedThreadPool();

            List<FetchOffsetFromZKTask> taskList = topicPartitions.stream().map(
                    tp -> new FetchOffsetFromZKTask(zookeeperUtils, tp.topic(), consumerGroup, tp.partition()))
                    .collect(toList());
            List<Future<FetchOffSetFromZKResult>> resultList = null;

            try {
                resultList = executor.invokeAll(taskList);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            executor.shutdown();

            for (int i = 0; i < resultList.size(); i++) {
                Future<FetchOffSetFromZKResult> future = resultList.get(i);
                try {
                    FetchOffSetFromZKResult offsetResult = future.get();
                    fetchOffSetFromZKResultList.put(
                            offsetResult.getParition(),
                            offsetResult.getOffset());
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }

            TopicMeta topicMeta = describeTopic(t);

            cgdList.addAll(setCGD(fetchOffSetFromZKResultList, ownerPartitionMap, t, consumerGroup, topicMeta));
            Collections.sort(cgdList);
            result.put(t, cgdList);
        }


        return result;
    }

    private List<ConsumerGroupDesc> setCGD(
            Map<Integer, Long> fetchOffSetFromZKResultList,
            Map<Integer, String> ownerPartitionMap,
            String topic, String consumerGroup, TopicMeta topicMeta) {
        return ownerPartitionMap.entrySet().stream().map(op -> {
            ConsumerGroupDesc cgd = new ConsumerGroupDesc();
            cgd.setGroupName(consumerGroup);
            cgd.setTopic(topic);
            cgd.setPartitionId(op.getKey());
            cgd.setCurrentOffset(fetchOffSetFromZKResultList.get(op.getKey()));
            cgd.setLogEndOffset(
                    topicMeta.getTopicPartitionInfos().stream()
                            .filter(tpi -> tpi.getPartitionId() == op.getKey()).findFirst().get().getEndOffset());
            cgd.setLag();
            if (op.getValue().equals("none")) {
                cgd.setConsumerId("-");
                cgd.setHost("-");
                cgd.setState(ConsumerState.PENDING);
            } else {
                cgd.setConsumerId(op.getValue());
                cgd.setHost(op.getValue().replace(consumerGroup + "_", ""));
                cgd.setState(ConsumerState.RUNNING);
            }
            cgd.setType(ConsumerType.OLD);
            return cgd;
        }).collect(toList());
    }

    public Map<String, List<ConsumerGroupDesc>> describeNewCG(String consumerGroup,
                                                              String topic) {
        if (!isNewConsumerGroup(consumerGroup)) {
            throw new RuntimeException(consumerGroup + " non-exist!");
        }

        Map<GroupTopicPartition, OffsetAndMetadata> cgdMap = storage.get(consumerGroup);

        List<AdminClient.ConsumerSummary> consumerSummaryList =
                CollectionConvertor.listConvertJavaList(kafkaAdminClient.describeConsumerGroup(consumerGroup));

        return setCGD(cgdMap, consumerSummaryList, consumerGroup, topic);
    }

    //Set consumergroupdescription for new consumer group
    private Map<String, List<ConsumerGroupDesc>> setCGD(
            Map<GroupTopicPartition, OffsetAndMetadata> storageMap,
            List<AdminClient.ConsumerSummary> consumerSummaryList,
            String consumerGroup,
            String topic) {
        Map<String, List<ConsumerGroupDesc>> result = new HashMap<>();
        List<ConsumerGroupDesc> cgdList;
        ConsumerGroupDesc cgd;
        ConsumerState state;
        List<String> topicList;
        ConsumerType type = ConsumerType.NEW;

        //Nothing about this consumer group obtained, return an empty map directly
        if (consumerSummaryList.size() == 0 && storageMap == null) {
            return Collections.emptyMap();
        }

        // If no information obtained from describeconsumergroup
        // we fetch topic list from our in-memory storageMap
        if ( consumerSummaryList.size() == 0 && storageMap != null) {
            topicList = storageMap.entrySet().stream()
                    .map(e -> e.getKey().topicPartition().topic()).distinct()
                    .filter(e -> {
                        if (Strings.isNullOrEmpty(topic)) {
                            return true;
                        } else {
                            return e.equals(topic);
                        }
                    })
                    .collect(toList());
        } else {
            topicList = consumerSummaryList.stream().flatMap(
                    cs -> CollectionConvertor.listConvertJavaList(cs.assignment()).stream())
                    .map(tp -> tp.topic()).distinct().collect(toList());
        }

        //Iterate the topic list
        for (String t : topicList) {

            TopicMeta topicMeta = describeTopic(t);
            Map<Integer, Long> partitionEndOffsetMap = topicMeta.getTopicPartitionInfos().stream()
                    .collect(Collectors.toMap(
                            tpi -> tpi.getPartitionId(),
                            tpi -> tpi.getEndOffset()
                            )
                    );

            //First get the information of this topic
            Map<GroupTopicPartition, OffsetAndMetadata> topicStorage = new HashMap<>();
            //Second get the current offset of each partition in this topic
            Map<Integer, Long> partitionCurrentOffsetMap = new HashMap<>();

            // Try to fetch current offset from in-memory storage map
            if (storageMap != null) {
                for (Map.Entry<GroupTopicPartition, OffsetAndMetadata> e : storageMap.entrySet()) {
                    if (e.getKey().topicPartition().topic().equals(t)) {
                        topicStorage.put(e.getKey(), e.getValue());
                    }
                }
            }

            //If the length of consumerSummaryList is 0, it indicates that the consumergroup's state is pending
            if (consumerSummaryList.size() == 0) {
                cgdList = new ArrayList<>();
                state = ConsumerState.PENDING;

                if (storageMap != null) {
                    partitionCurrentOffsetMap = topicStorage.entrySet().stream()
                            .filter(e -> e.getKey().topicPartition().topic().equals(t))
                            .collect(Collectors.toMap(
                                    e -> e.getKey().topicPartition().partition(),
                                    e -> {
                                        if (e.getValue() == null) {
                                            return -1l;
                                        } else {
                                            return e.getValue().offset();
                                        }
                                    }
                            ));
                }
                for (Map.Entry<GroupTopicPartition, OffsetAndMetadata> storage : topicStorage.entrySet()) {
                    cgd = new ConsumerGroupDesc();
                    cgd.setGroupName(consumerGroup);
                    cgd.setTopic(t);
                    cgd.setConsumerId("-");
                    cgd.setPartitionId(storage.getKey().topicPartition().partition());
                    if (partitionCurrentOffsetMap.size() != 0) {
                        cgd.setCurrentOffset(partitionCurrentOffsetMap.get(cgd.getPartitionId()));
                    } else {
                        cgd.setCurrentOffset(-1l);
                    }

                    Long endOffset = partitionEndOffsetMap.get(cgd.getPartitionId());

                    if(endOffset==null){ //if endOffset is null ,the partition of this topic has no leader replication
                        cgd.setLogEndOffset(-1l);
                    } else {
                        cgd.setLogEndOffset(endOffset);
                    }
                    cgd.setLag();
                    cgd.setHost("-");
                    cgd.setState(state);
                    cgd.setType(type);
                    cgdList.add(cgd);
                }

            }  // Indicates the consumer group is running
            else {
                cgdList = new ArrayList<>();
                state = ConsumerState.RUNNING;
                List<AdminClient.ConsumerSummary> consumerFilterList = consumerSummaryList.stream().filter(cs -> {
                    List<TopicPartition> assignment = CollectionConvertor.listConvertJavaList(cs.assignment());
                    if (assignment.stream().filter(tp -> tp.topic().equals(t)).count() != 0) {
                        return true;
                    } else {
                        return false;
                    }
                }).collect(toList());

                for (AdminClient.ConsumerSummary cs : consumerFilterList) {
                    //First get the end offset of each partition in a topic
                    List<TopicPartition> assignment = CollectionConvertor.listConvertJavaList(cs.assignment());

                    //Second get the current offset of each partition in this topic
                    if (storageMap != null) {
                        partitionCurrentOffsetMap = topicStorage.entrySet().stream()
                                .collect(Collectors.toMap(
                                        e -> e.getKey().topicPartition().partition(),
                                        e -> e.getValue().offset()
                                ));
                    }

                    for (TopicPartition tp : assignment) {
                        cgd = new ConsumerGroupDesc();
                        cgd.setGroupName(consumerGroup);
                        cgd.setTopic(tp.topic());
                        cgd.setPartitionId(tp.partition());
                        if (partitionCurrentOffsetMap.size() != 0) {
                            cgd.setCurrentOffset(partitionCurrentOffsetMap.get(tp.partition()));
                        } else {
                            cgd.setCurrentOffset(-1l);
                        }
                        Long endOffset = partitionEndOffsetMap.get(tp.partition());
                        if(endOffset==null){ //if endOffset is null ,the partition of this topic has no leader replication
                            cgd.setLogEndOffset(-1l);
                        } else {
                            cgd.setLogEndOffset(endOffset);
                        }
                        cgd.setLag();
                        cgd.setConsumerId(cs.clientId());
                        cgd.setHost(cs.clientHost());
                        cgd.setState(state);
                        cgd.setType(type);
                        cgdList.add(cgd);
                    }
                }
            }

            result.put(t, cgdList);
        }


        return result;
    }

    public String getMessage(@TopicExistConstraint String topic, int partition, long offset, String decoder, String avroSchema) {
        KafkaConsumer consumer = createNewConsumer(DEFAULTCP);
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
        consumer.assign(Arrays.asList(tp));
        consumer.seek(tp, offset);

        String last;

        while (true) {
            ConsumerRecords<String, String> crs = consumer.poll(channelRetryBackoffMs);
            if (crs.count() != 0) {
                Iterator<ConsumerRecord<String, String>> it = crs.iterator();

                ConsumerRecord<String, String> initCr = it.next();
                last = initCr.value() + String.valueOf(initCr.offset());
                while (it.hasNext()) {
                    ConsumerRecord<String, String> cr = it.next();
                    last = cr.value() + String.valueOf(cr.offset());
                }
                break;
            }
        }

        consumer.close();
        return last;
    }

    public GeneralResponse resetOffset(@TopicExistConstraint String topic, int partition,
                                       String consumerGroup,
                                       String type, String offset) {
        if (!Strings.isNullOrEmpty(type) && type.equals("new")) {
            if (!isNewConsumerGroup(consumerGroup)) {
                throw new ApiException("Consumer group " + consumerGroup + " is non-exist!");
            }
        }

        if (!Strings.isNullOrEmpty(type) && type.equals("old")) {
            if (!isOldConsumerGroup(consumerGroup)) {
                throw new ApiException("Consumer group " + consumerGroup + " is non-exist!");
            }
        }

        long offsetToBeReset;
        long beginningOffset = getBeginningOffset(topic, partition);
        long endOffset = getEndOffset(topic, partition);

        if (isConsumerGroupActive(consumerGroup, type)) {
            throw new ApiException("Assignments can only be reset if the group " + consumerGroup + " is inactive");
        }

        if ((!Strings.isNullOrEmpty(type) && type.equals("new")) && isNewConsumerGroup(consumerGroup)) {
            //if type is new or the consumergroup itself is new
            KafkaConsumer consumer = createNewConsumer(consumerGroup);
            TopicPartition tp = new TopicPartition(topic, partition);
            if (offset.equals("earliest")) {
                consumer.seekToBeginning(Arrays.asList(tp));
            } else if (offset.equals("latest")) {
                consumer.seekToEnd(Arrays.asList(tp));
            } else {
                if (Long.parseLong(offset) <= beginningOffset || Long.parseLong(offset) > endOffset) {
                    log.error(offset + " error");
                    consumer.close();
                    throw new ApiException(
                            "offsets must be between " + String.valueOf(beginningOffset
                                    + " and " + (endOffset - 1)
                            )
                    );
                }
                offsetToBeReset = Long.parseLong(offset);
                consumer.assign(Arrays.asList(tp));
                consumer.seek(tp, offsetToBeReset);
            }
            consumer.commitSync();
            consumer.close();
        }

        //if type is old or the consumer group itself is old
        if ((!Strings.isNullOrEmpty(type) && type.equals("old")) && isOldConsumerGroup(consumerGroup)) {
            if (offset.equals("earliest")) {
                offset = String.valueOf(beginningOffset);
            } else if (offset.equals("latest")) {
                offset = String.valueOf(endOffset);
            }
            try {
                if (Long.parseLong(offset) <= beginningOffset || Long.parseLong(offset) > endOffset) {
                    log.error(offset + " error");
                    throw new ApiException(
                            "offsets must be between " + String.valueOf(beginningOffset
                                    + " and " + (endOffset - 1)
                            )
                    );
                }
                zkUtils.zkClient().writeData(
                        "/consumers/" + consumerGroup + "/offsets/" + topic + "/" + partition,
                        offset);
            } catch (Exception e) {
                new ApiException("Wrote Data" +
                        offset + " to " + "/consumers/" + consumerGroup +
                        "/offsets/" + topic + "/" + partition);
            }
        }
        return new GeneralResponse(GeneralResponseState.success, "Reset the offset successfully!");
    }

    public Map<String, Map<Integer, Long>> getLastCommitTime(@ConsumerGroupExistConstraint String consumerGroup,
                                                                       @TopicExistConstraint String topic) {
        Map<String, Map<Integer, Long>> result = new ConcurrentHashMap<>();

        //Get Old Consumer commit time
        try {
            Map<Integer, Long> oldConsumerOffsetMap = new ConcurrentHashMap<>();
            if (zkClient.checkExists().forPath(CONSUMERPATHPREFIX + consumerGroup) != null
                    && zkClient.checkExists().forPath(CONSUMERPATHPREFIX + consumerGroup + OFFSETSPATHPREFIX + topic) != null) {
                List<String> offsets = zkClient.getChildren().forPath(CONSUMERPATHPREFIX + consumerGroup + OFFSETSPATHPREFIX + topic);
                for (String offset : offsets) {
                    Integer id = Integer.valueOf(offset);
                    long mtime = zkClient.checkExists().forPath(CONSUMERPATHPREFIX + consumerGroup + OFFSETSPATHPREFIX + topic + "/" + offset).getMtime();
                    oldConsumerOffsetMap.put(id, mtime);
                }

                result.put("old", oldConsumerOffsetMap);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


        //Get New consumer commit time, from offset storage instance
        if (storage.get(consumerGroup) != null) {
            Map<GroupTopicPartition, OffsetAndMetadata> storageResult = storage.get(consumerGroup);
            result.put("new", (storageResult.entrySet().parallelStream().filter(s -> s.getKey().topicPartition().topic().equals(topic))
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

        return result;
    }

    public GeneralResponse deleteConsumerGroup(String consumerGroup) {
        if (!isOldConsumerGroup(consumerGroup)) {
            throw new RuntimeException(consumerGroup + " non-exist");
        }
        try {
            if (zookeeperUtils.getCuratorClient().checkExists().forPath(CONSUMERPATHPREFIX + consumerGroup + "/ids") == null) {
                zookeeperUtils.getCuratorClient().delete().deletingChildrenIfNeeded().forPath(CONSUMERPATHPREFIX + consumerGroup);
            } else {
                if (!AdminUtils.deleteConsumerGroupInZK(zkUtils, consumerGroup)) {
                    throw new ApiException(consumerGroup + " has not been deleted for some reason");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new GeneralResponse(GeneralResponseState.success, consumerGroup + " has been deleted.");
    }

    public Map<String, List<String>> listAllConsumerGroups() {
        Map<String, List<String>> result = new HashMap<>();

        List<String> oldCGList = listAllOldConsumerGroups();
        if (oldCGList.size() != 0) {
            result.put("old", oldCGList);
        }

        List<String> newCGList = listAllNewConsumerGroups();
        if (newCGList.size() != 0) {
            result.put("new", newCGList);
        }

        return result;
    }

    public Map<String, List<String>> listConsumerGroupsByTopic(@TopicExistConstraint String topic) {
        Map<String, List<String>> result = new HashMap<>();

        List<String> oldCGList = null;
        try {
            oldCGList = listOldConsumerGroupsByTopic(topic);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (oldCGList.size() != 0) {
            result.put("old", oldCGList);
        }

        List<String> newCGList = null;
        newCGList = listNewConsumerGroupsByTopic(topic);

        if (newCGList.size() != 0) {
            result.put("new", newCGList);
        }

        return result;
    }

    private List<TopicAndPartition> getTopicPartitions(String t) {
        List<TopicAndPartition> tpList = new ArrayList<>();
        List<String> l = Arrays.asList(t);
        Map<String, Seq<Object>> tpMap = JavaConverters.mapAsJavaMapConverter(zkUtils.getPartitionsForTopics(JavaConverters.asScalaIteratorConverter(l.iterator()).asScala().toSeq())).asJava();
        if (tpMap != null) {
            ArrayList<Object> partitionLists = new ArrayList<>(JavaConverters.seqAsJavaListConverter(tpMap.get(t)).asJava());
            tpList = partitionLists.stream().map(p -> new TopicAndPartition(t, (Integer) p)).collect(toList());
        }
        return tpList;
    }

    private Properties getTopicPropsFromZk(String topic) {
        return AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topic);
    }


    private KafkaConsumer createNewConsumer(String consumerGroup) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUtils.getKafkaConfig().getBrokers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getCanonicalName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getCanonicalName());

        return new KafkaConsumer(properties);
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

        PartitionOffsetRequestInfo partitionOffsetRequestInfo = new PartitionOffsetRequestInfo(time, 10000);
        OffsetRequest offsetRequest = new OffsetRequest(ImmutableMap.of(topicAndPartition, partitionOffsetRequestInfo), kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());
        OffsetResponse offsetResponse = consumer.getOffsetsBefore(offsetRequest);

        if (offsetResponse.hasError()) {
            short errorCode = offsetResponse.errorCode(topic, partitionId);
            log.warn(format("Offset response has error: %d", errorCode));
            throw new ApiException("could not fetch data from Kafka, error code is '" + errorCode + "'Exception Message:" + offsetResponse.toString());
        }

        long[] offsets = offsetResponse.offsets(topic, partitionId);
        return offsets[0];
    }

    private long getOffsets(PartitionInfo partitionInfo, long time) {
        return getOffsets(partitionInfo.leader(), partitionInfo.topic(), partitionInfo.partition(), time);
    }

    public long getBeginningOffset(String topic, int partitionId) {
        return getOffsets(getLeader(topic, partitionId), topic, partitionId, kafka.api.OffsetRequest.EarliestTime());
    }

    public long getEndOffset(String topic, int partitionId) {
        return getOffsets(getLeader(topic, partitionId), topic, partitionId, kafka.api.OffsetRequest.LatestTime());
    }

    private long getBeginningOffset(Node leader, String topic, int partitionId) {
        return getOffsets(leader, topic, partitionId, kafka.api.OffsetRequest.EarliestTime());
    }

    private long getEndOffset(Node leader, String topic, int partitionId) {
        return getOffsets(leader, topic, partitionId, kafka.api.OffsetRequest.LatestTime());
    }

    private Node getLeader(String topic, int partitionId) {
        KafkaConsumer consumer = createNewConsumer(DEFAULTCP);
        List<PartitionInfo> tmList = consumer.partitionsFor(topic);

        PartitionInfo partitionInfo = tmList.stream().filter(pi -> pi.partition() == partitionId).findFirst().get();
        consumer.close();
        return partitionInfo.leader();
    }

    public boolean isOldConsumerGroup(String consumerGroup) {
        return listAllOldConsumerGroups().indexOf(consumerGroup) != -1;
    }

    public boolean isNewConsumerGroup(String consumerGroup) {
        Map<GroupTopicPartition, OffsetAndMetadata> cgdMap = storage.get(consumerGroup);

        //Active Consumergroup or Dead ConsumerGroup is OK
        return (listAllNewConsumerGroups().indexOf(consumerGroup) != -1) || ((cgdMap != null && cgdMap.size() != 0));
    }

    private boolean isConsumerGroupActive(String consumerGroup, String type) {
        if (type.equals("new")) {
            return CollectionConvertor.seqConvertJavaList(kafkaAdminClient.listAllConsumerGroupsFlattened()).stream()
                    .map(GroupOverview::groupId).filter(c -> c.equals(consumerGroup)).count() == 1;
        } else if (type.equals("old")) {
            return AdminUtils.isConsumerGroupActive(zkUtils, consumerGroup);
        } else {
            throw new ApiException("Unknown type " + type);
        }
    }
}
