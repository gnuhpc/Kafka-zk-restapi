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
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

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

    //For AdminUtils use
    private ZkUtils zkUtils;

    //For zookeeper connection
    private CuratorFramework zkClient;

    //For Json serialized
    private Gson gson;

    private scala.Option<String> NONE = scala.Option.apply(null);

    @PostConstruct
    private void init() {
        this.zkUtils = zookeeperUtils.getZkUtils();
        this.zkClient = zookeeperUtils.getCuratorClient();
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(DateTime.class, (JsonDeserializer<DateTime>) (jsonElement, type, jsonDeserializationContext) -> new DateTime(jsonElement.getAsJsonPrimitive().getAsLong()));

        this.gson = builder.create();
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


        try {
            //Wait for a second for metadata propergating
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
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
                    long replicateCount = e.getValue().parallelStream().flatMap(pi -> Arrays.stream(pi.replicas())).count();
                    long isrCount = e.getValue().parallelStream().flatMap(pi -> Arrays.stream(pi.inSyncReplicas())).count();
                    if (replicateCount == 0) {
                        return new TopicBrief(topic, e.getValue().size(), 0);
                    } else {
                        return new TopicBrief(topic, e.getValue().size(), ((double) isrCount / replicateCount));
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
        List<Integer> brokerIdList = listBrokers().stream().map(broker -> broker.getId()).collect(toList());
        List partitionIdList = partitionMataData.stream().map(p -> String.valueOf(p.partition())).collect(toList());
        String assignmentStr = addPartition.getReplicaAssignment();
        String toBeSetReplicaAssignmentStr = "";

        if (assignmentStr != null && !assignmentStr.equals("")) {
            //Check out of index ids in replica assignment string
            String[] ids = addPartition.getReplicaAssignment().split(",|:");
            if (Arrays.stream(ids).filter(id -> brokerIdList.contains(id)).count() != 0) {
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

    private Set<String> listAllOldConsumerGroups() {
        log.info("Finish getting old consumers");
        return CollectionConvertor.seqConvertJavaList(zkUtils.getConsumerGroups()).stream().collect(toSet());
    }

    private Set<String> listOldConsumerGroupsByTopic(@TopicExistConstraint String topic) throws Exception {

        List<String> consumersFromZk = zkClient.getChildren().forPath(ZkUtils.ConsumersPath());
        Set<String> cList = new HashSet<>();

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

    private Set<String> listAllNewConsumerGroups() {
        AdminClient adminClient = createAdminClient();
        log.info("Calling the listAllConsumerGroupsFlattened");
        Set activeGroups = CollectionConvertor.seqConvertJavaList(adminClient.listAllConsumerGroupsFlattened()).stream()
                .map(GroupOverview::groupId).collect(toSet());
        log.info("Checking the groups in storage");
        Set usedTobeGroups = storage.getMap().entrySet().stream().map(Map.Entry::getKey).collect(toSet());
        activeGroups.addAll(usedTobeGroups);
        log.info("Finish getting new consumers");
        adminClient.close();
        return activeGroups;
    }

    private Set<String> listNewConsumerGroupsByTopic(@TopicExistConstraint String topic) {
        Set<String> result = new HashSet();
        Set<String> consumersList = listAllNewConsumerGroups();

        for (String c : consumersList) {
            AdminClient adminClient = createAdminClient();

            List<AdminClient.ConsumerSummary> consumerSummaryList = CollectionConvertor.listConvertJavaList(adminClient.describeConsumerGroup(c));
            Set<String> topicSet = consumerSummaryList.stream()
                    .flatMap(cs -> CollectionConvertor.listConvertJavaList(cs.assignment()).stream())
                    .map(TopicPartition::topic).filter(t->t.equals(topic)).distinct()
                    .collect(toSet());

            if (topicSet.size() != 0) {
                result.add(c);
            }
            adminClient.close();
        }
        return result;
    }

    public List<ConsumerGroupDesc> describeOldCGByTopic(String consumerGroup, String topic) {
        if (!isOldConsumerGroup(consumerGroup)) {
            throw new RuntimeException(consumerGroup + " non-exist");
        }
        List<ConsumerGroupDesc> cgdList = new ArrayList<>();
        Map<Integer, Long> fetchOffSetFromZKResultList = new HashMap<>();

        List<String> topicList = CollectionConvertor.seqConvertJavaList(zkUtils.getTopicsByConsumerGroup(consumerGroup));
        if (topicList.size() == 0) {
            log.info("No topic for the consumer group, nothing return");
            return null;
        }

        List<TopicAndPartition> topicPartitions = getTopicPartitions(topic);
        ZKGroupTopicDirs groupDirs = new ZKGroupTopicDirs(consumerGroup, topic);
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


        log.info("Getting topic Metadata " + topic);
        TopicMeta topicMeta = describeTopic(topic);

        cgdList.addAll(setCGD(fetchOffSetFromZKResultList, ownerPartitionMap, topic, consumerGroup, topicMeta));
        Collections.sort(cgdList);

        return cgdList;
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

    public List<ConsumerGroupDesc> describeNewCGByTopic(String consumerGroup,
                                                        String topic) {
        if (!isNewConsumerGroup(consumerGroup)) {
            throw new RuntimeException(consumerGroup + " non-exist!");
        }

        if (isConsumerGroupActive(consumerGroup, ConsumerType.NEW)) {
            return setRunningCGD(consumerGroup, topic);
        } else {
            return setPendingCGD(consumerGroup, topic);
        }
    }

    //Set consumergroupdescription for running new consumer group
    private List<ConsumerGroupDesc> setRunningCGD(
            String consumerGroup,
            String topic) {
        List<ConsumerGroupDesc> cgdList;
        ConsumerGroupDesc cgd;
        ConsumerState state = ConsumerState.RUNNING;
        ConsumerType type = ConsumerType.NEW;

        AdminClient adminClient = createAdminClient();

        List<AdminClient.ConsumerSummary> consumerSummaryList =
                CollectionConvertor.listConvertJavaList(adminClient.describeConsumerGroup(consumerGroup));
        //Nothing about this consumer group obtained, return an empty map directly
        adminClient.close();

        if (consumerSummaryList.size() == 0) {
            return null;
        }

        List<AdminClient.ConsumerSummary> consumerFilterList = consumerSummaryList.stream().filter(cs -> {
            List<TopicPartition> assignment = CollectionConvertor.listConvertJavaList(cs.assignment());
            if (assignment.stream().filter(tp -> tp.topic().equals(topic)).count() != 0) {
                return true;
            } else {
                return false;
            }
        }).collect(toList());

        cgdList = new ArrayList<>();
        //Get the meta information of the topic
        TopicMeta topicMeta = describeTopic(topic);
        //Construct <PartitionID, End offset> Map
        Map<Integer, Long> partitionEndOffsetMap = topicMeta.getTopicPartitionInfos().stream()
                .collect(Collectors.toMap(
                        tpi -> tpi.getPartitionId(),
                        tpi -> tpi.getEndOffset()
                        )
                );


        KafkaConsumer consumer = createNewConsumer(consumerGroup);
        for (AdminClient.ConsumerSummary cs : consumerFilterList) {
            List<TopicPartition> assignment = CollectionConvertor.listConvertJavaList(cs.assignment());
            //Second get the current offset of each partition in this topic

            for (TopicPartition tp : assignment) {
                cgd = new ConsumerGroupDesc();
                cgd.setGroupName(consumerGroup);
                cgd.setTopic(tp.topic());
                cgd.setPartitionId(tp.partition());
                long currentOffset = -1l;
                org.apache.kafka.clients.consumer.OffsetAndMetadata offset = consumer.committed(new TopicPartition(tp.topic(), tp.partition()));
                if (offset != null) {
                    currentOffset = offset.offset();
                }

                cgd.setCurrentOffset(currentOffset);

                Long endOffset = partitionEndOffsetMap.get(tp.partition());
                if (endOffset == null) { //if endOffset is null ,the partition of this topic has no leader replication
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
        consumer.close();
        return cgdList;

    }

    //Set consumergroupdescription for pending new consumer group
    private List<ConsumerGroupDesc> setPendingCGD(
            String consumerGroup, String topic) {
        Map<GroupTopicPartition, OffsetAndMetadata> storageMap = storage.get(consumerGroup);
        List<ConsumerGroupDesc> cgdList;
        ConsumerState state = ConsumerState.PENDING;
        ConsumerType type = ConsumerType.NEW;
        ConsumerGroupDesc cgd;
        List<String> topicList;
        Map<Integer, Long> partitionCurrentOffsetMap;

        //Nothing about this consumer group obtained, return an empty map directly
        if (storageMap == null) {
            return null;
        }

        //Get the metadata of the topic
        TopicMeta topicMeta = describeTopic(topic);

        //Construct <PartitionID, EndOffset> Map
        Map<Integer, Long> partitionEndOffsetMap = topicMeta.getTopicPartitionInfos().stream()
                .collect(Collectors.toMap(
                        tpi -> tpi.getPartitionId(),
                        tpi -> tpi.getEndOffset()
                        )
                );

        //First get the information of this topic
        Map<GroupTopicPartition, OffsetAndMetadata> topicStorage = new HashMap<>();
        cgdList = new ArrayList<>();
        //Second get the current offset of each partition in this topic.
        //Try to fetch it from in-memory storage map
        for (Map.Entry<GroupTopicPartition, OffsetAndMetadata> e : storageMap.entrySet()) {
            if (e.getKey().topicPartition().topic().equals(topic)) {
                topicStorage.put(e.getKey(), e.getValue());
            }
        }

        //Construct <PartitionID, current offset> Map
        partitionCurrentOffsetMap = topicStorage.entrySet().stream()
                .filter(e -> e.getKey().topicPartition().topic().equals(topic))
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

        for (Map.Entry<GroupTopicPartition, OffsetAndMetadata> storage : topicStorage.entrySet()) {
            cgd = new ConsumerGroupDesc();
            cgd.setGroupName(consumerGroup);
            cgd.setTopic(topic);
            cgd.setConsumerId("-");
            cgd.setPartitionId(storage.getKey().topicPartition().partition());
            cgd.setCurrentOffset(partitionCurrentOffsetMap.get(cgd.getPartitionId()));

            Long endOffset = partitionEndOffsetMap.get(cgd.getPartitionId());

            if (endOffset == null) { //if endOffset is null ,the partition of this topic has no leader replication
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

        return cgdList;
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

        long offsetToBeReset;
        long beginningOffset = getBeginningOffset(topic, partition);
        long endOffset = getEndOffset(topic, partition);

        log.info("To tell the consumergroup " + consumerGroup + " is active now");
        if (isConsumerGroupActive(consumerGroup, type)) {
            throw new ApiException("Assignments can only be reset if the group " + consumerGroup + " is inactive");
        }


        if (type != null && type == ConsumerType.NEW && isNewConsumerGroup(consumerGroup)) {
            try {
                log.info("The consumergroup " + consumerGroup + " is new. Reset offset now");
                consumer = createNewConsumer(consumerGroup);
                //if type is new or the consumergroup itself is new
                TopicPartition tp = new TopicPartition(topic, partition);
                consumer.assign(Arrays.asList(tp));
                consumer.poll(channelSocketTimeoutMs);
                if (offset.equals("earliest")) {
                    consumer.seekToBeginning(Arrays.asList(tp));
                    log.info("Reset to" + consumer.position(tp));
                } else if (offset.equals("latest")) {
                    consumer.seekToEnd(Arrays.asList(tp));
                    log.info("Reset to" + consumer.position(tp));
                } else {
                    if (Long.parseLong(offset) < beginningOffset || Long.parseLong(offset) > endOffset) {
                        log.error(offset + " error");
                        throw new ApiException(
                                "offsets must be between " + String.valueOf(beginningOffset
                                        + " and " + (endOffset - 1)
                                )
                        );
                    }
                    offsetToBeReset = Long.parseLong(offset);
                    consumer.seek(tp, offsetToBeReset);
                }
                consumer.commitSync();
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
            } else if (offset.equals("latest")) {
                offset = String.valueOf(endOffset);
            }
            try {
                if (Long.parseLong(offset) < beginningOffset || Long.parseLong(offset) > endOffset) {
                    log.info("Setting offset to " + offset + " error");
                    throw new ApiException(
                            "offsets must be between " + String.valueOf(beginningOffset
                                    + " and " + (endOffset - 1)
                            )
                    );
                }
                log.info("Offset will be reset to " + offset);
                zkUtils.zkClient().writeData(
                        "/consumers/" + consumerGroup + "/offsets/" + topic + "/" + partition,
                        offset);
            } catch (Exception e) {
                throw new ApiException(e);
            }
        }
        return new GeneralResponse(GeneralResponseState.success, "Reset the offset successfully!");
    }

    public Map<String, Map<Integer, java.lang.Long>> getLastCommitTime(@ConsumerGroupExistConstraint String consumerGroup,
                                                                       @TopicExistConstraint String topic,
                                                                       ConsumerType type) {
        Map<String, Map<Integer, java.lang.Long>> result = new ConcurrentHashMap<>();

        if (type != null && type == ConsumerType.OLD) {
            //Get Old Consumer commit time
            try {
                Map<Integer, java.lang.Long> oldConsumerOffsetMap = new ConcurrentHashMap<>();
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

        } else {
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

    private List<TopicAndPartition> getTopicPartitions(String t) {
        List<TopicAndPartition> tpList = new ArrayList<>();
        List<String> l = Arrays.asList(t);
        java.util.Map<String, Seq<Object>> tpMap = JavaConverters.mapAsJavaMapConverter(zkUtils.getPartitionsForTopics(JavaConverters.asScalaIteratorConverter(l.iterator()).asScala().toSeq())).asJava();
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
        consumer.close();
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
        return listAllOldConsumerGroups().contains(consumerGroup);
    }

    public boolean isNewConsumerGroup(String consumerGroup) {
        //Active Consumergroup or Dead ConsumerGroup is OK
        return (listAllNewConsumerGroups().contains(consumerGroup));
    }

    private boolean isConsumerGroupActive(String consumerGroup, ConsumerType type) {
        if (type == ConsumerType.NEW) {
            AdminClient adminClient = createAdminClient();
            boolean isActive = CollectionConvertor.seqConvertJavaList(adminClient.listAllConsumerGroupsFlattened()).stream()
                    .map(GroupOverview::groupId).filter(c -> c.equals(consumerGroup)).count() == 1;
            adminClient.close();
            return isActive;
        } else if (type == ConsumerType.OLD) {
            return AdminUtils.isConsumerGroupActive(zkUtils, consumerGroup);
        } else {
            throw new ApiException("Unknown type " + type);
        }
    }


    private AdminClient createAdminClient() {
        return AdminClient.createSimplePlaintext(kafkaUtils.getKafkaConfig().getBrokers());
    }

    public List<String> listTopicsByCG(String consumerGroup, ConsumerType type) {
        List<String> topicList = null;

        if (type == null) {
            throw new ApiException("Unknown Type " + type);
        }

        if (type == ConsumerType.OLD) {
            if (!isOldConsumerGroup(consumerGroup)) {
                throw new RuntimeException(consumerGroup + " non-exist");
            }

            topicList = CollectionConvertor.seqConvertJavaList(zkUtils.getTopicsByConsumerGroup(consumerGroup));
        } else if (type == ConsumerType.NEW) {
            if (!isNewConsumerGroup(consumerGroup)) {
                throw new RuntimeException(consumerGroup + " non-exist!");
            }

            if (isConsumerGroupActive(consumerGroup, ConsumerType.NEW)) {
                AdminClient adminClient = createAdminClient();

                List<AdminClient.ConsumerSummary> consumerSummaryList =
                        CollectionConvertor.listConvertJavaList(adminClient.describeConsumerGroup(consumerGroup));
                //Nothing about this consumer group obtained, return an empty map directly
                adminClient.close();

                if (consumerSummaryList.size() == 0) {
                    return null;
                } else {
                    //Get topic list and filter if topic is set
                    topicList = consumerSummaryList.stream().flatMap(
                            cs -> CollectionConvertor.listConvertJavaList(cs.assignment()).stream())
                            .map(tp -> tp.topic()).distinct()
                            .collect(toList());
                }
            } else {
                Map<GroupTopicPartition, OffsetAndMetadata> storageMap = storage.get(consumerGroup);
                if (storageMap == null) {
                    return null;
                }

                //Fetch the topics involved by consumer. And filter it by topic name
                topicList = storageMap.entrySet().stream()
                        .map(e -> e.getKey().topicPartition().topic()).distinct()
                        .collect(toList());
            }
        } else {
            throw new ApiException("Unknown Type " + type);
        }

        return topicList;

    }

    public Map<String, List<ConsumerGroupDesc>> describeConsumerGroup(String consumerGroup, ConsumerType type) {
        Map<String, List<ConsumerGroupDesc>> result = new HashMap<>();
        List<String> topicList = listTopicsByCG(consumerGroup, type);
        if(topicList==null){
            storage.remove(consumerGroup);
            return result;
        }
        if (type == ConsumerType.NEW) {
            for (String topic : topicList) {
                result.put(topic, describeNewCGByTopic(consumerGroup, topic));
            }

            if (result.size()==0){
                storage.remove(consumerGroup);
            }
        } else if (type == ConsumerType.OLD) {
            for (String topic : topicList) {
                result.put(topic, describeOldCGByTopic(consumerGroup, topic));
            }
        }

        return result;
    }

    public Map<Integer, Long> countPartition(String topic) {
        KafkaConsumer consumer = createNewConsumer(DEFAULTCP);
        List<PartitionInfo> piList = consumer.partitionsFor(topic);
        Map<Integer, Long> result = piList.stream().flatMap(pi -> Arrays.stream(pi.replicas()))
                .map(node -> node.id()).collect(Collectors.groupingBy(
                        Function.identity(), Collectors.counting()
                ));

        consumer.close();

        return result;

    }
}
