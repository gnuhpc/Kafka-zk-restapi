package org.gnuhpc.bigdata.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import io.swagger.models.auth.In;
import kafka.admin.AdminClient;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.admin.TopicCommand;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.OffsetAndMetadata;
import kafka.common.TopicAndPartition;
import kafka.coordinator.group.GroupOverview;
import kafka.coordinator.group.GroupTopicPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.server.ConfigType;
import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZkUtils;
import kafka.zk.KafkaZkClient;
import lombok.extern.log4j.Log4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.internals.Topic;
import org.gnuhpc.bigdata.CollectionConvertor;
import org.gnuhpc.bigdata.componet.OffsetStorage;
import org.gnuhpc.bigdata.config.KafkaConfig;
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

    private org.apache.kafka.clients.admin.AdminClient kafkaAdminClient;
    //For Json serialized
    private Gson gson;

    private scala.Option<String> NONE = scala.Option.apply(null);
    @PostConstruct
    private void init() {
        this.zkUtils = zookeeperUtils.getZkUtils();
        this.zkClient = zookeeperUtils.getCuratorClient();
        this.kafkaZkClient = zookeeperUtils.getKafkaZkClient();
        Properties adminClientProp = new Properties();
        adminClientProp.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBrokers());
        //TODO eliminate the init connection
        kafkaAdminClient = KafkaAdminClient.create(adminClientProp);
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(DateTime.class, (JsonDeserializer<DateTime>) (jsonElement, type, jsonDeserializationContext) -> new DateTime(jsonElement.getAsJsonPrimitive().getAsLong()));

        this.gson = builder.create();
    }

    public TopicMeta createTopic(TopicDetail topic, String reassignStr) throws InterruptedException, ExecutionException {
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

            TopicCommand.createTopic(kafkaZkClient, new TopicCommand.TopicCommandOptions(argsList.stream().toArray(String[]::new)));
        }


        try {
            //Wait for a second for metadata propergating
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return describeTopic(topic.getName());
    }

    public List<String> listTopics() throws InterruptedException, ExecutionException {
        List<String> topicNamesList = new ArrayList<String>();
        topicNamesList.addAll(getAllTopics());

        return topicNamesList;
    }

    public Set<String> getAllTopics() throws InterruptedException, ExecutionException {
        ListTopicsOptions options = new ListTopicsOptions();
        // includes internal topics such as __consumer_offsets
        options.listInternal(true);

        ListTopicsResult topics = kafkaAdminClient.listTopics(options);
        Set<String> topicNames = topics.names().get();
        log.info("Current topics in this cluster: " + topicNames);

        return topicNames;
    }

    public List<TopicBrief> listTopicBrief() throws InterruptedException, ExecutionException {
        DescribeTopicsResult describeTopicsResult = kafkaAdminClient.describeTopics(listTopics());
        Map<String, TopicDescription> topicMap = describeTopicsResult.all().get();
        List<TopicBrief> result = topicMap.entrySet().parallelStream().map(e -> {
            String topic = e.getKey();
            TopicDescription topicDescription = e.getValue();
            List<org.apache.kafka.common.TopicPartitionInfo> topicPartitionInfoList = topicDescription.partitions();
            int replicateCount = 0;
            int isrCount = 0;
            for (org.apache.kafka.common.TopicPartitionInfo topicPartitionInfo: topicPartitionInfoList) {
                replicateCount += topicPartitionInfo.replicas().size();
                isrCount += topicPartitionInfo.isr().size();
            }
            if (replicateCount == 0) {
                return new TopicBrief(topic, topicDescription.partitions().size(), 0);
            } else {
                return new TopicBrief(topic, topicDescription.partitions().size(), ((double) isrCount / replicateCount));
            }
        }).collect(toList());

        return result;
    }

    public boolean existTopic(String topicName) {
        return kafkaZkClient.topicExists(topicName);
    }

    public List<BrokerInfo> listBrokers() {
        List<Broker> brokerList = CollectionConvertor.seqConvertJavaList(kafkaZkClient.getAllBrokersInCluster());

        return brokerList.parallelStream().collect(Collectors.toMap(Broker::id, Broker::rack)).entrySet().parallelStream()
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
                    if (entry.getValue().isEmpty())
                        brokerInfo.setRack("");
                    else {
                        brokerInfo.setRack(entry.getValue().get());
                    }
                    brokerInfo.setId(entry.getKey());
                    return brokerInfo;
                }).collect(toList());
    }

    public TopicMeta describeTopic(@TopicExistConstraint String topicName) throws InterruptedException, ExecutionException {
        DescribeTopicsResult describeTopicsResult = kafkaAdminClient.describeTopics(Collections.singletonList(topicName));
        Map<String, TopicDescription> topicMap = describeTopicsResult.all().get();
        TopicMeta topicMeta = new TopicMeta(topicName);
        if (topicMap.containsKey(topicName)) {
            TopicDescription topicDescription = topicMap.get(topicName);
            List<TopicPartitionInfo> tmList = topicDescription.partitions();
            topicMeta.setPartitionCount(topicDescription.partitions().size());
            topicMeta.setReplicationFactor(tmList.get(0).replicas().size());
            topicMeta.setTopicPartitionInfos(tmList.parallelStream().map(
                    tm -> {
                        CustomTopicPartitionInfo customTopicPartitionInfo = new CustomTopicPartitionInfo();
                        customTopicPartitionInfo.setTopicPartitionInfo(tm);
                        customTopicPartitionInfo.setIn_sync();
                        customTopicPartitionInfo.setStartOffset(getBeginningOffset(tm.leader(), topicName, tm.partition()));
                        customTopicPartitionInfo.setEndOffset(getEndOffset(tm.leader(), topicName, tm.partition()));
                        customTopicPartitionInfo.setMessageAvailable();
                        return customTopicPartitionInfo;

                    }).collect(toList())
            );
            Collections.sort(topicMeta.getTopicPartitionInfos());
        }

        return topicMeta;
    }

    public GeneralResponse deleteTopic(@TopicExistConstraint String topic)
            throws InterruptedException, ExecutionException {
        log.warn("Delete topic " + topic);
        kafkaAdminClient.deleteTopics(Collections.singletonList(topic)).all().get();

        return new GeneralResponse(GeneralResponseState.success, topic + " has been deleted.");
    }

    public Collection<ConfigEntry> describeConfig(ConfigResource.Type type, String name) throws ExecutionException, InterruptedException {
        DescribeConfigsResult ret = kafkaAdminClient.describeConfigs(Collections.singleton(new ConfigResource(type, name)));
        Map<ConfigResource, Config> configs = ret.all().get();
        Collection<ConfigEntry> configEntries = null;

        for (Map.Entry<ConfigResource, Config> entry : configs.entrySet()) {
            Config value = entry.getValue();
            configEntries = value.entries();
        }

        return configEntries;
    }

    public void alterConfig(ConfigResource.Type type, String name, Collection<ConfigEntry> configEntries)
            throws InterruptedException, ExecutionException{
        Config config = new Config(configEntries);
        kafkaAdminClient.alterConfigs(Collections.singletonMap(new ConfigResource(type, name), config)).all().get();
    }

    public Collection<ConfigEntry> updateTopicConf(@TopicExistConstraint String topic, Properties props)
            throws InterruptedException, ExecutionException {
        Collection<ConfigEntry> configEntries = props.entrySet().stream().map(
                e -> new ConfigEntry(e.getKey().toString(), e.getValue().toString())).collect(Collectors.toList());
        alterConfig(ConfigResource.Type.TOPIC, topic, configEntries);

        return describeConfig(ConfigResource.Type.TOPIC, topic);
    }

    public Collection<ConfigEntry> getTopicConf(@TopicExistConstraint String topic)
            throws InterruptedException, ExecutionException {
        return describeConfig(ConfigResource.Type.TOPIC, topic);
    }

    public Properties getTopicConfByKey(@TopicExistConstraint String topic, String key)
            throws InterruptedException, ExecutionException{
        Collection<ConfigEntry> configEntries = describeConfig(ConfigResource.Type.TOPIC, topic);
        Properties returnProps = new Properties();
        for (ConfigEntry entry : configEntries)
            if (entry.name().equals(key)) {
                returnProps.put(key, entry.value());
                return returnProps;
            }

        return null;
    }

    public Collection<ConfigEntry> updateTopicConfByKey(@TopicExistConstraint String topic, String key, String value) throws InterruptedException, ExecutionException{
        alterConfig(ConfigResource.Type.TOPIC, topic, Collections.singletonList(new ConfigEntry(key, value)));

        return describeConfig(ConfigResource.Type.TOPIC, topic);
    }

    /*
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
    */
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
        AdminClient adminClient = kafkaUtils.createAdminClient();
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

    /*
    private Set<String> listNewConsumerGroupsByTopic(@TopicExistConstraint String topic) {
        Set<String> result = new HashSet();
        Set<String> consumersList = listAllNewConsumerGroups();

        for (String c : consumersList) {
            AdminClient adminClient = kafkaUtils.createAdminClient();

            List<AdminClient.ConsumerSummary> consumerSummaryList = CollectionConvertor.listConvertJavaList(adminClient.describeConsumerGroup(c));
            Set<String> topicSet = consumerSummaryList.stream()
                    .flatMap(cs -> CollectionConvertor.listConvertJavaList(cs.assignment()).stream())
                    .map(TopicPartition::topic).filter(t -> t.equals(topic)).distinct()
                    .collect(toSet());

            if (topicSet.size() != 0) {
                result.add(c);
            }
            adminClient.close();
        }
        return result;
    }*/

    public List<ConsumerGroupDesc> describeOldCGByTopic(String consumerGroup, @TopicExistConstraint String topic) throws InterruptedException, ExecutionException{
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

        cgdList.addAll(setOldCGD(fetchOffSetFromZKResultList, ownerPartitionMap, topic, consumerGroup, topicMeta));
        Collections.sort(cgdList);

        return cgdList;
    }

    private List<ConsumerGroupDesc> setOldCGD(
            Map<Integer, Long> fetchOffSetFromZKResultList,
            Map<Integer, String> ownerPartitionMap,
            String topic, String consumerGroup, TopicMeta topicMeta) {
        ConsumerGroupDescFactory factory = new ConsumerGroupDescFactory(kafkaUtils);
        return ownerPartitionMap.entrySet().stream().map(op ->
                factory.makeOldConsumerGroupDesc(
                        op, fetchOffSetFromZKResultList, topic, consumerGroup, topicMeta
                )
        )
                .collect(toList());
    }
/*
    public List<ConsumerGroupDesc> describeNewCGByTopic(String consumerGroup,
                                                        String topic) {
        if (!isNewConsumerGroup(consumerGroup)) {
            throw new RuntimeException(consumerGroup + " non-exist!");
        }

        return setNewCGD(consumerGroup, topic);
    }


    private List<ConsumerGroupDesc> setNewCGD(String consumerGroup, String topic) {
        List<ConsumerGroupDesc> cgdList = new ArrayList<>();
        AdminClient adminClient = kafkaUtils.createAdminClient();

        List<AdminClient.ConsumerSummary> consumerSummaryList =
                CollectionConvertor.listConvertJavaList(adminClient.describeConsumerGroup(consumerGroup));
        //Nothing about this consumer group obtained, return an empty map directly
        adminClient.close();

        List<AdminClient.ConsumerSummary> filteredCSList = consumerSummaryList.parallelStream()
                .filter(cs ->
                        CollectionConvertor.listConvertJavaList(cs.assignment()).parallelStream()
                                .filter(tp -> tp.topic().equals(topic)).count() != 0)
                .collect(toList());

        //Prepare the common metrics no matter the cg is active or not.

        //1. Get the meta information of the topic
        TopicMeta topicMeta = describeTopic(topic);

        //2. Get the log end offset for every partition
        Map<Integer, Long> partitionEndOffsetMap = topicMeta.getTopicPartitionInfos().stream()
                .collect(Collectors.toMap(
                        tpi -> tpi.getPartitionId(),
                        tpi -> tpi.getEndOffset()
                        )
                );
        if (filteredCSList.size() == 0) {//For Pending consumer group

            //Even from the offsetstorage, nothing about this consumer group obtained
            // In this case, return an empty map directly.
            Map<GroupTopicPartition, OffsetAndMetadata> storageMap = storage.get(consumerGroup);
            if (storageMap == null) {
                return null;
            }

            //Get the current offset of each partition in this topic.
            Map<GroupTopicPartition, OffsetAndMetadata> topicStorage = new HashMap<>();
            for (Map.Entry<GroupTopicPartition, OffsetAndMetadata> e : storageMap.entrySet()) {
                if (e.getKey().topicPartition().topic().equals(topic)) {
                    topicStorage.put(e.getKey(), e.getValue());
                }
            }

            //Build consumer group description
            ConsumerGroupDescFactory factory = new ConsumerGroupDescFactory(kafkaUtils);
            cgdList.addAll(
                    topicStorage.entrySet().stream().map(
                            storage -> factory.makeNewPendingConsumerGroupDesc(
                                    consumerGroup,
                                    partitionEndOffsetMap,
                                    storage,
                                    topic)
                    ).collect(toList()));

        } else { //For running consumer group
            //Build consumer group description
            ConsumerGroupDescFactory factory = new ConsumerGroupDescFactory(kafkaUtils);
            for (AdminClient.ConsumerSummary cs : filteredCSList) {
                List<TopicPartition> assignment = CollectionConvertor.listConvertJavaList(cs.assignment());
                //Second get the current offset of each partition in this topic

                cgdList.addAll(assignment.parallelStream()
                        .filter(tp->tp.topic().equals(topic))
                        .map(tp -> factory.makeNewRunningConsumerGroupDesc(tp, consumerGroup, partitionEndOffsetMap, cs)
                ).collect(toList()));
            }
        }

        return cgdList;
    }
*/

    public String getMessage(@TopicExistConstraint String topic, int partition, long offset, String decoder, String avroSchema) {
        KafkaConsumer consumer = kafkaUtils.createNewConsumer(String.valueOf(System.currentTimeMillis()));
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
        log.info("Seek to offset:" + offset + ", topic:" + topic + ", partition:" + partition + ", crs.count:" + crs.count());
        if (crs.count() != 0) {
            Iterator<ConsumerRecord<String, String>> it = crs.iterator();
            while (it.hasNext()) {
                ConsumerRecord<String, String> initCr = it.next();
                last = "Value: " + initCr.value() + ", Offset: " + String.valueOf(initCr.offset());
                log.info("Value: " + initCr.value() + ", initCr.Offset: " + String.valueOf(initCr.offset()));
                if (last != null && initCr.offset() == offset) {
                    break;
                }
            }
        }
        log.info("last:" + last);
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
                consumer = kafkaUtils.createNewConsumer(consumerGroup);
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
                    throw new ApiException("The consumer " + consumerGroup + " is still active.Please stop it first");
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
    /*
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
    */
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
        return getOffsets(kafkaUtils.getLeader(topic, partitionId), topic, partitionId, kafka.api.OffsetRequest.EarliestTime());
    }

    public long getEndOffset(String topic, int partitionId) {
        return getOffsets(kafkaUtils.getLeader(topic, partitionId), topic, partitionId, kafka.api.OffsetRequest.LatestTime());
    }

    private long getBeginningOffset(Node leader, String topic, int partitionId) {
        return getOffsets(leader, topic, partitionId, kafka.api.OffsetRequest.EarliestTime());
    }

    private long getEndOffset(Node leader, String topic, int partitionId) {
        return getOffsets(leader, topic, partitionId, kafka.api.OffsetRequest.LatestTime());
    }


    public boolean isOldConsumerGroup(String consumerGroup) {
        return listAllOldConsumerGroups().contains(consumerGroup);
    }

    public boolean isNewConsumerGroup(String consumerGroup) {
        //Active Consumergroup or Dead ConsumerGroup is OK
        return (listAllNewConsumerGroups().contains(consumerGroup));
    }

    /*
    public Set<String> listTopicsByCG(String consumerGroup, ConsumerType type) {
        Set<String> topicList = new HashSet<>();

        if (type == null) {
            throw new ApiException("Unknown Type " + type);
        }

        if (type == ConsumerType.OLD) {
            if (!isOldConsumerGroup(consumerGroup)) {
                throw new RuntimeException(consumerGroup + " non-exist");
            }

            topicList = new HashSet<>(
                    CollectionConvertor.seqConvertJavaList(zkUtils.getTopicsByConsumerGroup(consumerGroup))
            );
        } else if (type == ConsumerType.NEW) {
            if (!isNewConsumerGroup(consumerGroup)) {
                throw new RuntimeException(consumerGroup + " non-exist!");
            }

            AdminClient adminClient = kafkaUtils.createAdminClient();

            List<AdminClient.ConsumerSummary> consumerSummaryList =
                    CollectionConvertor.listConvertJavaList(adminClient.describeConsumerGroup(consumerGroup));
            //Nothing about this consumer group obtained, return an empty map directly
            adminClient.close();

            if (isConsumerGroupActive(consumerGroup, ConsumerType.NEW) &&
                    consumerSummaryList.size() != 0) {

                //Get topic list and filter if topic is set
                topicList.addAll(consumerSummaryList.stream().flatMap(
                        cs -> CollectionConvertor.listConvertJavaList(cs.assignment()).stream())
                        .map(tp -> tp.topic()).distinct()
                        .collect(toList()));
            }

            if (consumerSummaryList.size() == 0) {   //PENDING Consumer Group
                Map<GroupTopicPartition, OffsetAndMetadata> storageMap = storage.get(consumerGroup);
                if (storageMap == null) {
                    return null;
                }

                //Fetch the topics involved by consumer. And filter it by topic name
                topicList.addAll(storageMap.entrySet().stream()
                        .map(e -> e.getKey().topicPartition().topic()).distinct()
                        .collect(toList()));
            }
        } else {
            throw new ApiException("Unknown Type " + type);
        }

        return topicList;

    }

    public Map<String, List<ConsumerGroupDesc>> describeConsumerGroup(String consumerGroup, ConsumerType type) {
        Map<String, List<ConsumerGroupDesc>> result = new HashMap<>();
        Set<String> topicList = listTopicsByCG(consumerGroup, type);
        if (topicList == null) {
            //Return empty result
            return result;
        }
        if (type == ConsumerType.NEW) {
            for (String topic : topicList) {
                result.put(topic, describeNewCGByTopic(consumerGroup, topic));
            }

        } else if (type == ConsumerType.OLD) {
            for (String topic : topicList) {
                result.put(topic, describeOldCGByTopic(consumerGroup, topic));
            }
        }

        return result;
    }
*/
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
            AdminClient adminClient = kafkaUtils.createAdminClient();
            boolean isActive = CollectionConvertor.seqConvertJavaList(adminClient.listAllConsumerGroupsFlattened()).stream()
                    .map(GroupOverview::groupId).filter(c -> c.equals(consumerGroup)).count() == 1;
            adminClient.close();
            return isActive;
        } else if (type == ConsumerType.OLD) {
            return AdminUtils.isConsumerGroupActive(zookeeperUtils.getZkUtils(), consumerGroup);
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
            healthCheckResult.setMsg("HealthCheckTopic: " + healthCheckTopic + " Non-Exist. Please create it before doing health check.");
            return healthCheckResult;
        }

        String message = "healthcheck_" + System.currentTimeMillis();
        ProducerRecord<String, String> record = new ProducerRecord(healthCheckTopic, null, message);
        log.info("Generate message:" + message);
        try {
            RecordMetadata recordMetadata = (RecordMetadata) producer.send(record).get();
            log.info("Message:" + message + " has been sent to Partition:" + recordMetadata.partition());
        } catch (Exception e){
            healthCheckResult.setStatus("error");
            healthCheckResult.setMsg("Health Check: Produce Message Failure. Exception: " + e.getMessage());
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
                if (noRecordsCount > retries) break;
                else continue;
            }
            Iterator<ConsumerRecord<Long, String>> iterator = consumerRecords.iterator();
            while(iterator.hasNext()) {
                ConsumerRecord msg = iterator.next();
                log.info("Health Check: Fetch Message " + msg.value() + ", offset:" + msg.offset());
                if(msg.value().equals(message)) {
                    healthCheckResult.setStatus("ok");
                    healthCheckResult.setMsg(message);
                    return healthCheckResult;
                }
            }
            consumer.commitAsync();
        }
        consumer.close();

        if(healthCheckResult.getStatus() == null) {
            healthCheckResult.setStatus("error");
            healthCheckResult.setMsg("Health Check: Consume Message Failure. Consumer can't fetch the message.");
        }
        return healthCheckResult;
    }

    public void printObjectInJson(Object object) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            //objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
            System.out.println("object:" + objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(object));
        } catch (JsonProcessingException jsonProcessingException) {
            jsonProcessingException.printStackTrace();
        }
    }
}
