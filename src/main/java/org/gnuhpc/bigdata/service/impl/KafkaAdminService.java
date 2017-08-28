package org.gnuhpc.bigdata.service.impl;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import kafka.admin.*;
import kafka.cluster.Broker;
import kafka.common.OffsetAndMetadata;
import kafka.common.TopicAndPartition;
import kafka.controller.ReassignedPartitionsContext;
import kafka.coordinator.group.GroupOverview;
import kafka.coordinator.group.GroupTopicPartition;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import lombok.extern.log4j.Log4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.gnuhpc.bigdata.componet.OffsetStorage;
import org.gnuhpc.bigdata.constant.ConsumerState;
import org.gnuhpc.bigdata.model.*;
import org.gnuhpc.bigdata.service.IKafkaAdminService;
import org.gnuhpc.bigdata.utils.KafkaUtils;
import org.gnuhpc.bigdata.utils.ZookeeperUtils;
import org.gnuhpc.bigdata.validator.TopicExistConstraint;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;
import scala.Int;
import scala.Long;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * Created by gnuhpc on 2017/7/17.
 */

@Service
@Log4j
@Validated
public class KafkaAdminService implements IKafkaAdminService {

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

    @PostConstruct
    private void init() {
        this.zkUtils = zookeeperUtils.getZkUtils();
        this.zkClient = zookeeperUtils.getCuratorClient();
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(DateTime.class, (JsonDeserializer<DateTime>) (jsonElement, type, jsonDeserializationContext) -> new DateTime(jsonElement.getAsJsonPrimitive().getAsLong()));

        this.gson = builder.create();
        this.kafkaAdminClient = kafkaUtils.getKafkaAdminClient();
    }

    @Override
    public boolean createTopic(TopicDetail topic) {
        if (StringUtils.isEmpty(topic.getName())) {
            throw new InvalidTopicException("Empty topic name");
        }

        if (Topic.hasCollisionChars(topic.getName())) {
            throw new InvalidTopicException("Invalid topic name");
        }

        if (Strings.isNullOrEmpty(topic.getReassignStr()) && topic.getPartitions() <= 0) {
            throw new InvalidTopicException("Number of partitions must be larger than 0");
        }
        Topic.validate(topic.getName());


        if (Strings.isNullOrEmpty(topic.getReassignStr())) {
            AdminUtils.createTopic(zkUtils,
                    topic.getName(), topic.getPartitions(), topic.getFactor(),
                    topic.getProp(), RackAwareMode.Enforced$.MODULE$);
        } else {
            List<String> argsList = new ArrayList<>();
            argsList.add("--topic");
            argsList.add(topic.getName());
            argsList.add("--config");

            for (String key : topic.getProp().stringPropertyNames()) {
                argsList.add(key + "=" + topic.getProp().get(key));
            }

            argsList.add("--replica-assignment");
            argsList.add(topic.getReassignStr());

            TopicCommand.createTopic(zkUtils, new TopicCommand.TopicCommandOptions(argsList.stream().toArray(String[]::new)));
        }

        return AdminUtils.topicExists(zkUtils, topic.getName());
    }

    @Override
    public List<String> listTopics() {
        return JavaConverters.asJavaCollectionConverter(zkUtils.getAllTopics()).asJavaCollection().parallelStream().collect(toList());
    }


    @Override
    public List<TopicBrief> listTopicBrief() {
        KafkaConsumer consumer = createNewConsumer(DEFAULTCP);
        Map<String, List<PartitionInfo>> topicMap = consumer.listTopics();
        return topicMap.entrySet().parallelStream().map(e -> {
                    String topic = e.getKey();
                    long isrCount = e.getValue().parallelStream().flatMap(pi -> Arrays.stream(pi.replicas())).count();
                    long replicateCount = e.getValue().parallelStream().flatMap(pi -> Arrays.stream(pi.inSyncReplicas())).count();
                    return new TopicBrief(topic, e.getValue().size(), isrCount / replicateCount);
                }
        ).collect(toList());
    }

    @Override
    public boolean existTopic(String topicName) {
        return AdminUtils.topicExists(zkUtils, topicName);
    }

    @Override
    public List<BrokerInfo> listBrokers() {
        List<Broker> brokerList = JavaConverters.seqAsJavaListConverter(zkUtils.getAllBrokersInCluster()).asJava();
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

    @Override
    public TopicMeta describeTopic(@TopicExistConstraint String topicName) {
        KafkaConsumer consumer = createNewConsumer(DEFAULTCP);
        TopicMeta topicMeta = new TopicMeta(topicName);
        List<PartitionInfo> tmList = consumer.partitionsFor(topicName);
        topicMeta.setPartitionCount(tmList.size());
        topicMeta.setReplicationFactor(tmList.get(0).replicas().length);
        topicMeta.setTopicCustomConfigs(getTopicPropsFromZk(topicName));
        topicMeta.setTopicPartitionInfos(tmList.parallelStream().map(tm -> {
            TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo();
            topicPartitionInfo.setLeader(tm.leader().host());
            topicPartitionInfo.setIsr(Arrays.stream(tm.inSyncReplicas()).map(node -> node.host()).collect(toList()));
            topicPartitionInfo.setPartitionId(tm.partition());
            topicPartitionInfo.setReplicas(Arrays.stream(tm.replicas()).map(node -> node.host()).collect(toList()));
            topicPartitionInfo.setIn_sync();

            //Set offset
            TopicPartition tp = new TopicPartition(topicName, tm.partition());
            Map<TopicAndPartition, java.lang.Long> bOffsets = consumer.beginningOffsets(Arrays.asList(tp));
            Map<TopicAndPartition, java.lang.Long> eOffsets = consumer.endOffsets(Arrays.asList(tp));
            topicPartitionInfo.setStartOffset(bOffsets.get(tp));
            topicPartitionInfo.setEndOffset(eOffsets.get(tp));
            topicPartitionInfo.setMessageAvailable();

            return topicPartitionInfo;

        }).collect(toList()));

        return topicMeta;
    }

    @Override
    public boolean deleteTopic(@TopicExistConstraint String topic) {
        log.warn("Delete topic " + topic);
        AdminUtils.deleteTopic(zkUtils, topic);
        return existTopic(topic);
    }

    @Override
    public Properties createTopicConf(@TopicExistConstraint String topic, Properties prop) {
        Properties configs = getTopicPropsFromZk(topic);
        configs.putAll(prop);
        AdminUtils.changeTopicConfig(zkUtils, topic, configs);
        log.info("Create config for topic: " + topic + "Configs:" + configs);
        return getTopicPropsFromZk(topic);
    }

    @Override
    public Properties deleteTopicConf(@TopicExistConstraint String topic, List<String> deleteProps) {
        // compile the final set of configs
        Properties configs = getTopicPropsFromZk(topic);
        deleteProps.stream().forEach(config -> configs.remove(config));
        AdminUtils.changeTopicConfig(zkUtils, topic, configs);
        log.info("Delete config for topic: " + topic);
        return getTopicPropsFromZk(topic);
    }

    @Override
    public Properties updateTopicConf(@TopicExistConstraint String topic, Properties prop) {
        AdminUtils.changeTopicConfig(zkUtils, topic, prop);
        return getTopicPropsFromZk(topic);
    }

    @Override
    public Properties getTopicConf(@TopicExistConstraint String topic) {
        return getTopicPropsFromZk(topic);
    }

    @Override
    public Properties getTopicConfByKey(@TopicExistConstraint String topic, String key) {
        String value = String.valueOf(AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topic).get(key));
        Properties returnProps = new Properties();
        if (!value.equals("null")) {
            returnProps.setProperty(key, value);
            return returnProps;
        } else
            return null;
    }

    @Override
    public boolean deleteTopicConfByKey(@TopicExistConstraint String topic, String key) {
        Properties configs = getTopicPropsFromZk(topic);
        configs.remove(key);
        AdminUtils.changeTopicConfig(zkUtils, topic, configs);
        return getTopicPropsFromZk(topic)
                .get(key) == null ? true : false;
    }

    @Override
    public Properties updateTopicConfByKey(@TopicExistConstraint String topic, String key, String value) {
        Properties props = new Properties();
        props.setProperty(key, value);
        String validValue = String.valueOf(updateTopicConf(topic, props).get(key));
        if (!validValue.equals("null") && validValue.equals(value)) {
            return props;
        } else {
            return null;
        }
    }

    @Override
    public Properties createTopicConfByKey(@TopicExistConstraint String topic, String key, String value) {
        Properties props = new Properties();
        props.setProperty(key, value);
        String validValue = String.valueOf(createTopicConf(topic, props).get(key));
        if (!validValue.equals("null") && validValue.equals(value)) {
            return props;
        } else {
            return null;
        }

    }

    @Override
    public TopicMeta addPartition(@TopicExistConstraint String topic, AddPartition addPartition) {
        List<MetadataResponse.PartitionMetadata> paritionMataData = AdminUtils.fetchTopicMetadataFromZk(topic, zkUtils).partitionMetadata();
        int numPartitions = paritionMataData.size();
        int numReplica = paritionMataData.get(0).replicas().size();
        List paritionIdList = paritionMataData.stream().map(p -> String.valueOf(p.partition())).collect(toList());
        String assignMentStr = addPartition.getReplicaAssignment();
        String toBeSetreplicaAssignmentStr = "";

        if (assignMentStr != null && !assignMentStr.equals("")) {
            //Check outofindex ids in replicaassignment string
            String[] ids = addPartition.getReplicaAssignment().split(",|:");
            if (Arrays.stream(ids).filter(id -> !paritionIdList.contains(id)).count() != 0) {
                throw new InvalidTopicException("Topic " + topic + ": manual reassignment str has wrong id!");
            }

            //Check if any ids duplicated in one partition in replicaassignment
            String[] assignPartitions = addPartition.getReplicaAssignment().split(",");
            if (Arrays.stream(assignPartitions).filter(p ->
                    Arrays.stream(p.split(":")).collect(Collectors.toSet()).size()
                            != p.split(":").length).count()
                    != 0) {
                throw new InvalidTopicException("Topic " + topic + ": manual reassignment str has duplicated id in one partition!");
            }

            String replicaStr = Strings.repeat("0:", numReplica).replaceFirst(".$", ",");
            toBeSetreplicaAssignmentStr = Strings.repeat(replicaStr, numPartitions) + addPartition.getReplicaAssignment();
        } else {
            toBeSetreplicaAssignmentStr = "";
        }

        AdminUtils.addPartitions(zkUtils, topic, addPartition.getNumPartitionsAdded() + numPartitions,
                toBeSetreplicaAssignmentStr, true,
                RackAwareMode.Enforced$.MODULE$);

        return describeTopic(topic);
    }

    @Override
    public List<String> generateReassignPartition(ReassignWrapper reassignWrapper) {
        Seq brokerSeq = JavaConverters.asScalaBufferConverter(reassignWrapper.getBrokers()).asScala().toSeq();
        //<Proposed partition reassignment，Current partition replica assignment>
        Tuple2 resultTuple2 = ReassignPartitionsCommand.generateAssignment(zkUtils, brokerSeq, reassignWrapper.generateReassignJsonString(), false);
        List<String> result = new ArrayList<>();
        result.add(zkUtils.formatAsReassignmentJson((scala.collection.Map<TopicAndPartition, Seq<Object>>) resultTuple2._2()));
        result.add(zkUtils.formatAsReassignmentJson((scala.collection.Map<TopicAndPartition, Seq<Object>>) resultTuple2._1()));

        return result;
    }

    @Override
    public Map<TopicAndPartition, Integer> executeReassignPartition(String reassignStr, long throttle) {
        ReassignPartitionsCommand.executeAssignment(
                zkUtils,
                reassignStr,
                new ReassignPartitionsCommand.Throttle(throttle, new AbstractFunction0<BoxedUnit>() {
                    @Override
                    public BoxedUnit apply() {
                        return null;
                    }
                }));
        return checkReassignStatus(reassignStr);
    }

    @Override
    public Map<TopicAndPartition, Integer> checkReassignStatus(String reassignStr) {
        scala.collection.Map<TopicAndPartition, Seq<Object>> partitionsToBeReassigned = zkUtils.parsePartitionReassignmentData(reassignStr);

        java.util.Map<TopicAndPartition, ReassignedPartitionsContext> partitionsBeingReassignedMap =
                JavaConverters.mapAsJavaMapConverter(zkUtils.getPartitionsBeingReassigned()).asJava();
        java.util.Map<TopicAndPartition, Seq<Object>> partitionsBeingReassigned = partitionsBeingReassignedMap.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().newReplicas()
        ));

        java.util.Map<TopicAndPartition, ReassignmentStatus> reassignedPartitionsStatus = JavaConverters.mapAsJavaMapConverter(partitionsToBeReassigned).asJava().entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                p -> ReassignPartitionsCommand.checkIfPartitionReassignmentSucceeded(
                        zkUtils,
                        p.getKey(),
                        partitionsToBeReassigned,
                        JavaConverters.mapAsScalaMapConverter(partitionsBeingReassigned).asScala())
        ));

        return reassignedPartitionsStatus.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                r -> r.getValue().status()
        ));
    }

    @Override
    public List<String> listAllOldConsumerGroups() {
        return JavaConverters.seqAsJavaListConverter(zkUtils.getConsumerGroups()).asJava();
    }

    @Override
    public List<String> listOldConsumerGroupsByTopic(String topic) throws Exception {

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

    @Override
    public List<String> listAllNewConsumerGroups() {
        return JavaConverters.seqAsJavaListConverter(kafkaAdminClient.listAllConsumerGroupsFlattened()).asJava().stream()
                .map(GroupOverview::groupId).collect(toList());
    }

    @Override
    public List<String> listNewConsumerGroupsByTopic(String topic) {

        AdminClient.ConsumerSummary cs;
        String t;
        List<String> consumersList = listAllNewConsumerGroups();

        Map<String, String> consumerTopicMap = new HashMap<>();
        for (String c : consumersList) {
            AdminClient.ConsumerGroupSummary consumerGroupSummary = kafkaAdminClient.describeConsumerGroup(c, 0);
            List<AdminClient.ConsumerSummary> consumerSummaryList = new ArrayList<>(JavaConverters.asJavaCollectionConverter(consumerGroupSummary.consumers().get())
                    .asJavaCollection());
            if (consumerSummaryList.size() > 0) {
                cs = consumerSummaryList.get(0);
                t = JavaConverters.asJavaCollectionConverter(cs.assignment()).asJavaCollection().stream().collect(toList()).get(0).topic();
                consumerTopicMap.put(c, t);
            }
        }
        List<java.util.Map.Entry<String, String>> consumerEntryList = consumerTopicMap.entrySet().stream().collect(
                Collectors.groupingBy(
                        Map.Entry::getValue,
                        Collectors.toList()
                )
        ).getOrDefault(topic, new ArrayList<>());

        return consumerEntryList.stream().map(Map.Entry::getKey).collect(Collectors.toList());
    }

    @Override
    public List<ConsumerGroupDesc> describeOldCG(String consumerGroup, String topic) {
        List<ConsumerGroupDesc> result;
        if (!zkUtils.getConsumerGroups().contains(consumerGroup)) {
            result = new ArrayList<>();
            return result;
        }

        ConsumerGroupCommand.ZkConsumerGroupService zkConsumerGroupService =
                new ConsumerGroupCommand.ZkConsumerGroupService(
                        new ConsumerGroupCommand.ConsumerGroupCommandOptions(
                                new String[]{"--describe", "--group", consumerGroup, "--zookeeper", zookeeperUtils.getZookeeperConfig().getUris()}
                        )
                );

        List<ConsumerGroupCommand.PartitionAssignmentState> partitionAssignmentStateList = JavaConverters.seqAsJavaListConverter(zkConsumerGroupService.describeGroup()._2().get()).asJava();
        log.info("Got Partition Assignment");

        partitionAssignmentStateList.forEach(ps->log.info(ps));

        result = setCGD(partitionAssignmentStateList);

        if (!Strings.isNullOrEmpty(topic)) {
            result = result.stream().filter(cgd -> cgd.getTopic().equals(topic)).collect(toList());
        }

        return result;
    }

    @Override
    public List<ConsumerGroupDesc> describeNewCG(String consumerGroup,String topic) {
        List<ConsumerGroupDesc> result;
        AdminClient adminClient = AdminClient.create(kafkaUtils.getProp());

        List<GroupOverview> cgList = JavaConverters.asJavaCollectionConverter(adminClient.listAllConsumerGroupsFlattened()).asJavaCollection().stream().collect(toList());
        if (cgList.stream().filter(cg -> cg.groupId().equals(consumerGroup)).count() == 0) {
            result = new ArrayList<>();
            return result;
        }


        ConsumerGroupCommand.KafkaConsumerGroupService kafkaConsumerGroupService =
                new ConsumerGroupCommand.KafkaConsumerGroupService(
                        new ConsumerGroupCommand.ConsumerGroupCommandOptions(
                                new String[]{"--new-consumer","--describe", "--group", consumerGroup, "--bootstrap-server", kafkaUtils.getKafkaConfig().getBrokers()}
                        )
                );

        List<ConsumerGroupCommand.PartitionAssignmentState> partitionAssignmentStateList = JavaConverters.seqAsJavaListConverter(kafkaConsumerGroupService.describeGroup()._2().get()).asJava();

        result = setCGD(partitionAssignmentStateList);
        if (!Strings.isNullOrEmpty(topic)) {
            result = result.stream().filter(cgd -> cgd.getTopic().equals(topic)).collect(toList());
        }

        return result;
    }

    @Override
    public String getMessage(@TopicExistConstraint String topic, int partition, long offset, String decoder, String avroSchema) {
        KafkaConsumer consumer = createNewConsumer(DEFAULTCP);
        TopicPartition tp = new TopicPartition(topic, partition);
        Map<TopicAndPartition, java.lang.Long> bOffsets = consumer.beginningOffsets(Arrays.asList(tp));
        Map<TopicAndPartition, java.lang.Long> eOffsets = consumer.endOffsets(Arrays.asList(tp));
        if (offset < bOffsets.get(tp) || offset >= eOffsets.get(tp)) {
            log.error(offset + " error");
            throw new ApiException("offsets must be between " + String.valueOf(bOffsets.get(tp)
                    + " and " + String.valueOf(eOffsets.get(tp)))
            );
        }
        consumer.assign(Arrays.asList(tp));
        consumer.seek(tp, offset);

        String last = null;

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
        return last;
    }

    @Override
    public void resetOffset(@TopicExistConstraint String topic, int partition, String consumerGroup, String offset) {
        AdminClient.ConsumerGroupSummary consumerGroupSummary = kafkaAdminClient.describeConsumerGroup(consumerGroup, channelSocketTimeoutMs);
        if (consumerGroupSummary.state().equals("Empty") || consumerGroupSummary.state().equals("Dead")) {
            KafkaConsumer consumer = createNewConsumer(consumerGroup);
            TopicPartition tp = new TopicPartition(topic, partition);
            long o;
            if (offset.equals("earliest")) {
                Map<TopicAndPartition, java.lang.Long> bOffsets = consumer.beginningOffsets(Arrays.asList(tp));
                o = bOffsets.get(tp);
            } else if (offset.equals("lastest")) {
                Map<TopicAndPartition, java.lang.Long> eOffsets = consumer.endOffsets(Arrays.asList(tp));
                o = eOffsets.get(tp);
            } else {
                o = java.lang.Long.parseLong(offset);
            }

            consumer.assign(Arrays.asList(tp));
            consumer.seek(tp, o);
            consumer.commitSync();
        } else {
            throw new ApiException("Assignments can only be reset if the group " + consumerGroup + " is inactive, " +
                    "but the current state is " + consumerGroupSummary.state() + ".");
        }
    }

    @Override
    public Map<String, Map<Integer, java.lang.Long>> getLastCommitTime(String consumerGroup, @TopicExistConstraint String topic) {
        Map<String, Map<Integer, java.lang.Long>> result = new ConcurrentHashMap<>();

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


        //Get New consumer commit time, from offset storage instance
        if (storage.get(consumerGroup) != null) {
            Map<GroupTopicPartition, OffsetAndMetadata> storageResult = storage.get(consumerGroup);
            result.put("new", (storageResult.entrySet().parallelStream().filter(s -> s.getKey().topicPartition().topic().equals(topic))
                            .collect(
                                    Collectors.toMap(
                                            s -> s.getKey().topicPartition().partition(),
                                            s -> s.getValue().commitTimestamp()
                                    )
                            )
                    )
            );
        }

        return result;
    }

    @Override
    public boolean deleteConsumerGroup(String consumerGroup) {
        return AdminUtils.deleteConsumerGroupInZK(zkUtils,consumerGroup);
    }

    private List<ConsumerGroupDesc> setCGD(List<ConsumerGroupCommand.PartitionAssignmentState> partitionAssignmentStateList) {
        scala.Option<String> NONE = scala.Option.apply(null);
        scala.Option<Object> objectNONE = scala.Option.apply(null);
        List<ConsumerGroupDesc> result = new ArrayList<>();
        for (ConsumerGroupCommand.PartitionAssignmentState paState : partitionAssignmentStateList) {
            ConsumerGroupDesc cgd = new ConsumerGroupDesc();
            String topic = paState.topic().get();
            cgd.setTopic(topic);
            cgd.setPartitionId(Int.unbox(paState.partition().get()));
            cgd.setLogEndOffset(Long.unbox(paState.logEndOffset().get()));
            if (paState.offset() != objectNONE) {
                cgd.setCurrentOffset(Long.unbox(paState.offset().get()));
            } else {
                cgd.setCurrentOffset(-1);
            }
            if (paState.lag() != objectNONE) {
                cgd.setLag(Long.unbox(paState.lag().get()));
            } else {
                cgd.setLag(-1);
            }


            if (paState.consumerId().get() != null && !paState.consumerId().get().equals("-")) {
                cgd.setState(ConsumerState.RUNNING);
                cgd.setConsumerId(paState.consumerId().get());
            } else {
                cgd.setConsumerId("-");
                cgd.setState(ConsumerState.PENDING);
            }

            if (paState.host() != NONE) {
                cgd.setHost(paState.host().get());
            }


            result.add(cgd);
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

}
