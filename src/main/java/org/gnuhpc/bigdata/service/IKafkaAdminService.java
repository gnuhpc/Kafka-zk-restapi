package org.gnuhpc.bigdata.service;

import kafka.common.TopicAndPartition;
import org.gnuhpc.bigdata.model.*;
import org.gnuhpc.bigdata.validator.TopicExistConstraint;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by gnuhpc on 2017/7/17.
 */
public interface IKafkaAdminService {

    boolean createTopic(TopicDetail topic);
    List<String> listTopics();
    List<TopicBrief> listTopicBrief();
    boolean existTopic(String topic);
    List<BrokerInfo> listBrokers();
    TopicMeta describeTopic(@TopicExistConstraint String topic);
    boolean deleteTopic(@TopicExistConstraint String topic);
    Properties createTopicConf(@TopicExistConstraint String topic, Properties prop);
    Properties deleteTopicConf(@TopicExistConstraint String topic, List<String> propKeys);
    Properties updateTopicConf(@TopicExistConstraint String topic, Properties prop);
    Properties getTopicConf(@TopicExistConstraint String topic);
    Properties getTopicConfByKey(@TopicExistConstraint String topic, String key);
    boolean deleteTopicConfByKey(@TopicExistConstraint String topic, String key);
    Properties updateTopicConfByKey(@TopicExistConstraint String topic, String key,String value);
    Properties createTopicConfByKey(@TopicExistConstraint String topic, String key, String value);
    TopicMeta addPartition(@TopicExistConstraint String topic, AddPartition addPartition);
    List<String> generateReassignPartition(ReassignWrapper reassignWrapper);
    Map<TopicAndPartition, Integer> executeReassignPartition(String reassignStr,long throttle);
    Map<TopicAndPartition, Integer> checkReassignStatus(String reassignStr);
    List<String> listAllOldConsumerGroups();
    List<String> listOldConsumerGroupsByTopic(String topic) throws Exception;
    List<String> listAllNewConsumerGroups();
    List<String> listNewConsumerGroupsByTopic(String topic);
    List<ConsumerGroupDesc> describeOldCG(String consumerGroup,String topic);
    List<ConsumerGroupDesc> describeNewCG(String consumerGroup,String topic);

    String getMessage(@TopicExistConstraint String topic, int partition, long offset, String decoder, String avroSchema);
    void resetOffset(@TopicExistConstraint String topic, int partition, String consumerGroup, String offset);
    Map<String, Map<Integer, Long>> getLastCommitTime(String consumerGroup, @TopicExistConstraint String topic);
    boolean deleteConsumerGroup(String consumerGroup);
}
