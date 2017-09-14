package org.gnuhpc.bigdata.service;

import kafka.common.TopicAndPartition;
import org.gnuhpc.bigdata.model.*;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by gnuhpc on 2017/7/17.
 */
public interface IKafkaAdminService {

    TopicMeta createTopic(TopicDetail topic, String reassignStr);
    List<String> listTopics();
    List<TopicBrief> listTopicBrief();
    boolean existTopic(String topic);
    List<BrokerInfo> listBrokers();

    TopicMeta describeTopic(String topic);

    GeneralResponse deleteTopic(String topic);

    Properties createTopicConf(String topic, Properties prop);

    Properties deleteTopicConf(String topic, List<String> propKeys);

    Properties updateTopicConf(String topic, Properties prop);

    Properties getTopicConf(String topic);

    Properties getTopicConfByKey(String topic, String key);

    boolean deleteTopicConfByKey(String topic, String key);

    Properties updateTopicConfByKey(String topic, String key, String value);

    Properties createTopicConfByKey(String topic, String key, String value);

    TopicMeta addPartition(String topic, AddPartition addPartition);
    List<String> generateReassignPartition(ReassignWrapper reassignWrapper);

    Map<TopicAndPartition, Integer> executeReassignPartition(String reassignStr);
    Map<TopicAndPartition, Integer> checkReassignStatus(String reassignStr);

    String getMessage(String topic, int partition, long offset, String decoder, String avroSchema);

    GeneralResponse resetOffset(String topic, int partition, String consumerGroup, String type, String offset);

    Map<String, Map<Integer, Long>> getLastCommitTime(String consumerGroup, String topic);

    GeneralResponse deleteConsumerGroup(String consumerGroup);

    Map<String, List<String>> listAllConsumerGroups();

    Map<String, List<String>> listConsumerGroupsByTopic(String topic);

    Map<String, List<ConsumerGroupDesc>> describeNewCG(String consumerGroup, String topic);

    Map<String, List<ConsumerGroupDesc>> describeOldCG(String consumerGroup, String topic);
}
