package org.gnuhpc.bigdata.controller;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import joptsimple.internal.Strings;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.requests.DescribeLogDirsResponse.LogDirInfo;
import org.gnuhpc.bigdata.constant.ConsumerType;
import org.gnuhpc.bigdata.model.AddPartition;
import org.gnuhpc.bigdata.model.BrokerInfo;
import org.gnuhpc.bigdata.model.ConsumerGroupDesc;
import org.gnuhpc.bigdata.model.ConsumerGroupMeta;
import org.gnuhpc.bigdata.model.CustomConfigEntry;
import org.gnuhpc.bigdata.model.GeneralResponse;
import org.gnuhpc.bigdata.model.HealthCheckResult;
import org.gnuhpc.bigdata.model.ReassignWrapper;
import org.gnuhpc.bigdata.model.TopicBrief;
import org.gnuhpc.bigdata.model.TopicDetail;
import org.gnuhpc.bigdata.model.TopicMeta;
import org.gnuhpc.bigdata.service.KafkaAdminService;
import org.gnuhpc.bigdata.validator.ConsumerGroupExistConstraint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/** Created by gnuhpc on 2017/7/16. */
@Log4j
@RequestMapping("/kafka")
@RestController
public class KafkaController {

  @Autowired private KafkaAdminService kafkaAdminService;

//  @Autowired private KafkaProducerService kafkaProducerService;

  @GetMapping(value = "/cluster")
  @ApiOperation(value = "Describe cluster, nodes, controller info.")
  public Map<String, Object> describeCluster() {
    return kafkaAdminService.describeCluster();
  }

  @GetMapping(value = "/brokers")
  @ApiOperation(value = "List brokers in this cluster")
  public List<BrokerInfo> listBrokers() {
    return kafkaAdminService.listBrokers();
  }

  @GetMapping(value = "/controller")
  @ApiOperation(value = "Get controller id in this cluster")
  public int getControllerId() {
    return kafkaAdminService.getControllerId();
  }

  @GetMapping(value = "/brokers/logdirs")
  @ApiOperation(value = "List log dirs by broker list")
  public Map<Integer, List<String>> listLogDirs(
      @RequestParam(required = false) List<Integer> brokerList) {
    return kafkaAdminService.listLogDirsByBroker(brokerList);
  }

  @GetMapping(value = "/brokers/logdirs/detail")
  @ApiOperation(value = "Describe log dirs by broker list and topic list")
  public Map<Integer, Map<String, LogDirInfo>> describeLogDirs(
      @RequestParam(required = false) List<Integer> brokerList,
      @RequestParam(required = false) List<String> topicList) {
    return kafkaAdminService.describeLogDirsByBrokerAndTopic(brokerList, topicList);
  }

  @GetMapping(value = "/brokers/replicalogdirs")
  @ApiOperation(value = "Describe replica log dirs.")
  public Map<TopicPartitionReplica, ReplicaLogDirInfo> describeReplicaLogDirs(
      @RequestParam List<TopicPartitionReplica> replicas) {
    return kafkaAdminService.describeReplicaLogDirs(replicas);
  }

  @GetMapping(value = "/brokers/{brokerId}/conf")
  @ApiOperation(value = "Get broker configs, including dynamic configs")
  public Collection<CustomConfigEntry> getBrokerConfig(@PathVariable int brokerId) {
    return kafkaAdminService.getBrokerConf(brokerId);
  }

  @GetMapping(value = "/brokers/{brokerId}/dynconf")
  @ApiOperation(value = "Get broker dynamic configs")
  public Properties getBrokerDynConfig(@PathVariable int brokerId) {
    return kafkaAdminService.getConfigInZk(Type.BROKER, String.valueOf(brokerId));
  }

  @PutMapping(value = "/brokers/{brokerId}/dynconf")
  @ApiOperation(value = "Update broker configs")
  public Properties updateBrokerDynConfig(
      @PathVariable int brokerId, @RequestBody Properties props) {
    return kafkaAdminService.updateBrokerDynConf(brokerId, props);
  }

  @DeleteMapping(value = "/brokers/{brokerId}/dynconf")
  @ApiOperation(value = "Remove broker dynamic configs")
  public void removeBrokerDynConfig(
      @PathVariable int brokerId, @RequestParam List<String> configKeysToBeRemoved) {
    kafkaAdminService.removeConfigInZk(
        Type.BROKER, String.valueOf(brokerId), configKeysToBeRemoved);
  }

  @GetMapping("/topics")
  @ApiOperation(value = "List topics")
  public List<String> listTopics() {
    return kafkaAdminService.listTopics();
  }

  @GetMapping("/topicsbrief")
  @ApiOperation(value = "List topics Brief")
  public List<TopicBrief> listTopicBrief() {
    return kafkaAdminService.listTopicBrief();
  }

  @PostMapping(value = "/topics/create", consumes = "application/json")
  @ResponseStatus(HttpStatus.CREATED)
  @ApiOperation(value = "Create a topic")
  @ApiParam(value = "if reassignStr set, partitions and repli-factor will be ignored.")
  public TopicMeta createTopic(
      @RequestBody TopicDetail topic, @RequestParam(required = false) String reassignStr) {
    return kafkaAdminService.createTopic(topic, reassignStr);
  }

  @ApiOperation(value = "Tell if a topic exists")
  @GetMapping(value = "/topics/{topic}/exist")
  public boolean existTopic(@PathVariable String topic) {
    return kafkaAdminService.existTopic(topic);
  }

//  @PostMapping(value = "/topics/{topic}/write", consumes = "text/plain")
//  @ResponseStatus(HttpStatus.CREATED)
//  @ApiOperation(value = "Write a message to the topic, for testing purpose")
//  public GeneralResponse writeMessage(@PathVariable String topic, @RequestBody String message) {
//    kafkaProducerService.send(topic, message);
//    return GeneralResponse.builder()
//        .state(GeneralResponseState.success)
//        .msg(message + " has been sent")
//        .build();
//  }

  @GetMapping(value = "/consumer/{topic}/{partition}/{offset}")
  @ApiOperation(
      value =
          "Get the message from the offset of the partition in the topic"
              + ", decoder is not supported yet")
  public String getMessage(
      @PathVariable String topic,
      @PathVariable int partition,
      @PathVariable long offset,
      @RequestParam(required = false) String decoder) {
    return kafkaAdminService.getRecordByOffset(topic, partition, offset, decoder, "").getValue();
  }

  @GetMapping(value = "/topics/{topic}")
  @ApiOperation(value = "Describe a topic by fetching the metadata and config")
  public TopicMeta describeTopic(@PathVariable String topic) {
    return kafkaAdminService.describeTopic(topic);
  }

  @DeleteMapping(value = "/topics")
  @ApiOperation(value = "Delete a topic list (you should enable topic deletion")
  public Map<String, GeneralResponse> deleteTopicList(@RequestParam List<String> topicList) {
    return kafkaAdminService.deleteTopicList(topicList);
  }

  @PutMapping(value = "/topics/{topic}/conf")
  @ApiOperation(value = "Update topic configs")
  public Collection<CustomConfigEntry> updateTopicConfig(
      @PathVariable String topic, @RequestBody Properties props) {
    return kafkaAdminService.updateTopicConf(topic, props);
  }

  @GetMapping(value = "/topics/{topic}/conf")
  @ApiOperation(value = "Get topic configs")
  public Collection<CustomConfigEntry> getTopicConfig(@PathVariable String topic) {
    return kafkaAdminService.getTopicConf(topic);
  }

  @GetMapping(value = "/topics/{topic}/dynconf")
  @ApiOperation(value = "Get topic dyn configs")
  public Properties getTopicDynConfig(@PathVariable String topic) {
    return kafkaAdminService.getConfigInZk(Type.TOPIC, topic);
  }

  @GetMapping(value = "/topics/{topic}/conf/{key}")
  @ApiOperation(value = "Get topic config by key")
  public Properties getTopicConfigByKey(@PathVariable String topic, @PathVariable String key) {
    return kafkaAdminService.getTopicConfByKey(topic, key);
  }

  @PutMapping(value = "/topics/{topic}/conf/{key}={value}")
  @ApiOperation(value = "Update a topic config by key")
  public Collection<CustomConfigEntry> updateTopicConfigByKey(
      @PathVariable String topic, @PathVariable String key, @PathVariable String value) {
    return kafkaAdminService.updateTopicConfByKey(topic, key, value);
  }

  @PostMapping(value = "/partitions/add")
  @ApiOperation(value = "Add partitions to the topics")
  public Map<String, GeneralResponse> addPartition(@RequestBody List<AddPartition> addPartitions) {
    return kafkaAdminService.addPartitions(addPartitions);
  }

  @PostMapping(value = "/partitions/reassign/generate")
  @ApiOperation(value = "Generate plan for the partition reassignment")
  public List<String> generateReassignPartitions(@RequestBody ReassignWrapper reassignWrapper) {
    return kafkaAdminService.generateReassignPartition(reassignWrapper);
  }

  @PutMapping(value = "/partitions/reassign/execute")
  @ApiOperation(value = "Execute the partition reassignment")
  public Map<String, Object> executeReassignPartitions(
      @RequestBody String reassignStr,
      long interBrokerThrottle,
      long replicaAlterLogDirsThrottle,
      long timeoutMs) {
    return kafkaAdminService.executeReassignPartition(
        reassignStr, interBrokerThrottle, replicaAlterLogDirsThrottle, timeoutMs);
  }

  @PutMapping(value = "/partitions/reassign/check")
  @ApiOperation(value = "Check the partition reassignment process")
  @ApiResponses(
      value = {
        @ApiResponse(code = 1, message = "Reassignment Completed"),
        @ApiResponse(code = 0, message = "Reassignment In Progress"),
        @ApiResponse(code = -1, message = "Reassignment Failed")
      })
  public Map<String, Object> checkReassignPartitions(@RequestBody String reassignStr) {
    return kafkaAdminService.checkReassignStatusByStr(reassignStr);
  }

  @GetMapping(value = "/consumergroups")
  @ApiOperation(value = "List all consumer groups from zk and kafka")
  public Map<String, Set<String>> listAllConsumerGroups(
      @RequestParam(required = false) ConsumerType type,
      @RequestParam(required = false) String topic) {
    if (topic != null) {
      return kafkaAdminService.listConsumerGroupsByTopic(topic, type);
    } else {
      return kafkaAdminService.listAllConsumerGroups(type);
    }
  }

  @GetMapping(value = "/consumergroups/{consumerGroup}/{type}/topic")
  @ApiOperation(value = "Get the topics involved of the specify consumer group")
  public Set<String> listTopicByConsumerGroup(
      @PathVariable String consumerGroup, @PathVariable ConsumerType type) {
    return kafkaAdminService.listTopicsByConsumerGroup(consumerGroup, type);
  }

  @GetMapping(value = "/consumergroups/{consumerGroup}/meta")
  @ApiOperation(
      value =
          "Get the meta data of the specify new consumer group, including state, coordinator,"
              + " assignmentStrategy, members")
  public ConsumerGroupMeta getConsumerGroupMeta(@PathVariable String consumerGroup) {
    if (kafkaAdminService.isNewConsumerGroup(consumerGroup)) {
      return kafkaAdminService.getConsumerGroupMeta(consumerGroup);
    }

    throw new ApiException("New consumer group:" + consumerGroup + " non-exist.");
  }

  @GetMapping(value = "/consumergroups/{consumerGroup}/{type}/topic/{topic}")
  @ApiOperation(value = "Describe consumer groups by topic, showing lag and offset")
  public List<ConsumerGroupDesc> describeConsumerGroupByTopic(
      @ConsumerGroupExistConstraint @PathVariable String consumerGroup,
      @PathVariable ConsumerType type,
      @PathVariable String topic) {
    if (!Strings.isNullOrEmpty(topic)) {
      existTopic(topic);
    } else {
      throw new ApiException("Topic must be set!");
    }
    if (type != null && type == ConsumerType.NEW) {
      return kafkaAdminService.describeNewConsumerGroupByTopic(consumerGroup, topic);
    }

    if (type != null && type == ConsumerType.OLD) {
      return kafkaAdminService.describeOldConsumerGroupByTopic(consumerGroup, topic);
    }

    throw new ApiException("Unknown type specified!");
  }

  @GetMapping(value = "/consumergroups/{consumerGroup}/{type}")
  @ApiOperation(
      value =
          "Describe consumer group, showing lag and offset, may be slow if multi"
              + " topics are listened")
  public Map<String, List<ConsumerGroupDesc>> describeConsumerGroup(
      @ConsumerGroupExistConstraint @PathVariable String consumerGroup,
      @PathVariable ConsumerType type) {
    return kafkaAdminService.describeConsumerGroup(consumerGroup, type);
  }

  @PutMapping(value = "/consumergroup/{consumergroup}/{type}/topic/{topic}/{partition}/{offset}")
  @ApiOperation(
      value =
          "Reset consumer group offset, earliest/latest can be used. Support reset by time for "
              + "new consumer group, pass a parameter that satisfies yyyy-MM-dd HH:mm:ss "
              + "to offset.")
  public GeneralResponse resetOffset(
      @PathVariable String topic,
      @PathVariable int partition,
      @PathVariable String consumergroup,
      @PathVariable
          @ApiParam(
              value =
                  "[earliest/latest/{long}/yyyy-MM-dd HH:mm:ss] can be supported. "
                      + "The date type is only valid for new consumer group.")
          String offset,
      @PathVariable ConsumerType type) {
    return kafkaAdminService.resetOffset(topic, partition, consumergroup, type, offset);
  }

  @GetMapping(value = "/consumergroup/{consumergroup}/{type}/topic/{topic}/lastcommittime")
  public Map<String, Map<Integer, Long>> getLastCommitTimestamp(
      @PathVariable String consumergroup,
      @PathVariable String topic,
      @PathVariable ConsumerType type) {
    return kafkaAdminService.getLastCommitTime(consumergroup, topic, type);
  }

  @DeleteMapping(value = "/consumergroup/{consumergroup}/{type}")
  @ApiOperation(value = "Delete Consumer Group")
  public GeneralResponse deleteOldConsumerGroup(
      @PathVariable String consumergroup, @PathVariable ConsumerType type) {
    return kafkaAdminService.deleteConsumerGroup(consumergroup, type);
  }

  @GetMapping(value = "/health")
  @ApiOperation(value = "Check the cluster health.")
  public HealthCheckResult healthCheck() {
    return kafkaAdminService.healthCheck();
  }

  //TODO add kafkaAdminClient.deleterecords api
}
