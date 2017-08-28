package org.gnuhpc.bigdata.controller;

import io.swagger.annotations.*;
import joptsimple.internal.Strings;
import kafka.common.TopicAndPartition;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.gnuhpc.bigdata.model.*;
import org.gnuhpc.bigdata.service.IKafkaAdminService;
import org.gnuhpc.bigdata.service.KafkaProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import springfox.documentation.annotations.ApiIgnore;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by gnuhpc on 2017/7/16.
 */
@Log4j
@RequestMapping("/kafka")
@RestController
public class KafkaController {
    @Autowired
    private IKafkaAdminService kafkaAdminService;

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @GetMapping("/topics")
    @ApiOperation(value = "List topics")
    public List<String> listTopics(){
        return kafkaAdminService.listTopics();
    }

    @GetMapping("/topicsbrief")
    @ApiOperation(value = "List topics Brief")
    public List<TopicBrief> listTopicBrief(){
        return kafkaAdminService.listTopicBrief();
    }

    @PostMapping(value = "/topics/create", consumes = "application/json")
    @ResponseStatus(HttpStatus.CREATED)
    @ApiOperation(value = "Create a topic")
    @ApiParam(value = "if reassignStr set, partitions and repli-factor will be ignored.")
    public boolean createTopic(@RequestBody TopicDetail topic){
        return kafkaAdminService.createTopic(topic);
    }

    @ApiOperation(value = "Tell if a topic exists")
    @GetMapping(value = "/topics/{topic}/exist")
    public boolean existTopic(@RequestParam String topic){
        return kafkaAdminService.existTopic(topic);
    }

    @PostMapping(value = "/topics/{topic}/write", consumes = "text/plain")
    @ResponseStatus(HttpStatus.CREATED)
    @ApiOperation(value = "Write a message to the topic, for testing purpose")
    public void writeMessage(@PathVariable String topic, @RequestBody String message) {
        kafkaProducerService.send(topic, message);
    }

    @GetMapping(value = "/topics/{topic}")
    @ApiOperation(value = "Describe a topic by fetching the metadata and config")
    public TopicMeta describeTopic(@PathVariable String topic){
        return kafkaAdminService.describeTopic(topic);
    }

    @GetMapping(value = "/brokers")
    @ApiOperation(value = "List brokers in this cluster")
    public List<BrokerInfo> listBrokers(){
        return kafkaAdminService.listBrokers();
    }

    @DeleteMapping(value = "/topics/{topic}")
    @ApiOperation(value = "Delete a topic (you should enable topic deletion")
    public boolean deleteTopic(@PathVariable String topic){
        return kafkaAdminService.deleteTopic(topic);
    }

    @PostMapping(value = "/topics/{topic}/conf")
    @ApiOperation(value = "Create topic configs")
    public Properties createTopicConfig(@PathVariable String topic, @RequestBody Properties prop){
        return kafkaAdminService.createTopicConf(topic,prop);
    }

    @PutMapping(value = "/topics/{topic}/conf")
    @ApiOperation(value = "Update topic configs")
    public Properties updateTopicConfig(@PathVariable String topic, @RequestBody Properties prop){
        return kafkaAdminService.updateTopicConf(topic,prop);
    }

    @DeleteMapping(value = "/topics/{topic}/conf")
    @ApiOperation(value = "Delete topic configs")
    public Properties deleteTopicConfig(@PathVariable String topic, @RequestBody List<String> delProps){
        return kafkaAdminService.deleteTopicConf(topic,delProps);
    }

    @GetMapping(value = "/topics/{topic}/conf")
    @ApiOperation(value = "Get topic configs")
    public Properties getTopicConfig(@PathVariable String topic){
        return kafkaAdminService.getTopicConf(topic);
    }

    @GetMapping(value = "/topics/{topic}/conf/{key}")
    @ApiOperation(value = "Get topic config by key")
    public Properties getTopicConfigByKey(@PathVariable String topic, @PathVariable String key){
        return kafkaAdminService.getTopicConfByKey(topic,key);
    }

    @PostMapping(value = "/topics/{topic}/conf/{key}={value}")
    @ApiOperation(value = "Create a topic config by key")
    public Properties createTopicConfigByKey(@PathVariable String topic, @PathVariable String key,
                                          @PathVariable String value){
        return kafkaAdminService.createTopicConfByKey(topic, key, value);
    }

    @PutMapping(value = "/topics/{topic}/conf/{key}={value}")
    @ApiOperation(value = "Update a topic config by key")
    public Properties updateTopicConfigByKey(@PathVariable String topic, @PathVariable String key,
                                          @PathVariable String value){
        return kafkaAdminService.updateTopicConfByKey(topic, key, value);
    }

    @DeleteMapping (value = "/topics/{topic}/conf/{key}")
    @ApiOperation(value = "Delete a topic config by key")
    public boolean deleteTopicConfigByKey(@PathVariable String topic, @PathVariable String key){
        return kafkaAdminService.deleteTopicConfByKey(topic, key);
    }

    @PostMapping(value = "/partitions/add")
    @ApiOperation(value = "Add a partition to the topic")
    public TopicMeta addPartition(@RequestBody AddPartition addPartition){
        String topic = addPartition.getTopic();
        isTopicExist(topic);

        if(addPartition.getReplicaAssignment()!=null && !addPartition.getReplicaAssignment().equals("") && addPartition.getReplicaAssignment().split(",").length
                != addPartition.getNumPartitionsAdded()){
            throw new InvalidTopicException("Topic "+ topic + ": num of partitions added not equal to manual reassignment str!");
        }

        if(addPartition.getNumPartitionsAdded()==0){
            throw new InvalidTopicException("Num of paritions added must be specified and should not be 0");
        }
        return kafkaAdminService.addPartition(topic,addPartition);
    }

    @PostMapping(value = "/partitions/reassign/generate")
    @ApiOperation(value = "Generate plan for the partition reassignment")
    public List<String> generateReassignPartitions(@RequestBody ReassignWrapper reassignWrapper){
        return kafkaAdminService.generateReassignPartition(reassignWrapper);

    }

    @PutMapping(value = "/partitions/reassign/execute")
    @ApiOperation(value = "Execute the partition reassignment")
    public Map<TopicAndPartition, Integer> executeReassignPartitions(
            @RequestBody String reassignStr,
            @RequestParam(required = false,defaultValue = "-1" ) long throttle) {
        return kafkaAdminService.executeReassignPartition(reassignStr,throttle);
    }

    @PutMapping(value = "/partitions/reassign/check")
    @ApiOperation(value = "Check the partition reassignment process")
    @ApiResponses(value = {@ApiResponse(code = 1, message = "Reassignment Completed"),
            @ApiResponse(code = 0, message = "Reassignment In Progress"),
            @ApiResponse(code = -1, message = "Reassignment Failed")})
    public Map<TopicAndPartition, Integer> checkReassignPartitions(@RequestBody String reassignStr){
        return kafkaAdminService.checkReassignStatus(reassignStr);
    }

    @GetMapping(value = "/consumergroups/old")
    @ApiOperation(value = "List old consumer groups from zk")
    public List<String> listAllOldConsumerGroups(
            @RequestParam(value = "t", required = false) String topic) throws Exception {

        if (Strings.isNullOrEmpty(topic)){
            return kafkaAdminService.listAllOldConsumerGroups();
        }
        return kafkaAdminService.listOldConsumerGroupsByTopic(topic);
    }


    @GetMapping(value = "/consumergroups/new")
    @ApiOperation(value = "List new consumer groups")
    public List<String> listAllNewConsumerGroups(
            @RequestParam(value = "t", required = false) String topic) throws Exception {
        if (Strings.isNullOrEmpty(topic) ) {
            return kafkaAdminService.listAllNewConsumerGroups();
        }

        return kafkaAdminService.listNewConsumerGroupsByTopic(topic);
    }


    @GetMapping(value = "/consumergroups/old/{consumerGroup}/{topic}")
    @ApiOperation(value = "List old consumergroups lag and offset by consumergroup and topic")
    public List<ConsumerGroupDesc> describeOldCG(String consumerGroup, String topic){
        if(!Strings.isNullOrEmpty(topic)){
            existTopic(topic);
        }
        return kafkaAdminService.describeOldCG(consumerGroup, topic);
    }

    @GetMapping(value = "/consumergroups/new/{consumerGroup}/{topic}")
    @ApiOperation(value = "List new consumergroups lag and offset by consumergroup and topic")
    public List<ConsumerGroupDesc> describeNewCG(String consumerGroup, String topic){
        if(!Strings.isNullOrEmpty(topic)){
            existTopic(topic);
        }
        return kafkaAdminService.describeNewCG(consumerGroup,topic);
    }

    @GetMapping(value = "/consumer/{topic}/{partition}/{offset}")
    public String getMessage(String topic, int partition, long offset, String decoder){
        return kafkaAdminService.getMessage(topic, partition, offset, decoder,"");
    }

    @PutMapping(value = "/consumergroup/{consumergroup}/{topic}/{partition}/{offset}")
    public void resetOffset(String topic, int partition, String consumergroup, String offset){
        kafkaAdminService.resetOffset(topic, partition, consumergroup, offset);
    }

    @GetMapping(value = "/consumergroup/{consumergroup}/{topic}/lastcommittime")
    public Map<String, Map<Integer,Long>> getLastCommitTimestamp(String consumergroup, String topic){
        return kafkaAdminService.getLastCommitTime(consumergroup, topic);
    }

    @DeleteMapping(value = "/consumergroup/old/{consumergroup}")
    public boolean deleteOldConsumerGroup(String consumergroup){
        return kafkaAdminService.deleteConsumerGroup(consumergroup);
    }

    private void isTopicExist(String topic) throws InvalidTopicException {
        if(!kafkaAdminService.existTopic(topic)){
            throw new InvalidTopicException("Topic "+ topic + " non-exist!");
        }
    }
}
