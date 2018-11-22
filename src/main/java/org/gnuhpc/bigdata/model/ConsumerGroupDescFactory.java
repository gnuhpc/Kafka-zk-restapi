package org.gnuhpc.bigdata.model;

import kafka.admin.AdminClient;
import kafka.common.OffsetAndMetadata;
import kafka.coordinator.group.GroupTopicPartition;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.gnuhpc.bigdata.constant.ConsumerState;
import org.gnuhpc.bigdata.constant.ConsumerType;
import org.gnuhpc.bigdata.utils.KafkaUtils;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

@AllArgsConstructor
public class ConsumerGroupDescFactory {
    private KafkaUtils kafkaUtils;

    public ConsumerGroupDesc makeOldConsumerGroupDesc(
            Map.Entry<Integer, String> op,
            Map<Integer, Long> fetchOffSetFromZKResultList,
            String topic, String consumerGroup, TopicMeta topicMeta) {

        ConsumerGroupDesc.Builder cgdBuilder = ConsumerGroupDesc.newBuilder()
                .setGroupName(consumerGroup)
                .setTopic(topic)
                .setPartitionId(op.getKey())
                .setCurrentOffset(fetchOffSetFromZKResultList.get(op.getKey()))
                .setLogEndOffset(
                        topicMeta.getTopicPartitionInfos().stream()
                                .filter(tpi -> tpi.getTopicPartitionInfo().partition() == op.getKey()).findFirst().get().getEndOffset());


        if (op.getValue().equals("none")) {
            cgdBuilder.setConsumerId("-");
            cgdBuilder.setHost("-");
            cgdBuilder.setState(ConsumerState.PENDING);
        } else {
            cgdBuilder.setConsumerId(op.getValue());
            cgdBuilder.setHost(op.getValue().replace(consumerGroup + "_", ""));
            cgdBuilder.setState(ConsumerState.RUNNING);
        }
        cgdBuilder.setType(ConsumerType.OLD);
        return cgdBuilder.build();
    }

    public ConsumerGroupDesc makeNewRunningConsumerGroupDesc(
            TopicPartition tp,
            String consumerGroup,
            Map<Integer, Long> partitionEndOffsetMap,
            AdminClient.ConsumerSummary cs) {
        KafkaConsumer consumer = kafkaUtils.createNewConsumer(consumerGroup);
        ConsumerGroupDesc.Builder cgdBuilder = ConsumerGroupDesc.newBuilder()
                .setGroupName(consumerGroup)
                .setTopic(tp.topic())
                .setPartitionId(tp.partition())
                .setConsumerId(cs.clientId())
                .setHost(cs.host())
                .setState(ConsumerState.RUNNING)
                .setType(ConsumerType.NEW);

        long currentOffset = -1L;

        org.apache.kafka.clients.consumer.OffsetAndMetadata offset = consumer.committed(new TopicPartition(tp.topic(), tp.partition()));
        if (offset != null) {
            currentOffset = offset.offset();
        }
        cgdBuilder.setCurrentOffset(currentOffset);

        Long endOffset = partitionEndOffsetMap.get(tp.partition());
        if (endOffset == null) { //if endOffset is null ,the partition of this topic has no leader replication
            cgdBuilder.setLogEndOffset(-1l);
        } else {
            cgdBuilder.setLogEndOffset(endOffset);
        }
        consumer.close();

        return cgdBuilder.build();
    }

    public ConsumerGroupDesc makeNewPendingConsumerGroupDesc(
            String consumerGroup,
            Map<Integer, Long> partitionEndOffsetMap,
            Map.Entry<GroupTopicPartition, OffsetAndMetadata> topicStorage,
            String topic) {
        Long partitionCurrentOffset = (topicStorage.getValue() == null) ? -1l: topicStorage.getValue().offset();

        int partitionId = topicStorage.getKey().topicPartition().partition();
        ConsumerGroupDesc.Builder cgdBuilder = ConsumerGroupDesc.newBuilder()
                .setGroupName(consumerGroup)
                .setTopic(topic)
                .setConsumerId("-")
                .setPartitionId(partitionId)
                .setCurrentOffset(partitionCurrentOffset)
                .setHost("-")
                .setState(ConsumerState.PENDING)
                .setType(ConsumerType.NEW);

        Long endOffset = partitionEndOffsetMap.get(partitionId);

        if (endOffset == null) { //if endOffset is null ,the partition of this topic has no leader replication
            cgdBuilder.setLogEndOffset(-1l);
        } else {
            cgdBuilder.setLogEndOffset(endOffset);
        }

        return cgdBuilder.build();
    }
}
