package org.gnuhpc.bigdata.model;

import kafka.admin.AdminClient;
import kafka.common.OffsetAndMetadata;
import kafka.coordinator.GroupTopicPartition;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.gnuhpc.bigdata.constant.ConsumerState;
import org.gnuhpc.bigdata.constant.ConsumerType;
import org.gnuhpc.bigdata.utils.KafkaUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@AllArgsConstructor
public class ConsumerGroupDescFactory {
    private KafkaUtils kafkaUtils;
    public ConsumerGroupDesc makeOldConsumerGroupDesc(
            Map.Entry<Integer, String> op,
            Map<Integer, Long> fetchOffSetFromZKResultList,
            String topic, String consumerGroup, TopicMeta topicMeta) {

        ConsumerGroupDesc cgd = ConsumerGroupDesc.newBuilder()
                .setGroupName(consumerGroup)
                .setTopic(topic)
                .setPartitionId(op.getKey())
                .setCurrentOffset(fetchOffSetFromZKResultList.get(op.getKey()))
                .setLogEndOffset(
                        topicMeta.getTopicPartitionInfos().stream()
                                .filter(tpi -> tpi.getPartitionId() == op.getKey()).findFirst().get().getEndOffset())
                .build();

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
    }

    public ConsumerGroupDesc makeNewRunningConsumerGroupDesc(
            TopicPartition tp,
            String consumerGroup,
            Map<Integer, Long> partitionEndOffsetMap,
            AdminClient.ConsumerSummary cs) {
        KafkaConsumer consumer = kafkaUtils.createNewConsumer(consumerGroup);
        ConsumerGroupDesc cgd = ConsumerGroupDesc.newBuilder()
                .setGroupName(consumerGroup)
                .setTopic(tp.topic())
                .setPartitionId(tp.partition())
                .setConsumerId(cs.clientId())
                .setHost(cs.clientHost())
                .setState(ConsumerState.RUNNING)
                .setType(ConsumerType.NEW).build();

        long currentOffset = -1L;
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
        consumer.close();

        return cgd;
    }

    public ConsumerGroupDesc makeNewPendingConsumerGroupDesc(
            String consumerGroup,
            Map<Integer, Long> partitionEndOffsetMap,
            Map<Integer, Long> partitionCurrentOffsetMap,
            Map.Entry<GroupTopicPartition, OffsetAndMetadata> storage,
            String topic) {
        KafkaConsumer consumer = kafkaUtils.createNewConsumer(consumerGroup);
        ConsumerGroupDesc cgd = ConsumerGroupDesc.newBuilder()
                .setGroupName(consumerGroup)
                .setTopic(topic)
                .setConsumerId("-")
                .setPartitionId(storage.getKey().topicPartition().partition())
                .setCurrentOffset(partitionCurrentOffsetMap.get(storage.getKey().topicPartition().partition()))
                .setHost("-")
                .setState(ConsumerState.PENDING)
                .setType(ConsumerType.NEW)
                .build();

        Long endOffset = partitionEndOffsetMap.get(cgd.getPartitionId());

        if (endOffset == null) { //if endOffset is null ,the partition of this topic has no leader replication
            cgd.setLogEndOffset(-1l);
        } else {
            cgd.setLogEndOffset(endOffset);
        }

        return cgd;
    }
}
