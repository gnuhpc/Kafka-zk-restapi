package org.gnuhpc.bigdata.model;

import java.util.Map;
import kafka.admin.AdminClient;
import kafka.common.OffsetAndMetadata;
import kafka.coordinator.group.GroupTopicPartition;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.gnuhpc.bigdata.constant.ConsumerGroupState;
import org.gnuhpc.bigdata.constant.ConsumerType;
import org.gnuhpc.bigdata.utils.KafkaUtils;

@AllArgsConstructor
@NoArgsConstructor
public class ConsumerGroupDescFactory {

  private KafkaUtils kafkaUtils;

  public ConsumerGroupDesc makeOldConsumerGroupDesc(
      Map.Entry<Integer, String> op,
      Map<Integer, Long> fetchOffSetFromZkResultList,
      String topic,
      String consumerGroup,
      TopicMeta topicMeta) {

    ConsumerGroupDesc.ConsumerGroupDescBuilder cgdBuilder =
        ConsumerGroupDesc.builder()
            .groupName(consumerGroup)
            .topic(topic)
            .partitionId(op.getKey())
            .currentOffset(fetchOffSetFromZkResultList.get(op.getKey()))
            .logEndOffset(
                topicMeta
                    .getTopicPartitionInfos()
                    .stream()
                    .filter(tpi -> tpi.getTopicPartitionInfo().partition() == op.getKey())
                    .findFirst()
                    .get()
                    .getEndOffset());

    if (op.getValue().equals("none")) {
      cgdBuilder.consumerId("-");
      cgdBuilder.host("-");
      cgdBuilder.state(ConsumerGroupState.EMPTY);
    } else {
      cgdBuilder.consumerId(op.getValue());
      cgdBuilder.host(op.getValue().replace(consumerGroup + "_", ""));
      cgdBuilder.state(ConsumerGroupState.STABLE);
    }
    cgdBuilder.type(ConsumerType.OLD);
    return cgdBuilder.build();
  }

  public ConsumerGroupDesc makeNewRunningConsumerGroupDesc(
      TopicPartition tp,
      String consumerGroup,
      Map<Integer, Long> partitionEndOffsetMap,
      AdminClient.ConsumerSummary cs) {
    KafkaConsumer consumer = kafkaUtils.createNewConsumer(consumerGroup);
    ConsumerGroupDesc.ConsumerGroupDescBuilder cgdBuilder =
        ConsumerGroupDesc.builder()
            .groupName(consumerGroup)
            .topic(tp.topic())
            .partitionId(tp.partition())
            .consumerId(cs.clientId())
            .host(cs.host())
            .state(ConsumerGroupState.STABLE)
            .type(ConsumerType.NEW);

    long currentOffset = -1L;

    org.apache.kafka.clients.consumer.OffsetAndMetadata offset =
        consumer.committed(new TopicPartition(tp.topic(), tp.partition()));
    if (offset != null) {
      currentOffset = offset.offset();
    }
    cgdBuilder.currentOffset(currentOffset);

    Long endOffset = partitionEndOffsetMap.get(tp.partition());
    if (endOffset
        == null) { // if endOffset is null ,the partition of this topic has no leader replication
      cgdBuilder.logEndOffset(-1L);
    } else {
      cgdBuilder.logEndOffset(endOffset);
    }
    consumer.close();

    return cgdBuilder.build();
  }

  public ConsumerGroupDesc makeNewPendingConsumerGroupDesc(
      String consumerGroup,
      Map<Integer, Long> partitionEndOffsetMap,
      Map.Entry<GroupTopicPartition, OffsetAndMetadata> topicStorage,
      String topic) {
    Long partitionCurrentOffset =
        (topicStorage.getValue() == null) ? -1L : topicStorage.getValue().offset();

    int partitionId = topicStorage.getKey().topicPartition().partition();
    ConsumerGroupDesc.ConsumerGroupDescBuilder cgdBuilder =
        ConsumerGroupDesc.builder()
            .groupName(consumerGroup)
            .topic(topic)
            .consumerId("-")
            .partitionId(partitionId)
            .currentOffset(partitionCurrentOffset)
            .host("-")
            .state(ConsumerGroupState.EMPTY)
            .type(ConsumerType.NEW);

    Long endOffset = partitionEndOffsetMap.get(partitionId);

    if (endOffset
        == null) { // if endOffset is null ,the partition of this topic has no leader replication
      cgdBuilder.logEndOffset(-1L);
    } else {
      cgdBuilder.logEndOffset(endOffset);
    }

    return cgdBuilder.build();
  }
}
