package org.gnuhpc.bigdata.model;

import java.util.Map;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;

@Getter
@Setter
@Data
public class ReassignStatus {
  Map<TopicPartition, Integer> partitionsReassignStatus;
  Map<TopicPartitionReplica, Integer> replicasReassignStatus;
  boolean removeThrottle;
  String msg;
}
