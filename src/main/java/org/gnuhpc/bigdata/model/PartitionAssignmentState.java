package org.gnuhpc.bigdata.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.apache.kafka.common.Node;

@Data
@Builder
@AllArgsConstructor
public class PartitionAssignmentState implements Comparable<PartitionAssignmentState>{

  private String group;
  private Node coordinator;
  private String topic;
  private int partition;
  private long offset;
  private long lag;
  private String consumerId;
  private String host;
  private String clientId;
  private long logEndOffset;

  @Override
  public int compareTo(PartitionAssignmentState that) {
    if (this.getGroup().equals(that.getGroup())) {
      if (this.getTopic().equals(that.getTopic())) {
        return (this.partition - that.getPartition());
      } else {
        return this.getTopic().compareTo(that.getTopic());
      }
    } else {
      return this.getGroup().compareTo(that.getGroup());
    }
  }
}
