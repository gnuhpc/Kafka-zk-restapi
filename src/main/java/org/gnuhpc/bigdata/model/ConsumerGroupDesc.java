package org.gnuhpc.bigdata.model;

import lombok.*;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.Node;
import org.gnuhpc.bigdata.constant.ConsumerGroupState;
import org.gnuhpc.bigdata.constant.ConsumerType;

/** Created by gnuhpc on 2017/7/27. */
@Setter
@Getter
@Log4j2
@ToString
@EqualsAndHashCode
@Builder(builderClassName = "ConsumerGroupDescBuilder")
public class ConsumerGroupDesc implements Comparable<ConsumerGroupDesc> {

  private String groupName;
  private ConsumerGroupState state;
  private String assignmentStrategy;
  private Node coordinator;
  private String topic;
  private int partitionId;
  private long currentOffset;
  private long logEndOffset;

  @Setter(AccessLevel.NONE)
  private long lag;

  private String consumerId;
  private String clientId;
  private String host = "-";
  private ConsumerType type;

  @Override
  public int compareTo(ConsumerGroupDesc o) {
    if (this.topic.equals(o.topic)) {
      return (this.partitionId - o.partitionId);
    } else {
      return this.topic.compareTo(o.topic);
    }
  }
}
