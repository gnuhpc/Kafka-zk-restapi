package org.gnuhpc.bigdata.model;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.common.Node;
import org.gnuhpc.bigdata.constant.ConsumerGroupState;
import org.gnuhpc.bigdata.constant.ConsumerType;

/** Created by gnuhpc on 2017/7/27. */
@Setter
@Getter
@Log4j
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
  private String host = "-";
  private ConsumerType type;

  @Override
  public int compareTo(ConsumerGroupDesc o) {
    if (this.topic.equals(o.topic)) {
      if (this.partitionId == o.partitionId) {
        return 0;
      } else if (this.partitionId < o.partitionId) {
        return -1;
      } else {
        return 1;
      }
    } else {
      return this.topic.compareTo(o.topic);
    }
  }
}
