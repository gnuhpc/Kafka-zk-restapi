package org.gnuhpc.bigdata.model;

import java.io.Serializable;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.common.TopicPartitionInfo;

@Getter
@Setter
@ToString
@EqualsAndHashCode
public class CustomTopicPartitionInfo
    implements Comparable<CustomTopicPartitionInfo>, Serializable {

  private TopicPartitionInfo topicPartitionInfo;
  private boolean in_sync;
  private long startOffset;
  private long endOffset;

  @Setter(AccessLevel.NONE)
  private long messageAvailable;

  public void setIn_sync() {
    if (topicPartitionInfo.isr() != null
        && topicPartitionInfo.replicas() != null
        && topicPartitionInfo.isr().size() == topicPartitionInfo.replicas().size()) {
      in_sync =
          CollectionUtils.isEqualCollection(
              topicPartitionInfo.isr(), topicPartitionInfo.replicas());
    } else {
      in_sync = false;
    }
  }

  public void setMessageAvailable() {
    this.messageAvailable = this.endOffset - this.startOffset;
  }

  @Override
  public int compareTo(CustomTopicPartitionInfo topicPartitionInfo) {
    if (this.topicPartitionInfo.partition() < topicPartitionInfo.topicPartitionInfo.partition()) {
      return -1;
    } else if (this.topicPartitionInfo.partition()
        == topicPartitionInfo.topicPartitionInfo.partition()) {
      return 0;
    } else {
      return 1;
    }
  }

  public TopicPartitionInfo getTopicPartitionInfo() {
    return this.topicPartitionInfo;
  }
}
