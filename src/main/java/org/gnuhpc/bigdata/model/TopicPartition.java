package org.gnuhpc.bigdata.model;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Data
public class TopicPartition {
  String topic;
  int partition;

  @Override
  public String toString() {
    return topic + "-" + partition;
  }
}
