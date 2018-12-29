package org.gnuhpc.bigdata.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.apache.kafka.common.Node;

@Data
@Builder
@AllArgsConstructor
public class PartitionAssignmentState {

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
}
