package org.gnuhpc.bigdata.model;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.apache.kafka.common.Node;
import org.gnuhpc.bigdata.constant.ConsumerGroupState;

@Data
@Builder
public class ConsumerGroupMeta {
  private String groupId;
  private ConsumerGroupState state;
  private String assignmentStrategy;
  private Node coordinator;
  private List<MemberDescription> members;
}
