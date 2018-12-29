package org.gnuhpc.bigdata.model;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.TopicPartition;

@AllArgsConstructor
@Getter
@Setter
public class MemberDescription {

  private String memberId;
  private String clientId;
  private String host;
  private List<TopicPartition> assignment;
}
