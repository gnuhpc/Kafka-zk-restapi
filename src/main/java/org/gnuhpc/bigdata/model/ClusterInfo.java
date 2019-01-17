package org.gnuhpc.bigdata.model;

import java.util.Collection;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.Node;

@Getter
@Setter
public class ClusterInfo {
  private Collection<Node> nodes;
  private Node controller;
  private String clusterId;
}
