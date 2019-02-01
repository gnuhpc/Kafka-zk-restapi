package org.gnuhpc.bigdata.model;

import java.util.List;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.log4j.Log4j;

/** Created by gnuhpc on 2017/7/23. */
@Getter
@Setter
@Log4j
@ToString
@Builder
public class AddPartition {

  String topic;
  int numPartitionsAdded;
  List<List<Integer>> replicaAssignment;
}
