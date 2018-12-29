package org.gnuhpc.bigdata.model;

import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/** Created by gnuhpc on 2017/7/25. */
@Setter
@Getter
@ToString
public class ReassignJsonWrapper {

  private List<Map<String, String>> topics;
  private int version = 1;
}
