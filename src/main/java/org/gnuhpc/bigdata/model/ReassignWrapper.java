package org.gnuhpc.bigdata.model;

import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/** Created by gnuhpc on 2017/7/25. */
@Getter
@Setter
@ToString
public class ReassignWrapper {

  private List<String> topics;
  private List<Integer> brokers;

  public String generateReassignJsonString() {
    ReassignJsonWrapper reassignJsonWrapper = new ReassignJsonWrapper();
    List<Map<String, String>> topicList = new ArrayList<>();
    for (String topic : topics) {
      Map<String, String> topicMap = new HashMap<>();
      topicMap.put("topic", topic);
      topicList.add(topicMap);
    }
    reassignJsonWrapper.setTopics(topicList);
    Gson gson = new Gson();
    return gson.toJson(reassignJsonWrapper);
  }
}
