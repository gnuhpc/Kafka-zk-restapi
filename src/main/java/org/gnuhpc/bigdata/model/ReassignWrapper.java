package org.gnuhpc.bigdata.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.errors.ApiException;

/** Created by gnuhpc on 2017/7/25. */
@Getter
@Setter
@ToString
@Log4j2
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
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      return objectMapper.writeValueAsString(reassignJsonWrapper);
    } catch (JsonProcessingException exeception) {
      log.error("Serialize ReassignWrapper object to string error." + exeception);
      throw new ApiException("Serialize ReassignWrapper object to string error." + exeception);
    }
  }
}
