package org.gnuhpc.bigdata.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.LinkedList;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class JMXMetricData {

  private String host;

  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
  private LocalDateTime timestamp;

  private Boolean collected;
  private LinkedList<HashMap<String, Object>> metrics;
  private String msg;

  public JMXMetricData(String host, LinkedList<HashMap<String, Object>> metrics) {
    this.host = host;
    this.timestamp = LocalDateTime.now();
    this.metrics = metrics;
  }
}
