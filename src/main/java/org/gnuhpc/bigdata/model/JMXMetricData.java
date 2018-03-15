package org.gnuhpc.bigdata.model;

import java.time.LocalDateTime;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class JMXMetricData {
  private String host;
  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
  private LocalDateTime timestamp;
  private Boolean collected;
  private Map<String, Object> mbeanInfo;
  private String msg;

  public JMXMetricData(String host, Map<String, Object> mbeanInfo) {
    this.host = host;
    this.timestamp = LocalDateTime.now();
    this.mbeanInfo = mbeanInfo;
  }
}
