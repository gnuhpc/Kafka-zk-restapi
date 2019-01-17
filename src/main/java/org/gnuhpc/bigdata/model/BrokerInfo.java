package org.gnuhpc.bigdata.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.gnuhpc.bigdata.serializer.JsonJodaDateTimeSerializer;
import org.gnuhpc.bigdata.utils.TimestampDeserializer;
import org.joda.time.DateTime;

@Getter
@Setter
@AllArgsConstructor
@ToString
public class BrokerInfo {
  private Map listener_security_protocol_map;

  private List<String> endpoints;

  private int jmxport;

  private String host;

  public DateTime starttime;

  private int port;
  private int version;
  private String rack = "";
  private int id = -1;

  @JsonProperty("jmxPort")
  public int getJmxport() {
    return jmxport;
  }

  @JsonProperty("jmx_port")
  public void setJmxPort(int jmxport) {
    this.jmxport = jmxport;
  }

  @JsonProperty("securityProtocol")
  public Map getListener_security_protocol_map() {
    return listener_security_protocol_map;
  }

  @JsonProperty("listener_security_protocol_map")
  public void setSecurityProtocol(Map listener_security_protocol_map) {
    this.listener_security_protocol_map = listener_security_protocol_map;
  }

  @JsonProperty("startTime")
  @JsonSerialize(using = JsonJodaDateTimeSerializer.class)
  public DateTime getStarttime() {
    return starttime;
  }

  @JsonProperty("timestamp")
  @JsonDeserialize(using = TimestampDeserializer.class)
  public void setStartTime(DateTime timestamp) {
    this.starttime = timestamp;
  }
}
