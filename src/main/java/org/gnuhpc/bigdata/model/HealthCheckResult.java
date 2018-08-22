package org.gnuhpc.bigdata.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
public class HealthCheckResult {
  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
  private LocalDateTime timestamp;
  public String status;
  public String msg;

  public HealthCheckResult() {
    this.timestamp = LocalDateTime.now();
  }
}
