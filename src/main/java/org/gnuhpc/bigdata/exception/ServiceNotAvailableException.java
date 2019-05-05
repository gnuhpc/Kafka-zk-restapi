package org.gnuhpc.bigdata.exception;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.gnuhpc.bigdata.constant.ZkServerMode;

@Data
@EqualsAndHashCode
public class ServiceNotAvailableException extends RuntimeException {

  private String serviceType;
  private ZkServerMode serviceState;

  public ServiceNotAvailableException(String serviceType, ZkServerMode serviceState, String message) {
    super(message);
    this.serviceType = serviceType;
    this.serviceState = serviceState;
  }

  @Override
  public String toString() {
    return "ServiceNotAvailableException{"
        + "serviceType='"
        + serviceType
        + '\''
        + ", serviceState='"
        + serviceState
        + '\''
        + '}';
  }
}
