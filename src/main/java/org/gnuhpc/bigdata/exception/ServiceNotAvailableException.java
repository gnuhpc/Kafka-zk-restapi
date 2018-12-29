package org.gnuhpc.bigdata.exception;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
public class ServiceNotAvailableException extends RuntimeException {

  private String serviceType;
  private String serviceState;

  public ServiceNotAvailableException(String serviceType, String serviceState, String message) {
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
