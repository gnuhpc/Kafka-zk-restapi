package org.gnuhpc.bigdata.exception;

public enum KafkaErrorCode {
  NO_ERROR,
  OFFSET_OUT_OF_RANGE,
  INVALID_MESSAGE,
  UNKNOWN_TOPIC_OR_PARTITION,
  INVALID_FETCH_SIZE,
  LEADER_NOT_AVAILABLE,
  NOT_LEADER_FOR_PARTITION,
  REQUEST_TIMED_OUT,
  BROKER_NOT_AVAILABLE,
  REPLICA_NOT_AVAILABLE,
  MESSAGE_SIZE_TOO_LARGE,
  STALE_CONTROLLER_EPOCH,
  OFFSET_METADATA_TOO_LARGE,
  OFFSETS_LOAD_IN_PROGRESS,
  CONSUMER_COORDINATOR_NOT_AVAILABLE,
  NOT_COORDINATOR_FOR_CONSUMER,
  SERVICE_DOWN,
  UNKNOWN;

  public static KafkaErrorCode getError(int errorCode) {
    if (errorCode < 0 || errorCode >= UNKNOWN.ordinal()) {
      return UNKNOWN;
    } else {
      return values()[errorCode];
    }
  }
}
