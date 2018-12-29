package org.gnuhpc.bigdata.exception;

public class CollectorException extends Exception {

  public CollectorException(String message, Throwable cause) {
    super(message, cause);
  }

  public CollectorException(String message) {
    super(message);
  }

  public CollectorException(Throwable cause) {
    super(cause);
  }

  public CollectorException() {
    super();
  }
}
