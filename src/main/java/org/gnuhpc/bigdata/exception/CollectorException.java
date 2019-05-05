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

  public String catchStackTrace() {
    String stackTraceString = "";
    StackTraceElement[] stackElements = this.getStackTrace();
    if (stackElements != null) {
      for (int i = 0; i < stackElements.length; i++) {
        stackTraceString = stackTraceString + stackElements[i].getClassName()+"\\/t";
        stackTraceString = stackTraceString + stackElements[i].getFileName()+"\\/t";
        stackTraceString = stackTraceString + stackElements[i].getLineNumber()+"\\/t";
        stackTraceString = stackTraceString + stackElements[i].getMethodName()+"\\/t";
      }
    }
    return stackTraceString;
  }
}
