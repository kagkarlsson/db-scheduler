package com.github.kagkarlsson.scheduler.exceptions;

public class FailedToScheduleBatchException extends DbSchedulerException {
  private static final long serialVersionUID = -2132850112553296792L;

  public FailedToScheduleBatchException(String message) {
    super(message);
  }

  public FailedToScheduleBatchException(String message, Throwable cause) {
    super(message, cause);
  }
}
