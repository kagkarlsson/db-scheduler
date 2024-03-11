package com.github.kagkarlsson.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  public static void sleep(int millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static void retryOnFailed(int retryTimes, Runnable r) {
    try {
      r.run();
    } catch (RuntimeException | AssertionError e) {
      if (retryTimes == 0) {
        throw e;
      } else {
        LOG.info("Retrying test after failure.", e);
        retryOnFailed(retryTimes - 1, r);
      }
    }
  }
}
