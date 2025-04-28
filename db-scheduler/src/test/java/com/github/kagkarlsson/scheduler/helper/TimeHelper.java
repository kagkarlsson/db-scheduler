package com.github.kagkarlsson.scheduler.helper;

import static java.time.temporal.ChronoUnit.MILLIS;

import java.time.Instant;

public final class TimeHelper {
  private TimeHelper() {}

  /**
   * Similar to {@link Instant#now()} but truncated to millisecond precision.
   *
   * @see <a href="https://bugs.openjdk.java.net/browse/JDK-8068730">JDK-8068730</a>
   */
  public static Instant truncatedInstantNow() {
    return truncated(Instant.now());
  }

  private static Instant truncated(Instant instant) {
    return instant.truncatedTo(MILLIS);
  }
}
