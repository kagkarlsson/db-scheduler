package com.github.kagkarlsson.scheduler;

import java.time.Duration;

public class HeartbeatConfig {

  public final Duration heartbeatInterval;
  public final int missedHeartbeatsLimit;
  public final Duration maxAgeBeforeConsideredDead;

  public HeartbeatConfig(
      Duration heartbeatInterval, int missedHeartbeatsLimit, Duration maxAgeBeforeConsideredDead) {
    this.heartbeatInterval = heartbeatInterval;
    this.missedHeartbeatsLimit = missedHeartbeatsLimit;
    this.maxAgeBeforeConsideredDead = maxAgeBeforeConsideredDead;
  }
}
