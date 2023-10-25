package com.github.kagkarlsson.scheduler;

import java.time.Duration;
import java.time.Instant;

public class HeartbeatState {
  private Clock clock;
  private final Instant startTime;
  private final HeartbeatConfig heartbeatConfig;
  private int heartbeatSuccessesSinceLastFailure = 0;
  private int heartbeatFailuresSinceLastSuccess = 0;
  private Instant heartbeatLastSuccess;
  private Instant heartbeatLastFailure;

  public HeartbeatState(Clock clock, Instant startTime, HeartbeatConfig heartbeatConfig) {
    this.clock = clock;
    this.startTime = startTime;
    this.heartbeatConfig = heartbeatConfig;
    heartbeatLastSuccess = startTime;
  }

  public boolean hasStaleHeartbeat() {
    Duration sinceLastSuccess = Duration.between(heartbeatLastSuccess, clock.now());
    return heartbeatFailuresSinceLastSuccess > 0
        || sinceLastSuccess.toMillis() > heartbeatConfig.heartbeatInterval.toMillis();
  }

  public double getFractionDead() {
    Duration sinceLastSuccess = Duration.between(heartbeatLastSuccess, clock.now());
    return (double) sinceLastSuccess.toMillis()
        / heartbeatConfig.maxAgeBeforeConsideredDead.toMillis();
  }

  public int getFailedHeartbeats() {
    return heartbeatFailuresSinceLastSuccess;
  }

  public void heartbeat(boolean successful, Instant lastHeartbeatAttempt) {
    if (successful) {
      heartbeatLastSuccess = lastHeartbeatAttempt;
      heartbeatSuccessesSinceLastFailure++;
      heartbeatFailuresSinceLastSuccess = 0;
    } else {
      heartbeatLastFailure = lastHeartbeatAttempt;
      heartbeatSuccessesSinceLastFailure = 0;
      heartbeatFailuresSinceLastSuccess++;
    }
  }
}
