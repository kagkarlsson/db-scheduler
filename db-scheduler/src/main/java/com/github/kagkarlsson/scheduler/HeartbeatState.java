/*
 * Copyright (C) Gustav Karlsson
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

    long heartbeatMillis = heartbeatConfig.heartbeatInterval.toMillis();
    long millisUntilConsideredStale = heartbeatMillis + Math.min(10_000, (int)(heartbeatMillis * 0.25));
    return heartbeatFailuresSinceLastSuccess > 0
        || sinceLastSuccess.toMillis() > millisUntilConsideredStale;
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

  public String describe() {
    return "HeartbeatState{"
        + "successesSinceLastFailure="
        + heartbeatSuccessesSinceLastFailure
        + ", failuresSinceLastSuccess="
        + heartbeatFailuresSinceLastSuccess
        + ", lastSuccess="
        + heartbeatLastSuccess
        + ", lastFailure="
        + heartbeatLastFailure
        + ", missedHeartbeatsLimit="
        + heartbeatConfig.missedHeartbeatsLimit
        + '}';
  }
}
