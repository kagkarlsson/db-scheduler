package com.github.kagkarlsson.scheduler;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import java.time.Duration;
import org.hamcrest.Matchers;
import org.hamcrest.number.IsCloseTo;
import org.junit.jupiter.api.Test;

class HeartbeatStateTest {
  private SettableClock clock = new SettableClock();

  @Test
  void happy() {
    HeartbeatState state =
        new HeartbeatState(
            clock,
            clock.now(),
            new HeartbeatConfig(Duration.ofMinutes(1), 4, Duration.ofMinutes(4)));

    assertOk(state);

    clock.tick(Duration.ofSeconds(30));
    state.heartbeat(true, clock.now());

    assertOk(state);

    clock.tick(Duration.ofSeconds(60));
    state.heartbeat(false, clock.now());

    assertFailing(state, 1, 0.25);
  }

  @Test
  void success_resets_failing() {
    HeartbeatState state =
        new HeartbeatState(
            clock,
            clock.now(),
            new HeartbeatConfig(Duration.ofMinutes(1), 4, Duration.ofMinutes(4)));

    assertOk(state);

    clock.tick(Duration.ofSeconds(30));
    state.heartbeat(false, clock.now());

    assertFailing(state, 1, 0.125);

    clock.tick(Duration.ofSeconds(60));
    state.heartbeat(false, clock.now());

    assertFailing(state, 2, 0.375);

    clock.tick(Duration.ofSeconds(60));
    state.heartbeat(true, clock.now());

    assertOk(state);
  }

  @Test
  void not_stale_until_tolerance_passed() {
    HeartbeatState state =
      new HeartbeatState(
        clock,
        clock.now(),
        new HeartbeatConfig(Duration.ofSeconds(60), 4, Duration.ofMinutes(4)));

    assertOk(state);

    clock.tick(Duration.ofSeconds(60));
    assertThat(state.hasStaleHeartbeat(), is(false));

    clock.tick(Duration.ofSeconds(5));
    assertThat(state.hasStaleHeartbeat(), is(false));

    clock.tick(Duration.ofSeconds(25));
    assertThat(state.hasStaleHeartbeat(), is(true));

    state.heartbeat(true, clock.now());
    assertThat(state.hasStaleHeartbeat(), is(false));

    assertOk(state);
  }

  private void assertFailing(HeartbeatState state, int timesFailed, double fractionDead) {
    assertTrue(state.hasStaleHeartbeat());
    assertThat(state.getFailedHeartbeats(), is(timesFailed));
    assertThat(state.getFractionDead(), IsCloseTo.closeTo(fractionDead, 0.01));
  }

  private void assertOk(HeartbeatState state) {
    assertFalse(state.hasStaleHeartbeat());
    assertThat(state.getFailedHeartbeats(), is(0));
    assertThat(state.getFractionDead(), Matchers.lessThan(0.01));
  }
}
