package com.github.kagkarlsson.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.kagkarlsson.scheduler.task.Task;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

public class SchedulerBuilderTest {

  private static SchedulerBuilder builder() {
    return new SchedulerBuilder(null, List.<Task<?>>of());
  }

  @Test
  public void polling_interval_must_be_greater_than_zero() {
    assertThatThrownBy(() -> builder().pollingInterval(Duration.ZERO))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Polling interval must be greater than 0");

    assertThatThrownBy(() -> builder().pollingInterval(Duration.ofSeconds(-1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Polling interval must be greater than 0");

    assertThatThrownBy(() -> builder().pollingInterval(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Polling interval must not be null");
  }

  @Test
  public void polling_interval_accepts_positive_duration() {
    SchedulerBuilder builder = builder();

    assertThat(builder.pollingInterval(Duration.ofMillis(1))).isSameAs(builder);
    assertThat(builder.poolingInterval).isEqualTo(Duration.ofMillis(1));
    assertThat(builder().poolingInterval).isEqualTo(SchedulerBuilder.DEFAULT_POLLING_INTERVAL);
  }
}
