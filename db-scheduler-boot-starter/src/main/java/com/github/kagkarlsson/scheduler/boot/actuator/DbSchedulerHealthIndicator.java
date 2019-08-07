package com.github.kagkarlsson.scheduler.boot.actuator;

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerState;
import java.util.Objects;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

public class DbSchedulerHealthIndicator implements HealthIndicator {
    private final SchedulerState state;

    public DbSchedulerHealthIndicator(Scheduler scheduler) {
        this.state = Objects.requireNonNull(scheduler).getSchedulerState();
    }

    @Override
    public Health health() {
        if (state.isStarted() && !state.isShuttingDown()) {
            return Health.up()
                .withDetail("state", "started")
                .build();
        } else if (state.isStarted() && state.isShuttingDown()) {
            return Health.outOfService()
                .withDetail("state", "shutting_down")
                .build();
        } else if (!state.isStarted() && !state.isShuttingDown()) {
            return Health.down()
                .withDetail("state", "not_started")
                .build();
        } else {
            return Health.down().build();
        }
    }
}
