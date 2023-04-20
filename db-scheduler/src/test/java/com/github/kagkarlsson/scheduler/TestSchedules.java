package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import java.time.Instant;

public class TestSchedules {

    public static Schedule now() {
        return new Schedule() {
            @Override
            public Instant getNextExecutionTime(ExecutionComplete executionComplete) {
                return executionComplete.getTimeDone();
            }

            @Override
            public boolean isDeterministic() {
                return false;
            }
        };
    }
}
