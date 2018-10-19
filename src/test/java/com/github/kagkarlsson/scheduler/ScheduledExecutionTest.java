package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.assertEquals;

public class ScheduledExecutionTest {

    @Test
    public void test_data_class_type_equals() {
        Instant now = Instant.now();
        OneTimeTask<Integer> task = TestTasks.oneTime("OneTime", Integer.class, (instance, executionContext) -> {});
        Execution execution = new Execution(now, task.instance("id1", new Integer(1)));

        ScheduledExecution<Integer> scheduledExecution = new ScheduledExecution<>(Integer.class, execution);
        assertEquals(new Integer(1), scheduledExecution.getData());
    }

    @Test(expected = ScheduledExecution.DataClassMismatchException.class)
    public void test_data_class_type_not_equals() {
        Instant now = Instant.now();
        OneTimeTask<Integer> task = TestTasks.oneTime("OneTime", Integer.class, (instance, executionContext) -> {});
        Execution execution = new Execution(now, task.instance("id1", new Integer(1))); // Data class is an integer

        new ScheduledExecution<>(String.class, execution).getData(); // Instantiate with incorrect type
    }
}
