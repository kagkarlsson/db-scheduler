package com.github.kagkarlsson.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.github.kagkarlsson.scheduler.exceptions.DataClassMismatchException;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import java.time.Instant;
import org.junit.jupiter.api.Test;

public class ScheduledExecutionTest {

    @Test
    public void test_equals() {
        Instant now = Instant.now();
        assertEquals(createExecution("task", "1", now), createExecution("task", "1", now));
        assertNotEquals(createExecution("task", "1", now), createExecution("task2", "1", now));
        assertNotEquals(createExecution("task", "1", now), createExecution("task", "2", now));
        assertNotEquals(createExecution("task", "1", now), createExecution("task", "1", now.plusSeconds(1)));
    }

    private ScheduledExecution<Void> createExecution(String taskname, String id, Instant executionTime) {
        OneTimeTask<Integer> task = TestTasks.oneTime(taskname, Integer.class, (instance, executionContext) -> {
        });
        return new ScheduledExecution<Void>(Void.class, new Execution(executionTime, task.instance(id)));
    }

    @Test
    public void test_data_class_type_equals() {
        Instant now = Instant.now();
        OneTimeTask<Integer> task = TestTasks.oneTime("OneTime", Integer.class, (instance, executionContext) -> {
        });
        Execution execution = new Execution(now, task.instance("id1", new Integer(1)));

        ScheduledExecution<Integer> scheduledExecution = new ScheduledExecution<>(Integer.class, execution);
        assertEquals(new Integer(1), scheduledExecution.getData());
    }

    @Test
    public void test_data_class_type_not_equals() {
        DataClassMismatchException dataClassMismatchException = assertThrows(DataClassMismatchException.class, () -> {

            Instant now = Instant.now();
            OneTimeTask<Integer> task = TestTasks.oneTime("OneTime", Integer.class, (instance, executionContext) -> {
            });
            Execution execution = new Execution(now, task.instance("id1", new Integer(1))); // Data class is an integer

            new ScheduledExecution<>(String.class, execution).getData(); // Instantiate with incorrect type
        });

        assertEquals("Task data mismatch. Expected class : class java.lang.String, actual : class java.lang.Integer",
                dataClassMismatchException.getMessage());
    }
}
