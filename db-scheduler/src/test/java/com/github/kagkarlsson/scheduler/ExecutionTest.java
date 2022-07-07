package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions.*;


public class ExecutionTest {

    @Test
    public void test_equals() {
        Instant now = Instant.now();
        OneTimeTask<Void> task = TestTasks.oneTime("OneTime", Void.class, (instance, executionContext) -> CompletableFuture.completedFuture(null));
        RecurringTask<Void> task2 = TestTasks.recurring("Recurring", FixedDelay.of(Duration.ofHours(1)), TestTasks.DO_NOTHING);

        assertEquals(new Execution(now, task.instance("id1")), new Execution(now, task.instance("id1")));
        assertNotEquals(new Execution(now, task.instance("id1")), new Execution(now.plus(Duration.ofMinutes(1)), task.instance("id1")));
        assertNotEquals(new Execution(now, task.instance("id1")), new Execution(now, task.instance("id2")));

        assertEquals(new Execution(now, task2.instance("id1")), new Execution(now, task2.instance("id1")));
        assertNotEquals(new Execution(now, task2.instance("id1")), new Execution(now.plus(Duration.ofMinutes(1)), task2.instance("id1")));
        assertNotEquals(new Execution(now, task2.instance("id1")), new Execution(now, task2.instance("id2")));

        assertNotEquals(new Execution(now, task.instance("id1")), new Execution(now, task2.instance("id1")));
    }
}
