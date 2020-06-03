package com.github.kagkarlsson.scheduler;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DueExecutionsBatchTest {

    private static final int HAPPY_THREADPOOL_SIZE = 10;
    private static final int HAPPY_GENERATION_NUMBER = 10;
    private static final int HAPPY_NUMBER_ADDED_LAST_TIME = 20;
    private static final boolean HAPPY_LIKELY_MORE_IN_DB = true;


    @Test
    public void test_trigger_check_for_more_due() {
        assertTrigger(0, 0, newBatch(HAPPY_THREADPOOL_SIZE, HAPPY_NUMBER_ADDED_LAST_TIME, HAPPY_LIKELY_MORE_IN_DB));
        assertTrigger(0, 10, newBatch(HAPPY_THREADPOOL_SIZE, HAPPY_NUMBER_ADDED_LAST_TIME, HAPPY_LIKELY_MORE_IN_DB));
        assertTrigger(0, 14, newBatch(HAPPY_THREADPOOL_SIZE, HAPPY_NUMBER_ADDED_LAST_TIME, HAPPY_LIKELY_MORE_IN_DB));
        assertTrigger(1, 15, newBatch(HAPPY_THREADPOOL_SIZE, HAPPY_NUMBER_ADDED_LAST_TIME, HAPPY_LIKELY_MORE_IN_DB));
        assertTrigger(1, HAPPY_NUMBER_ADDED_LAST_TIME, newBatch(HAPPY_THREADPOOL_SIZE, HAPPY_NUMBER_ADDED_LAST_TIME, HAPPY_LIKELY_MORE_IN_DB));

        // Db-query did not return items==limit
        assertTrigger(0, HAPPY_NUMBER_ADDED_LAST_TIME, newBatch(HAPPY_THREADPOOL_SIZE, HAPPY_NUMBER_ADDED_LAST_TIME, false));

        // Stale batch, Scheduler has already triggered a new executeDue
        assertTrigger(0, HAPPY_NUMBER_ADDED_LAST_TIME, staleBatch(HAPPY_THREADPOOL_SIZE, HAPPY_NUMBER_ADDED_LAST_TIME, HAPPY_LIKELY_MORE_IN_DB));
    }

    private void assertTrigger(int timesTriggered, int afterExeutionsHandled, DueExecutionsBatch batch) {
        AtomicInteger triggered = new AtomicInteger(0);
        IntStream.range(0, afterExeutionsHandled).forEach(val -> batch.oneExecutionDone(() -> {
            triggered.incrementAndGet();
            return true;
        }));
        assertEquals(timesTriggered, triggered.get());
    }

    private DueExecutionsBatch newBatch(int schedulerThreadpoolSize, int numberAddedFromLastDbQuery, boolean likelyMoreDueInDb) {
        DueExecutionsBatch batch = new DueExecutionsBatch(schedulerThreadpoolSize, HAPPY_GENERATION_NUMBER, numberAddedFromLastDbQuery, likelyMoreDueInDb, leftInBatch -> leftInBatch <= schedulerThreadpoolSize * 0.5);
        return batch;
    }

    private DueExecutionsBatch staleBatch(int schedulerThreadpoolSize, int numberAddedFromLastDbQuery, boolean likelyMoreDueInDb) {
        DueExecutionsBatch batch = newBatch(schedulerThreadpoolSize, numberAddedFromLastDbQuery, likelyMoreDueInDb);
        batch.markBatchAsStale();
        return batch;
    }

}
