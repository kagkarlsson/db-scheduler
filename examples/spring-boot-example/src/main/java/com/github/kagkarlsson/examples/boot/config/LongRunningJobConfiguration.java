package com.github.kagkarlsson.examples.boot.config;

import com.github.kagkarlsson.scheduler.task.CompletionHandler;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.CustomTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import utils.EventLogger;

import java.io.Serializable;
import java.time.Duration;

import static com.github.kagkarlsson.examples.boot.config.TaskNames.LONG_RUNNING_TASK;

@Configuration
public class LongRunningJobConfiguration {

    @Bean
    public CustomTask<PrimeGeneratorState> longRunningTask() {
        return Tasks.custom(LONG_RUNNING_TASK)
            .execute((TaskInstance<PrimeGeneratorState> taskInstance, ExecutionContext executionContext) -> {
                EventLogger.logTask(LONG_RUNNING_TASK, "Continuing prime-number generation from: " + taskInstance.getData());

                long currentNumber = taskInstance.getData().lastTestedNumber;
                long lastFoundPrime = taskInstance.getData().lastFoundPrime;
                long start = System.currentTimeMillis();

                // For long-running tasks, it is important to regularly check that
                // conditions allow us to continue executing
                // Here, we pretend that the execution may only run 3s straight, a more realistic
                // scenario is for example executions that may only run nightly, in the off-hours
                while (!executionContext.getSchedulerState().isShuttingDown()
                        && maxSecondsSince(start, 3)) {

                    if (isPrime(currentNumber)) {
                        lastFoundPrime = currentNumber;
                    }
                    currentNumber++;
                }

                if (currentNumber > 1_000_000) {
                    // lets say 1M is the end-condition for our long-running task
                    return new CompletionHandler.OnCompleteRemove<>();
                } else {
                    // save state and reschedule when conditions do not allow the execution to run anymore
                    PrimeGeneratorState stateToSave = new PrimeGeneratorState(lastFoundPrime, currentNumber);
                    EventLogger.logTask(LONG_RUNNING_TASK, "Ran for 3s. Saving state for next run. Current state: " + stateToSave);
                    return new CompletionHandler.OnCompleteReschedule<>(Schedules.fixedDelay(Duration.ofSeconds(10)), stateToSave);
                }
            });
    }

    private boolean maxSecondsSince(long start, long seconds) {
        return System.currentTimeMillis() - start < seconds * 1000;
    }

    private boolean isPrime(long currentNumber) {
        for (long i = 2; i < currentNumber; i++) {
            if (currentNumber % i == 0) {
                // divisible
                return false;
            }
        }
        return true;
    }

    public static class PrimeGeneratorState implements Serializable {
        private static final long serialVersionUID = 1L; // recommended when using Java serialization
        public final long lastFoundPrime;
        public final long lastTestedNumber;

        public PrimeGeneratorState() {
            this(0, 0);
        } // for serializing

        public PrimeGeneratorState(long lastFoundPrime, long lastTestedNumber) {
            this.lastFoundPrime = lastFoundPrime;
            this.lastTestedNumber = lastTestedNumber;
        }

        @Override
        public String toString() {
            return "PrimeGeneratorState{" +
                "lastFoundPrime=" + lastFoundPrime +
                ", lastTestedNumber=" + lastTestedNumber +
                '}';
        }
    }
}
