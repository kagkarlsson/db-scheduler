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
package com.github.kagkarlsson.examples.boot.config;

import com.github.kagkarlsson.examples.boot.ExampleContext;
import com.github.kagkarlsson.scheduler.task.CompletionHandler;
import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskWithDataDescriptor;
import com.github.kagkarlsson.scheduler.task.helper.CustomTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import utils.EventLogger;

@Configuration
public class LongRunningJobConfiguration {

  public static final TaskWithDataDescriptor<PrimeGeneratorState> LONG_RUNNING_TASK =
      new TaskWithDataDescriptor<>("long-running-task", PrimeGeneratorState.class);

  /** Start the example */
  public static void start(ExampleContext ctx) {
    ctx.log(
        "Scheduling long-running task "
            + LONG_RUNNING_TASK.getTaskName()
            + " to run 3s at a time until it "
            + "has found all prime-numbers smaller than 1.000.000.");

    PrimeGeneratorState initialState = new PrimeGeneratorState(0, 0);
    ctx.schedulerClient.schedule(
        LONG_RUNNING_TASK.instance("prime-generator", initialState), Instant.now());
  }

  /** Bean definition */
  @Bean
  public CustomTask<PrimeGeneratorState> longRunningTask() {
    return Tasks.custom(LONG_RUNNING_TASK)
        .execute(
            (TaskInstance<PrimeGeneratorState> taskInstance, ExecutionContext executionContext) -> {
              EventLogger.logTask(
                  LONG_RUNNING_TASK,
                  "Continuing prime-number generation from: " + taskInstance.getData());

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

              // Make the decision on whether to reschedule at a later time or terminate/remove
              if (currentNumber > 1_000_000) {
                // lets say 1M is the end-condition for our long-running task
                return new CompletionHandler.OnCompleteRemove<>();
              } else {
                // save state and reschedule when conditions do not allow the execution to run
                // anymore
                PrimeGeneratorState stateToSave =
                    new PrimeGeneratorState(lastFoundPrime, currentNumber);
                EventLogger.logTask(
                    LONG_RUNNING_TASK,
                    "Ran for 3s. Saving state for next run. Current state: " + stateToSave);
                return new CompletionHandler.OnCompleteReschedule<>(
                    Schedules.fixedDelay(Duration.ofSeconds(10)), stateToSave);
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
      return "PrimeGeneratorState{"
          + "lastFoundPrime="
          + lastFoundPrime
          + ", lastTestedNumber="
          + lastTestedNumber
          + '}';
    }
  }
}
