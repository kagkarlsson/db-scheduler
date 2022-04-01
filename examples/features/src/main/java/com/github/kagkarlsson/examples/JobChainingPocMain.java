package com.github.kagkarlsson.examples;

import com.github.kagkarlsson.examples.helpers.Example;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.logging.LogLevel;
import com.github.kagkarlsson.scheduler.task.CompletionHandler;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.ExecutionOperations;
import com.github.kagkarlsson.scheduler.task.helper.CustomTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;

import javax.sql.DataSource;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;

public class JobChainingPocMain extends Example {

    public static void main(String[] args) {
        new JobChainingPocMain().runWithDatasource();
    }

    @Override
    public void run(DataSource dataSource) {

        final CustomTask<JobState> chainingTask = Tasks.custom("job-chain-poc", JobState.class)
            .execute((taskInstance, executionContext) -> {

                // For illustration, can be made less verbose using suitable abstractions etc
                if (taskInstance.getData().currentStep == Step.STEP1) {
                    System.out.println("Step1 ran. Job: " + taskInstance.getData());
                    return (executionComplete, executionOperations) -> {
                        JobState nextJobState = taskInstance.getData().nextStep(Step.STEP2);
                        executionOperations.reschedule(executionComplete, Instant.now(), nextJobState);
                        // Feature: Optionally expose a method executionContext.triggerCheckForDueExecutions() to hint at immediate execution
                    };

                } else if (taskInstance.getData().currentStep == Step.STEP2) {
                    System.out.println("Step2 ran. Job: " + taskInstance.getData());
                    return (executionComplete, executionOperations) -> {
                        JobState nextJobState = taskInstance.getData().nextStep(Step.STEP3);
                        executionOperations.reschedule(executionComplete, Instant.now(), nextJobState);
                    };

                } else if (taskInstance.getData().currentStep == Step.STEP3) {
                    System.out.println("Step3 ran. Removing multistep-job. Job: " + taskInstance.getData());
                    return new CompletionHandler.OnCompleteRemove<>();

                } else {
                    throw new RuntimeException("Unknown step: " + taskInstance.getData());
                }
            });

        final Scheduler scheduler = Scheduler
            .create(dataSource, chainingTask)
            .enableImmediateExecution() // Bug: currently no effect, only works when using schedulerClient
            .pollingInterval(Duration.ofSeconds(1))
            .build();

        scheduler.start();

        sleep(1_000);

        // Schedule a multistep job. Simulate some instance-specific data, id=507
        scheduler.schedule(chainingTask.instance("job-507", JobState.newJob(507)), Instant.now().plusSeconds(1));
    }

    public enum Step {STEP1, STEP2, STEP3}

    public static class JobState implements Serializable {
        private static long serialVersionUID = 1L;
        public Step currentStep;
        public int id;

        public JobState(int id, Step currentStep) {
            this.id = id;
            this.currentStep = currentStep;
        }

        public static JobState newJob(int id) {
            return new JobState(id, Step.STEP1);
        }

        public JobState nextStep(Step nextStep) {
            return new JobState(id, nextStep);
        }

        @Override
        public String toString() {
            return "JobState{" +
                "currentStep=" + currentStep +
                ", id=" + id +
                '}';
        }
    }
}
