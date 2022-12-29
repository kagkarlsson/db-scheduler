package com.github.kagkarlsson.examples.boot.config;

import com.github.kagkarlsson.examples.boot.config.TaskChainingConfiguration.JobState;
import com.github.kagkarlsson.scheduler.task.TaskWithoutDataDescriptor;
import com.github.kagkarlsson.scheduler.task.TaskWithDataDescriptor;

public class TaskNames {
    public static final TaskWithoutDataDescriptor BASIC_ONE_TIME_TASK = new TaskWithoutDataDescriptor("sample-one-time-task");
    public static final TaskWithoutDataDescriptor BASIC_RECURRING_TASK = new TaskWithoutDataDescriptor("recurring-sample-task");

    public static final TaskWithoutDataDescriptor TRANSACTIONALLY_STAGED_TASK = new TaskWithoutDataDescriptor("transactionally-staged-task");

    public static final TaskWithDataDescriptor<JobState> CHAINED_STEP_1_TASK = new TaskWithDataDescriptor<>("chained-step-1", JobState.class);
    public static final TaskWithDataDescriptor<JobState> CHAINED_STEP_2_TASK = new TaskWithDataDescriptor<>("chained-step-2", JobState.class);
    public static final TaskWithDataDescriptor<JobState> CHAINED_STEP_3_TASK = new TaskWithDataDescriptor<>("chained-step-3", JobState.class);

    public static final TaskWithoutDataDescriptor PARALLEL_JOB_SPAWNER = new TaskWithoutDataDescriptor("parallel-job-spawner");
    public static final TaskWithDataDescriptor<Integer> PARALLEL_JOB = new TaskWithDataDescriptor<>("parallel-job", Integer.class);
}
