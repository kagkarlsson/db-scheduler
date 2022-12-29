package com.github.kagkarlsson.examples.boot.config;

import com.github.kagkarlsson.scheduler.task.TaskDescriptor;
import com.github.kagkarlsson.scheduler.task.TaskWithDataDescriptor;

public class TaskNames {
    public static final String BASIC_ONE_TIME_TASK = "sample-one-time-task";
    public static final String BASIC_RECURRING_TASK = "recurring-sample-task";

    public static final String TRANSACTIONALLY_STAGED_TASK = "transactionally-staged-task";

    public static final String CHAINED_STEP_1_TASK = "chained-step-1";
    public static final String CHAINED_STEP_2_TASK = "chained-step-2";
    public static final String CHAINED_STEP_3_TASK = "chained-step-3";

    public static final TaskDescriptor PARALLEL_JOB_SPAWNER = new TaskDescriptor("parallel-job-spawner");
    public static final TaskWithDataDescriptor<Integer> PARALLEL_JOB = new TaskWithDataDescriptor<>("parallel-job", Integer.class);
}
