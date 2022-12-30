/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.examples.boot.config;

import com.github.kagkarlsson.examples.boot.config.LongRunningJobConfiguration.PrimeGeneratorState;
import com.github.kagkarlsson.examples.boot.config.JobChainingConfiguration.JobState;
import com.github.kagkarlsson.examples.boot.config.MultiInstanceRecurringConfiguration.ScheduleAndCustomer;
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

    public static final TaskWithDataDescriptor<PrimeGeneratorState> LONG_RUNNING_TASK = new TaskWithDataDescriptor<>("long-running-task", PrimeGeneratorState.class);
    public static final TaskWithDataDescriptor<ScheduleAndCustomer> MULTI_INSTANCE_RECURRING_TASK = new TaskWithDataDescriptor<>("multi-instance-recurring-task", ScheduleAndCustomer.class);
}
