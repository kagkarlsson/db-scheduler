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
package com.github.kagkarlsson.examples.boot;

import com.github.kagkarlsson.examples.boot.config.*;
import com.github.kagkarlsson.examples.boot.config.JobChainingConfiguration.JobState;
import com.github.kagkarlsson.examples.boot.config.LongRunningJobConfiguration.PrimeGeneratorState;
import com.github.kagkarlsson.examples.boot.config.MultiInstanceRecurringConfiguration.Customer;
import com.github.kagkarlsson.examples.boot.config.MultiInstanceRecurringConfiguration.ScheduleAndCustomer;
import com.github.kagkarlsson.scheduler.ScheduledExecutionsFilter;
import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.schedule.CronSchedule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/admin")
public class AdminController {
    private static final Logger LOG = LoggerFactory.getLogger(AdminController.class);

    // Endpoints
    public static final String LIST_SCHEDULED = "/tasks";
    public static final String START = "/start";
    public static final String STOP = "/stop";
    public static final String SCHEDULE_TRANSACTIONALLY_STAGED = "/scheduleTransactionallyStaged";
    public static final String SCHEDULE_CHAINED = "/scheduleChained";
    public static final String START_PARALLEL = "/startParallel";
    public static final String SCHEDULE_LONG_RUNNING = "/scheduleLongRunning";
    public static final String SCHEDULE_MULTI_INSTANCE = "/scheduleMultiInstance";
    public static final String START_STATE_TRACKING_RECURRING = "/startStateTrackingRecurring";

    private final SchedulerClient schedulerClient;
    private final ExampleContext exampleContext;

    public AdminController(SchedulerClient schedulerClient, TransactionTemplate tx) {
        this.schedulerClient = schedulerClient;
        exampleContext = new ExampleContext(schedulerClient, tx, LOG);
    }

    @GetMapping(path = LIST_SCHEDULED)
    public List<Scheduled> list() {
        return schedulerClient.getScheduledExecutions().stream()
            .map(e -> {
                return new Scheduled(
                    e.getTaskInstance().getTaskName(),
                    e.getTaskInstance().getId(),
                    e.getExecutionTime(),
                    e.getData());
            })
            .collect(Collectors.toList());
    }

    @PostMapping(path = START, headers = {"Content-type=application/json"})
    public void start(@RequestBody StartRequest request) {

        Map<String, Runnable> taskStarterMapping = new HashMap<>();
        taskStarterMapping.put(TaskNames.BASIC_ONE_TIME_TASK.getTaskName(), () -> BasicExamplesConfiguration.triggerOneTime(exampleContext));
        taskStarterMapping.put(TaskNames.TRANSACTIONALLY_STAGED_TASK.getTaskName(), () -> TransactionallyStagedJobConfiguration.start(exampleContext));
        taskStarterMapping.put(TaskNames.CHAINED_STEP_1_TASK.getTaskName(), () -> JobChainingConfiguration.start(exampleContext));
        taskStarterMapping.put(TaskNames.PARALLEL_JOB_SPAWNER.getTaskName(), () -> ParallellJobConfiguration.start(exampleContext));
        taskStarterMapping.put(TaskNames.LONG_RUNNING_TASK.getTaskName(), () -> LongRunningJobConfiguration.start(exampleContext));
        taskStarterMapping.put(TaskNames.MULTI_INSTANCE_RECURRING_TASK.getTaskName(), () -> MultiInstanceRecurringConfiguration.start(exampleContext));
        taskStarterMapping.put(TaskNames.STATE_TRACKING_RECURRING_TASK.getTaskName(), () -> RecurringStateTrackingConfiguration.start(exampleContext));

        taskStarterMapping.get(request.taskName).run();
    }

    @PostMapping(path = STOP, headers = {"Content-type=application/json"})
    public void stop(@RequestBody StartRequest request) {
        schedulerClient.getScheduledExecutions().stream().filter(t)
    }

    public static class StartRequest {
        public final String taskName;

        public StartRequest() {
            this("");
        }

        public StartRequest(String taskName) {
            this.taskName = taskName;
        }
    }

    public static class Scheduled {
        public final String taskName;
        public final String id;
        public final Instant executionTime;
        public final Object data;

        public Scheduled() {
            this(null, null, null, null);
        }

        public Scheduled(String taskName, String id, Instant executionTime, Object data) {
            this.taskName = taskName;
            this.id = id;
            this.executionTime = executionTime;
            this.data = data;
        }
    }
}
