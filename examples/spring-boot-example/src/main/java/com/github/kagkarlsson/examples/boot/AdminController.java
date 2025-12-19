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
package com.github.kagkarlsson.examples.boot;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.kagkarlsson.examples.boot.config.*;
import com.github.kagkarlsson.scheduler.SchedulerClient;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/admin")
public class AdminController {
  private static final Logger LOG = LoggerFactory.getLogger(AdminController.class);

  // Endpoints
  public static final String LIST_SCHEDULED = "/tasks";
  public static final String START = "/start";
  public static final String STOP = "/stop";

  private static final Map<String, Consumer<ExampleContext>> TASK_STARTERS = new HashMap<>();

  static {
    TASK_STARTERS.put(
        BasicExamplesConfiguration.BASIC_ONE_TIME_TASK.getTaskName(),
        BasicExamplesConfiguration::triggerOneTime);
    TASK_STARTERS.put(
        TransactionallyStagedJobConfiguration.TRANSACTIONALLY_STAGED_TASK.getTaskName(),
        TransactionallyStagedJobConfiguration::start);
    TASK_STARTERS.put(
        JobChainingConfiguration.CHAINED_STEP_1_TASK.getTaskName(),
        JobChainingConfiguration::start);
    TASK_STARTERS.put(
        ParallellJobConfiguration.PARALLEL_JOB_SPAWNER.getTaskName(),
        ParallellJobConfiguration::start);
    TASK_STARTERS.put(
        LongRunningJobConfiguration.LONG_RUNNING_TASK.getTaskName(),
        LongRunningJobConfiguration::start);
    TASK_STARTERS.put(
        MultiInstanceRecurringConfiguration.MULTI_INSTANCE_RECURRING_TASK.getTaskName(),
        MultiInstanceRecurringConfiguration::start);
    TASK_STARTERS.put(
        RecurringStateTrackingConfiguration.STATE_TRACKING_RECURRING_TASK.getTaskName(),
        RecurringStateTrackingConfiguration::start);
  }

  private final SchedulerClient schedulerClient;
  private final ExampleContext exampleContext;

  public AdminController(SchedulerClient schedulerClient, TransactionTemplate tx) {
    this.schedulerClient = schedulerClient;
    exampleContext = new ExampleContext(schedulerClient, tx, LOG);
  }

  @GetMapping(path = LIST_SCHEDULED)
  public List<Scheduled> list() {
    return schedulerClient.getScheduledExecutions().stream()
        .map(
            e -> {
              return new Scheduled(
                  e.getTaskInstance().getTaskName(),
                  e.getTaskInstance().getId(),
                  e.getExecutionTime(),
                  e.getData());
            })
        .collect(Collectors.toList());
  }

  @PostMapping(
      path = START,
      headers = {"Content-type=application/json"})
  public void start(@RequestBody StartRequest request) {
    TASK_STARTERS.get(request.taskName).accept(exampleContext);
  }

  @PostMapping(
      path = STOP,
      headers = {"Content-type=application/json"})
  public void stop(@RequestBody StartRequest request) {
    schedulerClient.getScheduledExecutions().stream()
        .filter(s -> s.getTaskInstance().getTaskName().equals(request.taskName))
        .findAny()
        .ifPresent(s -> schedulerClient.cancel(s.getTaskInstance()));
  }

  public static class StartRequest {
    public final String taskName;

    public StartRequest() {
      this("");
    }

    @JsonCreator
    public StartRequest(@JsonProperty("taskName") String taskName) {
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
