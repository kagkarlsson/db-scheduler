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

import com.github.kagkarlsson.scheduler.CurrentlyExecuting;
import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.boot.config.DbSchedulerCustomizer;
import com.github.kagkarlsson.scheduler.event.AbstractSchedulerListener;
import com.github.kagkarlsson.scheduler.event.SchedulerListener;
import com.github.kagkarlsson.scheduler.serializer.Jackson3Serializer;
import com.github.kagkarlsson.scheduler.serializer.Serializer;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SchedulerConfiguration {

  private static final String MDC_TASK_NAME = "task-name";
  private static final String MDC_TASK_INSTANCE_ID = "task-instance-id";
  private static final Logger LOG = LoggerFactory.getLogger(SchedulerConfiguration.class);

  /** Bean defined when a configuration-property in DbSchedulerCustomizer needs to be overridden. */
  @Bean
  DbSchedulerCustomizer customizer() {
    return new DbSchedulerCustomizer() {
      @Override
      public Optional<SchedulerName> schedulerName() {
        return Optional.of(new SchedulerName.Fixed("spring-boot-scheduler-1"));
      }

      @Override
      public Optional<Serializer> serializer() {
        return Optional.of(new Jackson3Serializer());
      }
    };
  }

  @Bean
  SchedulerListener mdcSchedulerListener() {
    return new AbstractSchedulerListener() {
      @Override
      public void onExecutionStart(CurrentlyExecuting executing) {
        TaskInstance<?> taskInstance = executing.getTaskInstance();
        MDC.put(MDC_TASK_NAME, taskInstance.getTaskName());
        MDC.put(MDC_TASK_INSTANCE_ID, taskInstance.getId());
      }

      @Override
      public void onExecutionComplete(ExecutionComplete executionComplete) {
        MDC.remove(MDC_TASK_NAME);
        MDC.remove(MDC_TASK_INSTANCE_ID);
      }
    };
  }
}
