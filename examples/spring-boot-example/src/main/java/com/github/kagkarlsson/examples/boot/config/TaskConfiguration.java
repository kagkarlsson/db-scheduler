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

import static com.github.kagkarlsson.scheduler.task.schedule.Schedules.fixedDelay;

import com.github.kagkarlsson.examples.boot.CounterService;
import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.boot.config.DbSchedulerCustomizer;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import java.time.Duration;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TaskConfiguration {
    private static final Logger log = LoggerFactory.getLogger(TaskConfiguration.class);

    /**
     * Define a recurring task with a dependency, which will automatically be picked up by the
     * Spring Boot autoconfiguration.
     */
    @Bean
    Task<Void> recurringSampleTask(CounterService counter) {
        return Tasks
            .recurring("recurring-sample-task", fixedDelay(Duration.ofMinutes(1)))
            .execute((instance, ctx) -> {
                log.info("Running recurring-simple-task. Instance: {}, ctx: {}", instance, ctx);
                counter.increase();
            });
    }

    /**
     * Define a one-time task which have to be manually scheduled.
     */
    @Bean
    Task<Void> sampleOneTimeTask() {
        return Tasks.oneTime("sample-one-time-task")
            .execute((instance, ctx) -> {
                log.info("I am a one-time task!");
            });
    }

    /**
     * Bean defined when a configuration-property in DbSchedulerCustomizer needs to be overridden.
     */
    @Bean
    DbSchedulerCustomizer customizer() {
        return new DbSchedulerCustomizer() {
            @Override
            public Optional<SchedulerName> schedulerName() {
                return Optional.of(new SchedulerName.Fixed("spring-boot-scheduler-1"));
            }
        };
    }
}
