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

import com.github.kagkarlsson.scheduler.task.ExecutionContext;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.PlainScheduleAndData;
import com.github.kagkarlsson.scheduler.task.helper.ScheduleAndData;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.CronSchedule;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;
import utils.EventLogger;
import utils.Utils;

import java.io.Serializable;
import java.time.Instant;
import java.util.Random;

import static com.github.kagkarlsson.examples.boot.config.TaskNames.*;

@Configuration
public class MultiInstanceRecurringConfiguration {

    @Bean
    public Task<ScheduleAndCustomer> multiInstanceRecurring() {
        return Tasks.recurringWithPersistentSchedule(MULTI_INSTANCE_RECURRING_TASK)
            .execute((TaskInstance<ScheduleAndCustomer> taskInstance, ExecutionContext executionContext) -> {

                ScheduleAndCustomer data = taskInstance.getData();
                EventLogger.logTask(MULTI_INSTANCE_RECURRING_TASK,
                    String.format("Ran according to schedule '%s' for customer %s", data.getSchedule(), data.getData()));
            });
    }

    public static class ScheduleAndCustomer implements ScheduleAndData {
        private static final long serialVersionUID = 1L; // recommended when using Java serialization
        private final CronSchedule schedule;
        private final Customer customer;

        private ScheduleAndCustomer(){ this(null, null);} //
        public ScheduleAndCustomer(CronSchedule schedule, Customer customer) {
            this.schedule = schedule;
            this.customer = customer;
        }

        @Override
        public CronSchedule getSchedule() {
            return schedule;
        }

        @Override
        public Customer getData() {
            return customer;
        }

        @Override
        public String toString() {
            return "ScheduleAndCustomer{" +
                "schedule=" + schedule +
                ", customer=" + customer +
                '}';
        }
    }

    public static class Customer implements Serializable {
        private static final long serialVersionUID = 1L; // recommended when using Java serialization
        public final String id;

        private Customer() {this(null);}
        public Customer(String id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return "Customer{" +
                "id='" + id + '\'' +
                '}';
        }
    }
}
