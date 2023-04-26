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
package com.github.kagkarlsson.scheduler.serializer.jackson;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.github.kagkarlsson.scheduler.task.schedule.CronSchedule;
import com.github.kagkarlsson.scheduler.task.schedule.Daily;
import com.github.kagkarlsson.scheduler.task.schedule.FixedDelay;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = CronSchedule.class, name = "cron"),
        @JsonSubTypes.Type(value = FixedDelay.class, name = "fixedDelay"),
        @JsonSubTypes.Type(value = Daily.class, name = "daily") })
public abstract class ScheduleMixin {
    @JsonIgnore
    abstract boolean isDeterministic();

    @JsonIgnore
    abstract boolean isDisabled();
}
