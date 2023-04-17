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
package com.github.kagkarlsson.scheduler.boot.config;

import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.jdbc.JdbcCustomization;
import com.github.kagkarlsson.scheduler.serializer.Serializer;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

/**
 * Provides functionality for customizing various aspects of the db-scheduler configuration that
 * is not easily done with properties.
 */
public interface DbSchedulerCustomizer {
    /**
     * Provide a custom {@link SchedulerName} implementation.
     */
    default Optional<SchedulerName> schedulerName() {
        return Optional.empty();
    }

    /**
     * A custom serializer for task data.
     */
    default Optional<Serializer> serializer() {
        return Optional.empty();
    }

    /**
     * Provide an existing {@link ExecutorService} instance.
     */
    default Optional<ExecutorService> executorService() {
        return Optional.empty();
    }

    /**
     * Provide a custom JdbcCustomization.
     */
    default Optional<JdbcCustomization> jdbcCustomization() {
        return Optional.empty();
    }
}
