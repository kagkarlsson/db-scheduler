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
package com.github.kagkarlsson.scheduler.boot.autoconfigure;

import com.github.kagkarlsson.scheduler.stats.MicrometerStatsRegistry;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.task.Task;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnClass({
    MetricsAutoConfiguration.class,
    CompositeMeterRegistryAutoConfiguration.class,
})
@AutoConfigureAfter({
    MetricsAutoConfiguration.class,
    CompositeMeterRegistryAutoConfiguration.class,
})
@AutoConfigureBefore(DbSchedulerAutoConfiguration.class)
@ConditionalOnProperty(value = "db-scheduler.enabled", matchIfMissing = true)
public class DbSchedulerMetricsAutoConfiguration {
    private static final Logger log = LoggerFactory.getLogger(DbSchedulerMetricsAutoConfiguration.class);
    private final List<Task<?>> configuredTasks;

    public DbSchedulerMetricsAutoConfiguration(List<Task<?>> configuredTasks) {
        this.configuredTasks = configuredTasks;
    }

    @ConditionalOnClass(MeterRegistry.class)
    @ConditionalOnBean(MeterRegistry.class)
    @ConditionalOnMissingBean(StatsRegistry.class)
    @Bean
    StatsRegistry micrometerStatsRegistry(MeterRegistry registry) {
        log.debug("Spring Boot Actuator and Micrometer detected. Will use: {} for StatsRegistry", registry.getClass().getName());
        return new MicrometerStatsRegistry(registry, configuredTasks);
    }
}

