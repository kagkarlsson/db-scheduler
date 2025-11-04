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
package com.github.kagkarlsson.scheduler.boot.autoconfigure;

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.boot.actuator.DbSchedulerHealthIndicator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.actuate.autoconfigure.health.HealthContributorAutoConfiguration;
import org.springframework.boot.actuator.health.HealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnClass(HealthContributorAutoConfiguration.class)
@AutoConfigureAfter({
  HealthContributorAutoConfiguration.class,
  DbSchedulerAutoConfiguration.class,
})
public class DbSchedulerActuatorAutoConfiguration {
  private static final Logger log =
      LoggerFactory.getLogger(DbSchedulerActuatorAutoConfiguration.class);

  @ConditionalOnEnabledHealthIndicator("db-scheduler")
  @ConditionalOnClass(HealthIndicator.class)
  @ConditionalOnBean(Scheduler.class)
  @Bean
  public HealthIndicator dbScheduler(Scheduler scheduler) {
    log.debug("Exposing health indicator for db-scheduler");
    return new DbSchedulerHealthIndicator(scheduler);
  }
}
