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
package com.github.kagkarlsson.scheduler.stats;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.util.Collections;
import java.util.concurrent.ExecutorService;

public class MicrometerExecutorStatsBinder implements ExecutorStatsBinder {

  public static final String CANDIDATE_EXECUTOR_NAME = "dbSchedulerExecutor";
  public static final String CANDIDATE_DUE_EXECUTOR_NAME = "dbSchedulerCandidateDueExecutor";
  public static final String HOUSEKEEPER_EXECUTOR_NAME = "dbSchedulerHousekeeperExecutor";

  private final MeterRegistry meterRegistry;

  public MicrometerExecutorStatsBinder(MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
  }

  @Override
  public void bindToRegistry(ExecutorService executor, String executorName) {
    new ExecutorServiceMetrics(executor, executorName, Collections.emptyList())
        .bindTo(meterRegistry);
  }
}
