/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.scheduler.task.Execution;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface TaskRepository<T> {

	boolean createIfNotExists(Execution<T> execution);
	List<Execution<T>> getDue(Instant now);

	void remove(Execution<T> execution);
	void reschedule(Execution<T> execution, Instant nextExecutionTime, Instant lastSuccess, Instant lastFailure);

	Optional<Execution<T>> pick(Execution<T> e, Instant timePicked);

	List<Execution<T>> getOldExecutions(Instant olderThan);

	void updateHeartbeat(Execution<T> execution, Instant heartbeatTime);

	List<Execution<T>> getExecutionsFailingLongerThan(Duration interval);
}
