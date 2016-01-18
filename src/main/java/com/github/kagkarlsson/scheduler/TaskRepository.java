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
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface TaskRepository {

	boolean createIfNotExists(Execution execution);
	List<Execution> getDue(LocalDateTime now);

	void remove(Execution execution);
	void reschedule(Execution execution, LocalDateTime nextExecutionTime, LocalDateTime lastSuccess, LocalDateTime lastFailure);

	Optional<Execution> pick(Execution e, LocalDateTime timePicked);

	List<Execution> getOldExecutions(LocalDateTime olderThan);

	void updateHeartbeat(Execution execution, LocalDateTime heartbeatTime);

	List<Execution> getExecutionsFailingLongerThan(Duration interval);
}
