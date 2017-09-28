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
package com.github.kagkarlsson.scheduler.task;

import java.time.Instant;
import java.util.Optional;

public class ExecutionComplete {
	private final Execution execution;
	private final Instant timeDone;
	private final Result result;
	private final Throwable cause;

	public ExecutionComplete(Execution execution, Instant timeDone, Result result, Throwable cause) {
		this.cause = cause;
		if (result == Result.OK && cause != null) {
			throw new IllegalArgumentException("Result 'OK' should never have a cause.");
		}
		this.execution = execution;
		this.timeDone = timeDone;
		this.result = result;
	}

	public Execution getExecution() {
		return execution;
	}

	public Instant getTimeDone() {
		return timeDone;
	}

	public Result getResult() {
		return result;
	}

	public Optional<Throwable> getCause() {
		return Optional.ofNullable(cause);
	}

	public enum Result {
		OK,
		FAILED
	}
}
