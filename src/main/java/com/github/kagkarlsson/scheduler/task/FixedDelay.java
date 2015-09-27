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

import java.time.Duration;
import java.time.LocalDateTime;

public class FixedDelay implements Schedule {

	private final Duration duration;

	private FixedDelay(Duration duration) {
		this.duration = duration;
	}

	public static FixedDelay of(Duration duration) {
		return new FixedDelay(duration);
	}

	@Override
	public LocalDateTime getNextExecutionTime(LocalDateTime from) {
		return from.plus(duration);
	}
}
