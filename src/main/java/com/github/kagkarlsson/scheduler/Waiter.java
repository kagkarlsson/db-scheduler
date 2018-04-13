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

import java.time.Duration;
import java.time.Instant;

public class Waiter {
	private Object lock = new Object();
	private final Duration duration;
	private Clock clock;

	public Waiter(Duration duration) {
		this(duration, new SystemClock());
	}

	public Waiter(Duration duration, Clock clock) {
		this.duration = duration;
		this.clock = clock;
	}

	public void doWait() throws InterruptedException {
		final long millis = duration.toMillis();
		if (millis > 0) {
			Instant waitUntil = clock.now().plusMillis(millis);

			while (Instant.now().isBefore(waitUntil)) {
				lock.wait(millis);
			}
		}
	}

	public void wake() {
		lock.notify();
	}

}
