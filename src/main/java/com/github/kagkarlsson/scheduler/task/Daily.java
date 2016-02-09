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

import java.time.*;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Daily implements Schedule {

	private final List<LocalTime> times;

	public Daily(LocalTime... times) {
		this(Arrays.asList(times));
	}

	public Daily(List<LocalTime> times) {
		if (times.size() < 1) {
			throw new IllegalArgumentException("times cannot be empty");
		}
		this.times = times.stream().sorted().collect(Collectors.toList());
	}

	@Override
	public Instant getNextExecutionTime(Instant timeDone) {
		ZoneId zone = ZoneId.systemDefault();
		LocalDate doneDate = timeDone.atZone(zone).toLocalDate();

		for (LocalTime time : times) {
			Instant nextTimeCandidate = ZonedDateTime.of(doneDate, time, zone).toInstant();
			if (nextTimeCandidate.isAfter(timeDone)) {
				return nextTimeCandidate;
			}
		}

		return ZonedDateTime.of(doneDate, times.get(0), zone).plusDays(1).toInstant();
	}

}
