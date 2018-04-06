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
package com.github.kagkarlsson.scheduler.task.schedule;

import java.time.Duration;
import java.time.LocalTime;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Schedules {

	private static final Pattern DAILY_PATTERN = Pattern.compile("^DAILY\\|((\\d{2}:\\d{2})(,\\d{2}:\\d{2})*)$");
	private static final Pattern FIXED_DELAY_PATTERN = Pattern.compile("^FIXED_DELAY\\|(\\d+)s$");

	public static Schedule daily(LocalTime... times) {
		return new Daily(times);
	}

	public static Schedule fixedDelay(Duration delay) {
		return FixedDelay.of(delay);
	}

	/**
	 * Currently supports Daily- and FixedDelay-schedule on the formats:
	 * <pre>DAILY|hh:mm,hh:mm,...,hh:mm</pre><br/>
	 * <pre>FIXED_DELAY|xxxs  (xxx is number of seconds)</pre>
	 *
	 * @param scheduleString
	 * @return
	 */
	public static Schedule parseSchedule(String scheduleString) {
		if (scheduleString == null) throw new UnrecognizableSchedule("null");

		Matcher dailyMatcher = DAILY_PATTERN.matcher(scheduleString);
		if (dailyMatcher.matches()) {
			String[] times = dailyMatcher.group(1).split(",");
			List<LocalTime> parsedTimes = Stream.of(times).map(timeStr -> {
				String[] hourAndMinute = timeStr.split(":");
				return LocalTime.of(Integer.parseInt(hourAndMinute[0]), Integer.parseInt(hourAndMinute[1]));
			}).collect(Collectors.toList());
			return new Daily(parsedTimes);

		}

		Matcher fixedDelayMatcher = FIXED_DELAY_PATTERN.matcher(scheduleString);
		if (fixedDelayMatcher.matches()) {
			int secondsDelay = Integer.parseInt(fixedDelayMatcher.group(1));
			return FixedDelay.of(Duration.ofSeconds(secondsDelay));
		}

		throw new UnrecognizableSchedule(scheduleString);
	}

	public static class UnrecognizableSchedule extends RuntimeException {
		public UnrecognizableSchedule(String inputSchedule) {
			super("Unrecognized schedule: '"+inputSchedule+"'");
		}
	}

}
