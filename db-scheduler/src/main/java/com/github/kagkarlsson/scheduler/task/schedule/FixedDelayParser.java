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

import com.github.kagkarlsson.scheduler.task.schedule.Schedules.UnrecognizableSchedule;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class FixedDelayParser implements Parser {
    private static final Pattern FIXED_DELAY_PATTERN = Pattern.compile("^FIXED_DELAY\\|(\\d+)s$");
    private static final List<String> EXAMPLES = Collections.singletonList("FIXED_DELAY|120s");

    @Override
    public Schedule parse(String scheduleString) {
        return FixedDelay.ofSeconds(
            match(scheduleString).delayInSeconds()
        );
    }

    @Override
    public List<String> examples() {
        return EXAMPLES;
    }

    private MatchedSchedule match(String scheduleString) {
        return Optional.ofNullable(scheduleString)
            .map(FIXED_DELAY_PATTERN::matcher)
            .filter(Matcher::matches)
            .map(MatchedSchedule::new)
            .orElseThrow(() -> new UnrecognizableSchedule(scheduleString, this.examples()));
    }

    private static class MatchedSchedule {
        private final Matcher matcher;

        private MatchedSchedule(Matcher matcher) {
            this.matcher = matcher;
        }

        int delayInSeconds() {
            return Integer.parseInt(matcher.group(1));
        }
    }
}
