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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.DateTimeException;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class DailyParser implements Parser {
    private static final Logger LOG = LoggerFactory.getLogger(DailyParser.class);
    private static final Pattern DAILY_PATTERN_WITH_TIMEZONE = Pattern.compile("^DAILY\\|((\\d{2}:\\d{2})(,\\d{2}:\\d{2})*)(\\|(.+))?$");
    private static final List<String> EXAMPLES = Arrays.asList("DAILY|12:00", "DAILY|12:00,13:45", "DAILY|12:00,13:45|Europe/Rome");

    @Override
    public Optional<Schedule> parse(String scheduleString) {
        return OptionalMatcher.from(DAILY_PATTERN_WITH_TIMEZONE)
            .match(scheduleString)
            .map(MatchedSchedule::new)
            .flatMap(toDaily());
    }

    private Function<MatchedSchedule, Optional<Schedule>> toDaily() {
        return it -> it.hasTimeZone()
                ? it.dailyWithTimezone()
                : it.dailyWithoutTimezone();
    }

    @Override
    public List<String> examples() {
        return EXAMPLES;
    }

    private static class MatchedSchedule {
        private final MatchResult matcher;

        MatchedSchedule(MatchResult matcher) {
            this.matcher = Objects.requireNonNull(matcher);
        }

        private List<LocalTime> scheduleTimes() {
            String[] times = matcher.group(1).split(",");
            return Stream.of(times).map(LocalTime::parse).collect(Collectors.toList());
        }

        private Optional<String> maybeZoneIdStr() {
            return Optional.ofNullable(matcher.group(5));
        }

        private Optional<ZoneId> maybeCorrectZoneId(String str) {
            try {
                return Optional.of(ZoneId.of(str));
            } catch (DateTimeException exception) {
                LOG.warn("Unable to parse ZoneId {}", str, exception);
                return Optional.empty();
            }
        }

        boolean hasTimeZone() {
            return maybeZoneIdStr().isPresent();
        }

        Optional<Schedule> dailyWithTimezone() {
            return maybeZoneIdStr().flatMap(this::maybeCorrectZoneId).map(zoneId -> new Daily(zoneId, scheduleTimes()));
        }

        Optional<Schedule> dailyWithoutTimezone() {
            return Optional.of(new Daily(scheduleTimes()));
        }
    }
}
