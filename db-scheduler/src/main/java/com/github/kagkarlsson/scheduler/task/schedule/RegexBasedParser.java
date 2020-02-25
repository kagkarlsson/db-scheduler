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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class RegexBasedParser implements Parser {
    private final Pattern pattern;
    private final List<String> examples;

    public RegexBasedParser(Pattern pattern, List<String> examples) {
        this.pattern = Objects.requireNonNull(pattern);
        this.examples = new ArrayList<>(examples);
    }

    @Override
    public Optional<Schedule> parse(String scheduleString) {
        if (scheduleString == null) {
            return Optional.empty();
        }
        Matcher m = pattern.matcher(scheduleString);
        if (m.matches()) {
            return Optional.of(matchedSchedule(m));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public List<String> examples() {
        return Collections.unmodifiableList(examples);
    }

    protected abstract Schedule matchedSchedule(MatchResult result);
}
