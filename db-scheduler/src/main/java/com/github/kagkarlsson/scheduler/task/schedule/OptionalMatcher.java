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

import java.util.Objects;
import java.util.Optional;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OptionalMatcher {
    private final Pattern pattern;

    private OptionalMatcher(Pattern pattern) {
        this.pattern = Objects.requireNonNull(pattern, "A non null pattern must be specified");
    }

    public Optional<MatchResult> match(String str) {
        return Optional.ofNullable(str)
            .map(pattern::matcher)
            .flatMap(this::maybeMatchingResult);
    }

    private Optional<MatchResult> maybeMatchingResult(Matcher matcher) {
        return matcher.matches()
            ? Optional.of(matcher)
            : Optional.empty();
    }

    public static OptionalMatcher from(Pattern pattern) {
        return new OptionalMatcher(pattern);
    }
}
