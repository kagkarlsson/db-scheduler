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

import java.util.Collections;
import java.util.List;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;

final class FixedDelayParser extends RegexBasedParser {
    private static final Pattern FIXED_DELAY_PATTERN = Pattern.compile("^FIXED_DELAY\\|(\\d+)s$");
    private static final List<String> EXAMPLES = Collections.singletonList("FIXED_DELAY|120s");

    FixedDelayParser() {
        super(FIXED_DELAY_PATTERN, EXAMPLES);
    }

    @Override
    protected Schedule matchedSchedule(MatchResult matchResult) {
        return FixedDelay.ofSeconds(Integer.parseInt(matchResult.group(1)));
    }
}
