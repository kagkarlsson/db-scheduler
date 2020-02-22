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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

class NotThrowingParser implements Parser {
    private static final Logger LOG = LoggerFactory.getLogger(NotThrowingParser.class);
    private final Parser delegate;

    private NotThrowingParser(Parser delegate) {
        this.delegate = Objects.requireNonNull(delegate, "Delegate parser cannot be null");
    }

    @Override
    public Optional<Schedule> parse(String scheduleString) {
        try {
            return delegate.parse(scheduleString);
        } catch (Exception e) {
            LOG.warn(
                "Unable to parse {} with {}. Exception: {}",
                scheduleString,
                delegate.getClass().getSimpleName(),
                e.getMessage()
            );
            return Optional.empty();
        }
    }

    @Override
    public List<String> examples() {
        return delegate.examples();
    }

    static NotThrowingParser of(Parser parser) {
        return new NotThrowingParser(parser);
    }
}
