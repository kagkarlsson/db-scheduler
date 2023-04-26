/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler.task.schedule;

import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

public class FixedDelay implements Schedule, Serializable {

    private static final long serialVersionUID = 1L;

    private final Duration duration;

    private FixedDelay() { // For serializers
        duration = null;
    }

    private FixedDelay(Duration duration) {
        this.duration = Objects.requireNonNull(duration);
    }

    public static FixedDelay of(Duration duration) {
        return new FixedDelay(duration);
    }

    public static FixedDelay ofMillis(long millis) {
        validateDuration(millis);
        return new FixedDelay(Duration.ofMillis(millis));
    }

    public static FixedDelay ofSeconds(int seconds) {
        validateDuration(seconds);
        return new FixedDelay(Duration.ofSeconds(seconds));
    }

    public static FixedDelay ofMinutes(int minutes) {
        validateDuration(minutes);
        return new FixedDelay(Duration.ofMinutes(minutes));
    }

    public static FixedDelay ofHours(int hours) {
        validateDuration(hours);
        return new FixedDelay(Duration.ofHours(hours));
    }

    private static void validateDuration(long seconds) {
        if (seconds <= 0) {
            throw new IllegalArgumentException("argument must be greater than 0");
        }
    }

    @Override
    public Instant getNextExecutionTime(ExecutionComplete executionComplete) {
        return executionComplete.getTimeDone().plus(duration);
    }

    @Override
    public Instant getInitialExecutionTime(Instant now) {
        return now;
    }

    @Override
    public boolean isDeterministic() {
        // Only deterministic if relative to a certain instant
        return false;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof FixedDelay))
            return false;
        FixedDelay that = (FixedDelay) o;
        return Objects.equals(this.duration, that.duration);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(duration);
    }

    @Override
    public String toString() {
        return "FixedDelay duration=" + duration;
    }
}
