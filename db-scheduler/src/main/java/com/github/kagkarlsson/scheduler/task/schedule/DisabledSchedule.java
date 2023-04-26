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
import java.time.Instant;

public class DisabledSchedule implements Schedule {

    @Override
    public Instant getNextExecutionTime(ExecutionComplete executionComplete) {
        throw unsupportedException();
    }

    @Override
    public boolean isDeterministic() {
        throw unsupportedException();
    }

    @Override
    public Instant getInitialExecutionTime(Instant now) {
        throw unsupportedException();
    }

    @Override
    public boolean isDisabled() {
        return true;
    }

    private UnsupportedOperationException unsupportedException() {
        return new UnsupportedOperationException(
                "DisabledSchedule does not support any other operations than 'isDisabled()'. This appears to be a bug.");
    }
}
