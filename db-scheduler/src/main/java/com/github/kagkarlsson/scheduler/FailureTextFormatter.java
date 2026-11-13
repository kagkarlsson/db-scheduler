/*
 * Copyright (C) Gustav Karlsson
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * Creates the text stored in {@code last_failure_text} when an execution fails. Configured via
 * {@link SchedulerBuilder#failureTextFormatter(FailureTextFormatter)}.
 *
 * <p>The scheduler cuts any result longer than {@code maxLength}. Implementations that want to
 * control what is kept should stay within it.
 */
@NullMarked
@FunctionalInterface
public interface FailureTextFormatter {

  /**
   * Stack trace, trimming frames per exception to fit, but keeping every exception in the chain.
   */
  FailureTextFormatter STACK_TRACE = new StackTraceFailureTextFormatter(true);

  /** Only the exception and its causes, one line each, e.g. {@code java.io.IOException: msg}. */
  FailureTextFormatter MESSAGE_ONLY = new StackTraceFailureTextFormatter(false);

  /** Store nothing. */
  FailureTextFormatter NONE = (cause, maxLength) -> null;

  /**
   * @return the text to store, or null to store nothing
   */
  @Nullable String format(Throwable cause, int maxLength);
}
