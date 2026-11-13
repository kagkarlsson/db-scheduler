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

import com.github.kagkarlsson.scheduler.task.Execution;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates the text stored in {@code last_failure_text}, never longer than {@code maxLength}. */
@NullMarked
final class FailureTexts {
  private static final Logger LOG = LoggerFactory.getLogger(FailureTexts.class);
  static final String TRUNCATED_MARKER = "\n...[truncated]";

  private final FailureTextFormatter formatter;
  private final int maxLength;

  FailureTexts(FailureTextFormatter formatter, int maxLength) {
    if (maxLength < TRUNCATED_MARKER.length()) {
      throw new IllegalArgumentException(
          "Max length for failure text must be at least " + TRUNCATED_MARKER.length());
    }
    this.formatter = formatter;
    this.maxLength = maxLength;
  }

  @Nullable String format(Throwable cause) {
    try {
      return truncate(formatter.format(cause, maxLength), maxLength);
    } catch (RuntimeException e) {
      // not cause.toString(), since getMessage() may be what failed
      LOG.warn("FailureTextFormatter failed. Storing exception class instead.", e);
      return cause.getClass().getName();
    }
  }

  /** Written regardless of formatter, since it contains no data from the task. */
  String deadExecution(Execution execution, String detectedBy) {
    return truncate(
        "Dead execution: picked by '"
            + execution.pickedBy
            + "', last heartbeat "
            + execution.lastHeartbeat
            + " (detected by '"
            + detectedBy
            + "')",
        maxLength);
  }

  static @Nullable String truncate(@Nullable String text, int maxLength) {
    if (text == null) {
      return null;
    }
    // PostgreSQL rejects NUL in text columns
    text = text.replace('\u0000', '?');
    if (text.length() <= maxLength) {
      return text;
    }
    int cut = maxLength - TRUNCATED_MARKER.length();
    if (cut > 0 && Character.isHighSurrogate(text.charAt(cut - 1))) {
      cut--;
    }
    return text.substring(0, cut) + TRUNCATED_MARKER;
  }
}
