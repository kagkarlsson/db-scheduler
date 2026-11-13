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

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import org.jspecify.annotations.NullMarked;

/**
 * Formats like {@link Throwable#printStackTrace()}, but when the result is longer than {@code
 * maxLength}, keeps fewer frames for each exception instead of cutting the end, which would lose
 * the root cause. Suppressed exceptions are listed without frames.
 */
@NullMarked
final class StackTraceFailureTextFormatter implements FailureTextFormatter {

  private final boolean includeFrames;

  StackTraceFailureTextFormatter(boolean includeFrames) {
    this.includeFrames = includeFrames;
  }

  @Override
  public String format(Throwable cause, int maxLength) {
    var chain = chain(cause);
    if (!includeFrames) {
      return render(chain, 0);
    }
    int maxFrames = chain.stream().mapToInt(t -> t.uniqueFrames).max().orElse(0);
    var full = render(chain, maxFrames);
    if (full.length() <= maxLength) {
      return full;
    }
    // length grows with frames per exception, so find the largest number that fits. With 0 frames
    // only the exception lines remain; the scheduler cuts the end if even that is too long.
    int low = 0;
    int high = maxFrames - 1;
    while (low < high) {
      int mid = (low + high + 1) / 2;
      if (render(chain, mid).length() <= maxLength) {
        low = mid;
      } else {
        high = mid - 1;
      }
    }
    return render(chain, low);
  }

  private static String render(List<ChainedThrowable> chain, int maxFramesPerThrowable) {
    var sb = new StringBuilder();
    for (int i = 0; i < chain.size(); i++) {
      var t = chain.get(i);
      if (i > 0) {
        sb.append("\nCaused by: ");
      }
      sb.append(t.throwable);

      var frames = t.throwable.getStackTrace();
      int shown = Math.min(maxFramesPerThrowable, t.uniqueFrames);
      for (int f = 0; f < shown; f++) {
        sb.append("\n\tat ").append(frames[f]);
      }
      if (maxFramesPerThrowable > 0 && frames.length > shown) {
        sb.append("\n\t... ").append(frames.length - shown).append(" more");
      }
      for (Throwable suppressed : t.throwable.getSuppressed()) {
        sb.append("\n\tSuppressed: ").append(suppressed);
      }
    }
    return sb.toString();
  }

  private static List<ChainedThrowable> chain(Throwable cause) {
    Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    var chain = new ArrayList<ChainedThrowable>();
    StackTraceElement[] enclosing = new StackTraceElement[0];
    for (Throwable t = cause; t != null && seen.add(t); t = t.getCause()) {
      var frames = t.getStackTrace();
      chain.add(new ChainedThrowable(t, frames.length - framesInCommon(frames, enclosing)));
      enclosing = frames;
    }
    return chain;
  }

  // same as printStackTrace: frames shared with the enclosing exception are not repeated
  private static int framesInCommon(StackTraceElement[] frames, StackTraceElement[] enclosing) {
    int m = frames.length - 1;
    int n = enclosing.length - 1;
    while (m >= 0 && n >= 0 && frames[m].equals(enclosing[n])) {
      m--;
      n--;
    }
    return frames.length - 1 - m;
  }

  private record ChainedThrowable(Throwable throwable, int uniqueFrames) {}
}
