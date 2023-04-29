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
package com.github.kagkarlsson.scheduler.task.schedule;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

final class CompositeParser implements Parser {
  private final List<Parser> delegates;

  private CompositeParser(List<Parser> delegates) {
    this.delegates = delegates;
  }

  @Override
  public Optional<Schedule> parse(String scheduleString) {
    return delegates.stream()
        .map(it -> it.parse(scheduleString))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .findFirst();
  }

  @Override
  public List<String> examples() {
    return delegates.stream().flatMap(it -> it.examples().stream()).collect(Collectors.toList());
  }

  static CompositeParser of(Parser... parsers) {
    if (parsers == null || parsers.length == 0)
      throw new IllegalArgumentException("Unable to create CompositeParser");
    return new CompositeParser(Arrays.asList(parsers));
  }
}
