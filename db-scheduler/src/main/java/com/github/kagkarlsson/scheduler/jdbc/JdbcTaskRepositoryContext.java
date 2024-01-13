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
package com.github.kagkarlsson.scheduler.jdbc;

import com.github.kagkarlsson.jdbc.JdbcRunner;
import com.github.kagkarlsson.jdbc.ResultSetMapper;
import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.TaskResolver;
import com.github.kagkarlsson.scheduler.task.Execution;
import java.util.List;
import java.util.function.Supplier;

public class JdbcTaskRepositoryContext {
  public final TaskResolver taskResolver;
  public final String tableName;
  public final SchedulerName schedulerName;
  public final JdbcRunner jdbcRunner;
  public final Supplier<ResultSetMapper<List<Execution>>> resultSetMapper;

  JdbcTaskRepositoryContext(
      TaskResolver taskResolver,
      String tableName,
      SchedulerName schedulerName,
      JdbcRunner jdbcRunner,
      Supplier<ResultSetMapper<List<Execution>>> resultSetMapper) {
    this.taskResolver = taskResolver;
    this.tableName = tableName;
    this.schedulerName = schedulerName;
    this.jdbcRunner = jdbcRunner;
    this.resultSetMapper = resultSetMapper;
  }
}
