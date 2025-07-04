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
package com.github.kagkarlsson.scheduler.exceptions;

import java.io.Serial;

public class DataClassMismatchException extends DbSchedulerException {
  @Serial private static final long serialVersionUID = 6333316294241471977L;

  public DataClassMismatchException(Class expectedClass, Class actualClass) {
    super(
        String.format(
            "Task data mismatch. If actual data-class is byte[], it might have been fetched without"
                + " knowledge of task-data types, and is thus not deserialized."
                + " Use getRawData() to get non-deserialized data in that case."
                + " Expected class : %s, actual : %s",
            expectedClass, actualClass));
  }
}
