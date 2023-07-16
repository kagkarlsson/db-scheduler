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
package com.github.kagkarlsson.scheduler.logging;

import org.slf4j.Logger;

public class ConfigurableLogger {

  private interface LogMethod {
    void log(String format, Object... arguments);
  }

  private static class NoOpLogger extends ConfigurableLogger {
    private NoOpLogger() {
      super(null, false);
    }

    @Override
    public void log(String format, Throwable cause, Object... arguments) {}
  }

  private final LogMethod logMethod;
  private final boolean logStackTrace;

  private static final ConfigurableLogger NO_OP_LOGGER = new NoOpLogger();

  private ConfigurableLogger(LogMethod logMethod, boolean logStackTrace) {
    this.logMethod = logMethod;
    this.logStackTrace = logStackTrace;
  }

  public static ConfigurableLogger create(Logger logger, LogLevel logLevel, boolean logStackTrace) {
    return logLevel == LogLevel.OFF
        ? NO_OP_LOGGER
        : new ConfigurableLogger(getLogMethod(logger, logLevel), logStackTrace);
  }

  public void log(String format, Throwable cause, Object... arguments) {
    if (logStackTrace) {
      // to log stack trace, throwable must be the very last parameter passed to the log method
      Object[] newArguments = new Object[arguments.length + 1];
      System.arraycopy(arguments, 0, newArguments, 0, arguments.length);
      newArguments[newArguments.length - 1] = cause;

      logMethod.log(format, newArguments);
    } else {
      logMethod.log(format, arguments);
    }
  }

  private static LogMethod getLogMethod(Logger logger, LogLevel logLevel) {
    LogMethod logMethod = logger::debug;

    switch (logLevel) {
      case TRACE:
        logMethod = logger::trace;
        break;
      case DEBUG:
        logMethod = logger::debug;
        break;
      case INFO:
        logMethod = logger::info;
        break;
      case WARN:
        logMethod = logger::warn;
        break;
      case ERROR:
        logMethod = logger::error;
        break;
    }

    return logMethod;
  }
}
