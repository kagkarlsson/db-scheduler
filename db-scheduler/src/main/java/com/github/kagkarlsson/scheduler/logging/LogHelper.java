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
package com.github.kagkarlsson.scheduler.logging;

import org.slf4j.Logger;
import org.slf4j.event.Level;

public class LogHelper {

    public interface LogMethod {
        void log(String format, Object... arguments);
    }

    public static LogMethod getLogMethod(Logger logger, Level logLevel) {
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
