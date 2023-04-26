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
package com.github.kagkarlsson.scheduler.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class LoggingRunnable implements Runnable {
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    public abstract void runButLogExceptions();

    @Override
    public void run() {
        try {
            runButLogExceptions();
        } catch (Exception e) {
            LOG.error("Unexcepted exception when executing Runnable", e);
        }
    }
}
