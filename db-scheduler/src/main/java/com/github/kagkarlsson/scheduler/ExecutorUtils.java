/**
 * Copyright (C) Gustav Karlsson
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutorUtils.class);

    public static boolean shutdownAndAwaitTermination(
            ExecutorService executorService, Duration waitBeforeInterrupt, Duration waitAfterInterrupt) {
        executorService.shutdown();
        boolean successfulShutdown = awaitTermination(executorService, waitBeforeInterrupt);
        if (!successfulShutdown) {
            LOG.info("Interrupting executor service threads for shutdown.");
            executorService.shutdownNow();
            return awaitTermination(executorService, waitAfterInterrupt);
        } else {
            return true;
        }
    }

    public static boolean awaitTermination(ExecutorService executor, Duration timeout) {
        try {
            return executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while waiting for termination of executor.", e);
            return false;
        }
    }

    public static ThreadFactory defaultThreadFactoryWithPrefix(String prefix) {
        return new PrefixingDefaultThreadFactory(prefix);
    }

    private static class PrefixingDefaultThreadFactory implements ThreadFactory {

        private final String prefix;
        private final ThreadFactory defaultThreadFactory;

        public PrefixingDefaultThreadFactory(String prefix) {
            this.defaultThreadFactory = Executors.defaultThreadFactory();
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            final Thread thread = defaultThreadFactory.newThread(r);
            thread.setName(prefix + thread.getName());
            return thread;
        }
    }
}
