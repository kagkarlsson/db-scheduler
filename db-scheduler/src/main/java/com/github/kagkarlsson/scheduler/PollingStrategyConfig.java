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
package com.github.kagkarlsson.scheduler;

public class PollingStrategyConfig {

    public static final PollingStrategyConfig DEFAULT_FETCH =
        new PollingStrategyConfig(
            Type.FETCH,
            0.5, 3.0);

    public static final PollingStrategyConfig DEFAULT_SELECT_FOR_UPDATE =
        new PollingStrategyConfig(
            Type.LOCK_AND_FETCH,
            0.5, 1.0);

    public int getUpperLimit(int threadpoolSize) {
        return (int)(upperLimitFractionOfThreads*threadpoolSize);
    }

    public int getLowerLimit(int threadpoolSize) {
        return (int)(lowerLimitFractionOfThreads*threadpoolSize);
    }

    public enum Type {
        LOCK_AND_FETCH,
        FETCH,
    }

    public final Type type;
    public final double lowerLimitFractionOfThreads;
    public final double upperLimitFractionOfThreads;

    public PollingStrategyConfig(Type type, double lowerLimitFractionOfThreads, double upperLimitFractionOfThreads) {
        this.type = type;
        this.lowerLimitFractionOfThreads = lowerLimitFractionOfThreads;
        this.upperLimitFractionOfThreads = upperLimitFractionOfThreads;
        if (lowerLimitFractionOfThreads >= upperLimitFractionOfThreads) {
            throw new IllegalArgumentException("lowerLimitFractionOfThreads should be lower than upperLimitFractionOfThreads");
        }

        if (upperLimitFractionOfThreads < 1.0) {
            throw new IllegalArgumentException("upperLimit should be equals to number of threads or higher, i.e. fraction higher than 1");
        }
    }

    public String describe() {
        return "type=" + type.name() + ", lowerLimit=" + lowerLimitFractionOfThreads + ", upperLimit=" + upperLimitFractionOfThreads;
    }
}
