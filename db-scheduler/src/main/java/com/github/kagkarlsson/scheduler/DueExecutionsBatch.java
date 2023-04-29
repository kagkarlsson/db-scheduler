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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DueExecutionsBatch {

  private static final Logger LOG = LoggerFactory.getLogger(DueExecutionsBatch.class);
  private final int generationNumber;
  private final AtomicInteger executionsLeftInBatch;
  private final Predicate<Integer> whenToTriggerCheckForNewBatch;
  private boolean possiblyMoreExecutionsInDb;
  private boolean stale = false;
  private boolean triggeredExecuteDue;

  public DueExecutionsBatch(
      int generationNumber,
      int executionsAdded,
      boolean possiblyMoreExecutionsInDb,
      Predicate<Integer> whenToTriggerCheckForNewBatch) {
    this.generationNumber = generationNumber;
    this.possiblyMoreExecutionsInDb = possiblyMoreExecutionsInDb;
    this.executionsLeftInBatch = new AtomicInteger(executionsAdded);
    this.whenToTriggerCheckForNewBatch = whenToTriggerCheckForNewBatch;
  }

  public void markBatchAsStale() {
    this.stale = true;
  }

  /**
   * @param triggerCheckForNewBatch may be triggered more than one in racy conditions
   */
  public void oneExecutionDone(Runnable triggerCheckForNewBatch) {
    // May be called concurrently by multiple threads
    executionsLeftInBatch.decrementAndGet();

    LOG.trace(
        "Batch state: generationNumber:{}, stale:{}, triggeredExecuteDue:{}, possiblyMoreExecutionsInDb:{}, executionsLeftInBatch:{}",
        generationNumber,
        stale,
        triggeredExecuteDue,
        possiblyMoreExecutionsInDb,
        executionsLeftInBatch.get());
    // Will not synchronize this method as it is not a big problem if two threads manage to call
    // triggerCheckForNewBatch.run() at the same time.
    // There is synchronization further in, when waking the thread that will do the fetching.
    if (!stale
        && !triggeredExecuteDue
        && possiblyMoreExecutionsInDb
        && whenToTriggerCheckForNewBatch.test(executionsLeftInBatch.get())) {
      LOG.trace("Triggering check for new batch.");
      triggerCheckForNewBatch.run();
      triggeredExecuteDue = true;
    }
  }

  public boolean isOlderGenerationThan(int compareTo) {
    return generationNumber < compareTo;
  }

  public int getGenerationNumber() {
    return generationNumber;
  }
}
