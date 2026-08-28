/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.index;

import java.util.Objects;

/**
 * Per-{@link IndexWriter} {@link ConcurrentMergeScheduler} that stalls merge spawning against a
 * shared {@link MergeConcurrencySemaphore}, so node-wide concurrent merge threads can be capped
 * without sharing one CMS instance across cores.
 *
 * @lucene.experimental
 */
public class GlobalConcurrentMergeScheduler extends ConcurrentMergeScheduler {

  /**
   * Shared concurrency semaphore used by {@link GlobalConcurrentMergeScheduler} instances.
   * Implemented outside Lucene (e.g. Solr) so clusterprops / ZK wiring stays out of this class.
   */
  public interface MergeConcurrencySemaphore {
    /**
     * Tries to acquire a permit for the given merge. Returns true if a permit was acquired, false otherwise.
     * <b>
     * Call on merge that holds an active is a no-op and will return true
     * @param merge
     * @return whether a permit is successfully acquired by this merge
     */
    boolean tryAcquire(MergePolicy.OneMerge merge);

    /**
     * Release a previously acquired permit for the given merge.
     * This is idempotent: if the merge was already released/never acquired, this method is a no-op.
     * @param merge
     */
    void release(MergePolicy.OneMerge merge);
  }

  /** Semaphore that never restricts merge spawning (useful in unit tests). */
  public static final MergeConcurrencySemaphore UNLIMITED =
      new MergeConcurrencySemaphore() {
        @Override
        public boolean tryAcquire(MergePolicy.OneMerge merge) {
          return true;
        }

        @Override
        public void release(MergePolicy.OneMerge merge) {}
      };

  private final MergeConcurrencySemaphore semaphore;

  public GlobalConcurrentMergeScheduler(MergeConcurrencySemaphore semaphore) {
    this.semaphore = Objects.requireNonNull(semaphore);
  }

  /** Returns the semaphore injected at construction. */
  public MergeConcurrencySemaphore getSemaphore() {
    return semaphore;
  }

  @Override
  protected synchronized boolean maybeStall(MergePolicy.OneMerge merge) {
    if (merge.isAborted()) {
      return false;
    }

    if (super.maybeStall(merge) == false) {
      return false;
    }

    // Never stall a merge thread (see maybeStall(MergeSource)): if no global permit is
    // available, skip spawning so this thread can finish and notify waiters.
    if (mergeThreads.contains(Thread.currentThread())) {
      return semaphore.tryAcquire(merge);
    }

    while (!semaphore.tryAcquire(merge)) {
      if (merge.isAborted()) {
        return false;
      }
      if (verbose()) {
        message("    too many merges globally; stalling...");
      }
      doStall();
    }
    return true;
  }

  @Override
  synchronized void runOnMergeFinished(MergeThread mergeThread) {
    semaphore.release(mergeThread.merge); //need to release the semaphore here as super might call merge(mergeThread.mergeSource, MergeTrigger.MERGE_FINISHED), which could deadlock if we don't release the semaphore
    super.runOnMergeFinished(mergeThread);
  }

  @Override
  protected void postMerge(MergePolicy.OneMerge merge) {
    semaphore.release(merge); //in case the merge failed we still need to release the semaphore. If it was already released it will just be a no-op
  }


}
