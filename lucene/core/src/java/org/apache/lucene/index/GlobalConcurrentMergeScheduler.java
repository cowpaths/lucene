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
     * Tries to acquire a permit without blocking.
     *
     * @return whether a permit was acquired
     */
    boolean tryAcquire();

    /** Releases one previously acquired permit. */
    void release();
  }

  /** Semaphore that never restricts merge spawning (useful in unit tests). */
  public static final MergeConcurrencySemaphore UNLIMITED =
      new MergeConcurrencySemaphore() {
        @Override
        public boolean tryAcquire() {
          return true;
        }

        @Override
        public void release() {}
      };

  private final MergeConcurrencySemaphore semaphore;

  public GlobalConcurrentMergeScheduler(MergeConcurrencySemaphore semaphore) {
    this.semaphore = Objects.requireNonNull(semaphore);
  }

  /** Returns the semaphore injected at construction. */
  public MergeConcurrencySemaphore getSemaphore() {
    return semaphore;
  }

  private MergePermit getCurrentMergePermit() {
    return mergeThreads.get(Thread.currentThread());
  }

  private void releaseCurrentMergePermit() {
    MergePermit currentMergePermit = getCurrentMergePermit();
    if (currentMergePermit != null) {
      currentMergePermit.release();
    }
  }

  @Override
  protected synchronized MergePermit maybeStall(MergeSource mergeSource) {
    if (super.maybeStall(mergeSource) == null) {
      return null;
    }

    //this thread should have already acquired a permit for this merge, so we don't need to acquire it again
    MergePermit currentMergePermit = getCurrentMergePermit();
    if (currentMergePermit != null) {
      return currentMergePermit;
    }

    while (!semaphore.tryAcquire()) {
      if (verbose()) {
        message("    too many merges globally; stalling...");
      }
      doStall();
    }
    return new GlobalMergePermit();
  }

  class GlobalMergePermit extends MergePermit {
    private boolean released = false;

    @Override
    public synchronized void release() {
      if (!released) {
        semaphore.release();
        released = true;
      }
    }
  }

  @Override
  synchronized void runOnMergeFinished(MergeThread mergeThread) {
    releaseCurrentMergePermit(); //need to release the semaphore here as super might call merge(mergeThread.mergeSource, MergeTrigger.MERGE_FINISHED), which could deadlock if we don't release the semaphore
    super.runOnMergeFinished(mergeThread);
  }

  @Override
  protected void postMerge(MergePolicy.OneMerge merge) {
    releaseCurrentMergePermit(); //in case the merge failed we still need to release the semaphore. If it was already released it will just be a no-op
  }


}
