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

import java.io.IOException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.InfoStream;

/**
 * A JVM-wide singleton {@link ConcurrentMergeScheduler}. All {@link IndexWriter}s that use this
 * scheduler share one merge-thread pool, so {@code maxThreadCount} / {@code maxMergeCount} cap
 * concurrent merges across the entire process rather than per core.
 *
 * <p>Obtain the shared instance via {@link #getInstance()}. {@link IndexWriter} lifecycle is
 * refcounted: {@link #initialize} increments the active-writer count and {@link #close} decrements
 * it. Only the last writer's {@code close} tears down the pool; earlier closes are no-ops so other
 * cores can keep merging.
 *
 * <p>When one writer closes while others remain, {@link MergeTrigger#CLOSING} does not disable
 * global IO throttle (which would otherwise affect remaining writers).
 *
 * @lucene.experimental
 */
public final class GlobalConcurrentMergeScheduler extends ConcurrentMergeScheduler {

  private static final Object INSTANCE_LOCK = new Object();
  private static GlobalConcurrentMergeScheduler INSTANCE;

  /** Number of IndexWriters currently holding this scheduler open. */
  private int activeWriters;

  private GlobalConcurrentMergeScheduler() {}

  /** Returns the JVM-wide singleton instance, creating it on first use. */
  public static GlobalConcurrentMergeScheduler getInstance() {
    synchronized (INSTANCE_LOCK) {
      if (INSTANCE == null) {
        INSTANCE = new GlobalConcurrentMergeScheduler();
      }
      return INSTANCE;
    }
  }

  /**
   * Resets the singleton for tests. Must only be called when no writers still hold the scheduler
   * open ({@link #getActiveWriterCount()} == 0).
   *
   * @lucene.internal
   */
  public static void resetForTesting() {
    synchronized (INSTANCE_LOCK) {
      if (INSTANCE != null) {
        synchronized (INSTANCE) {
          if (INSTANCE.activeWriters != 0) {
            throw new IllegalStateException(
                "Cannot reset GlobalConcurrentMergeScheduler while activeWriters="
                    + INSTANCE.activeWriters);
          }
        }
      }
      INSTANCE = null;
    }
  }

  /** Returns how many IndexWriters currently reference this scheduler. */
  public synchronized int getActiveWriterCount() {
    return activeWriters;
  }

  @Override
  void initialize(InfoStream infoStream, Directory directory) throws IOException {
    synchronized (this) {
      activeWriters++;
      if (activeWriters == 1 || intraMergeExecutor == null) {
        // First writer, or re-open after a full close tore down the executor.
        super.initialize(infoStream, directory);
      } else {
        // Keep sharing the existing pool; refresh infoStream for logging (last-wins).
        this.infoStream = infoStream;
      }
    }
  }

  @Override
  public synchronized void merge(MergeSource mergeSource, MergeTrigger trigger) throws IOException {
    // CMS disables IO throttle on CLOSING; that must stay local to the last writer so other
    // cores are not affected when one core unloads.
    if (trigger == MergeTrigger.CLOSING && activeWriters > 1) {
      super.merge(mergeSource, MergeTrigger.EXPLICIT);
      return;
    }
    super.merge(mergeSource, trigger);
  }

  @Override
  public void close() throws IOException {
    final boolean doFullClose;
    synchronized (this) {
      if (activeWriters <= 0) {
        return;
      }
      activeWriters--;
      doFullClose = activeWriters == 0;
    }
    if (doFullClose) {
      // Join remaining merge threads and shut down the CachedExecutor only.
      // Do not call MergeScheduler.close(): its SameThreadExecutorService is final and cannot
      // be recreated, but cores may come and go over the JVM lifetime.
      // Do not hold 'this' across sync() — merge threads need the CMS monitor.
      try {
        sync();
      } finally {
        shutdownIntraMergeExecutor();
      }
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + "[singleton, activeWriters="
        + getActiveWriterCount()
        + ", "
        + "maxThreadCount="
        + getMaxThreadCount()
        + ", "
        + "maxMergeCount="
        + getMaxMergeCount()
        + ", "
        + "ioThrottle="
        + getAutoIOThrottle()
        + "]";
  }
}
