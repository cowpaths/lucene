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
import java.util.Objects;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.InfoStream;

/**
 * Per-{@link IndexWriter} {@link ConcurrentMergeScheduler} that consults a shared {@link
 * MergeConcurrencyGate} when assigning merge IO rates. The gate may force a merge thread to pause
 * ({@code 0.0} MB/s) so that node-wide running-merge concurrency can be capped without sharing one
 * CMS instance across cores.
 *
 * <p>Local {@code maxThreadCount}/{@code maxMergeCount}, IO throttle, stall, and close behavior
 * remain per-scheduler. The gate only adds an extra pause constraint on top.
 *
 * @lucene.experimental
 */
public final class GlobalConcurrentMergeScheduler extends ConcurrentMergeScheduler {

  /**
   * Shared concurrency gate used by {@link GlobalConcurrentMergeScheduler} instances. Implemented
   * outside Lucene (e.g. Solr) so clusterprops / ZK wiring stays out of this class.
   */
  public interface MergeConcurrencyGate {
    /**
     * Possibly reduce {@code proposedMBPerSec} (typically to {@code 0.0} to pause) based on
     * node-wide running-merge limits.
     *
     * @param scheduler the scheduler owning {@code mergeKey}
     * @param mergeKey stable identity for the merge thread (typically the {@link MergeThread})
     * @param proposedMBPerSec rate the local CMS would apply
     * @return rate to apply
     */
    double adjustRate(
        ConcurrentMergeScheduler scheduler, Object mergeKey, double proposedMBPerSec);

    /** Called when a scheduler is bound to an {@link IndexWriter}. */
    void register(ConcurrentMergeScheduler scheduler);

    /** Called when a scheduler is closed; must release that scheduler's permits. */
    void unregister(ConcurrentMergeScheduler scheduler);

    /**
     * Called after this scheduler finished {@link #updateMergeThreads()}. Implementations must not
     * call other schedulers synchronously (deadlock risk); wake peers asynchronously.
     */
    void afterUpdate(ConcurrentMergeScheduler scheduler);
  }

  /** Gate that never restricts rates (useful in unit tests). */
  public static final MergeConcurrencyGate NOOP_GATE =
      new MergeConcurrencyGate() {
        @Override
        public double adjustRate(
            ConcurrentMergeScheduler scheduler, Object mergeKey, double proposedMBPerSec) {
          return proposedMBPerSec;
        }

        @Override
        public void register(ConcurrentMergeScheduler scheduler) {}

        @Override
        public void unregister(ConcurrentMergeScheduler scheduler) {}

        @Override
        public void afterUpdate(ConcurrentMergeScheduler scheduler) {}
      };

  private final MergeConcurrencyGate gate;

  /** Creates a per-writer scheduler that consults {@code gate} for node-wide pause decisions. */
  public GlobalConcurrentMergeScheduler(MergeConcurrencyGate gate) {
    this.gate = Objects.requireNonNull(gate, "gate");
  }

  /** Returns the gate injected at construction. */
  public MergeConcurrencyGate getGate() {
    return gate;
  }

  /**
   * Re-evaluates pause/run rates. Invoked asynchronously by the gate when peer schedulers free
   * permits; must not be called while holding another CMS lock.
   */
  public void rebalanceMergeThreads() {
    updateMergeThreads();
  }

  @Override
  protected double adjustMergeRate(MergeThread mergeThread, double proposedMBPerSec) {
    return gate.adjustRate(this, mergeThread, proposedMBPerSec);
  }

  @Override
  protected synchronized void updateMergeThreads() {
    super.updateMergeThreads();
    gate.afterUpdate(this);
  }

  @Override
  void initialize(InfoStream infoStream, Directory directory) throws IOException {
    super.initialize(infoStream, directory);
    gate.register(this);
  }

  @Override
  public void close() throws IOException {
    try {
      gate.unregister(this);
    } finally {
      super.close();
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + "[maxThreadCount="
        + getMaxThreadCount()
        + ", maxMergeCount="
        + getMaxMergeCount()
        + ", ioThrottle="
        + getAutoIOThrottle()
        + "]";
  }
}
