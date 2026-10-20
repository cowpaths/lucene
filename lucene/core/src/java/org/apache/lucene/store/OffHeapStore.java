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

package org.apache.lucene.store;

/**
 * Raw primitive off-heap memory operations, abstracting a caller's strided storage needs (e.g.
 * Solr's {@code Cache2}) away from the specific backing mechanism. Multiple implementations may
 * exist (see {@link OffHeapStores} for the factory that selects among them):
 *
 * <ul>
 *   <li>A plain {@link java.nio.ByteBuffer}-backed implementation -- always available, on any JDK.
 *   <li>A {@code MemorySegment}-backed implementation, available on JDK 21+ -- see {@code
 *       MemorySegmentOffHeapStore}'s javadoc for the specific shape chosen and why.
 * </ul>
 *
 * <p>All operations take a {@code long offset} relative to whatever region this specific store
 * <em>instance</em> represents. A store obtained directly from {@link OffHeapStores} is addressed
 * from offset 0. A caller sharing one store across multiple regions (each with its own {@code
 * final long baseOffset} into the shared store) adds that offset at each call site itself -- a
 * bare primitive field, never wrapped in an object, which benchmarking showed matters for JIT
 * constant-folding of the backing store reference (see {@code solr/prodbench/FINDINGS.md}).
 *
 * <p>Method semantics (plain vs. volatile vs. compare-and-swap/exchange) exactly match {@code
 * sun.misc.Unsafe}'s own naming and behavior, since every implementation must support the
 * identical access patterns callers already rely on.
 */
public interface OffHeapStore {
  /** Volatile read of a 4-byte int at {@code offset}. */
  int getIntVolatile(long offset);

  /** Plain (non-atomic, non-volatile) write of a 4-byte int at {@code offset}. */
  void putInt(long offset, int value);

  /** Volatile write of a 4-byte int at {@code offset}. */
  void putIntVolatile(long offset, int value);

  /**
   * Atomically sets the int at {@code offset} to {@code update} if it currently equals {@code
   * expect}, returning {@code true} iff the exchange succeeded -- same semantics as {@code
   * VarHandle.compareAndSet}.
   */
  boolean compareAndSetInt(long offset, int expect, int update);

  /**
   * Atomically sets the int at {@code offset} to {@code update} if it currently equals {@code
   * expect}, returning the witness value (the value observed immediately before this call, which
   * equals {@code expect} iff the exchange succeeded) -- same semantics as {@code
   * VarHandle.compareAndExchange}.
   */
  int compareAndExchangeInt(long offset, int expect, int update);

  /** Volatile read of an 8-byte long at {@code offset}. */
  long getLongVolatile(long offset);

  /** Plain (non-atomic, non-volatile) read of an 8-byte long at {@code offset}. */
  long getLong(long offset);

  /** Plain (non-atomic, non-volatile) write of an 8-byte long at {@code offset}. */
  void putLong(long offset, long value);
}
