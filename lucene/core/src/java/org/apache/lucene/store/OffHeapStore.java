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
 * <p><b>Indexing is element-based, not byte-based:</b> int-typed methods ({@code getIntVolatile},
 * {@code putInt}, etc.) take an {@code intIndex} (the address of the target int, counted in 4-byte
 * words from the start of whatever region this store <em>instance</em> represents); long-typed
 * methods take a {@code longIndex} (counted in 8-byte words). This matches the one implementation
 * ({@code MemorySegmentOffHeapStore}, via {@code ValueLayout.arrayElementVarHandle}) whose native
 * coordinate is itself a scaled element index rather than a raw byte offset -- putting the
 * conversion at the external API boundary means that implementation's hot-path methods need zero
 * conversion at all, while the other two implementations ({@code UnsafeOffHeapStore}, {@code
 * ByteBufferOffHeapStore}, both natively byte-addressed) each pay one cheap, fixed left-shift
 * (`intIndex &lt;&lt; 2` / `longIndex &lt;&lt; 3`) per call instead.
 *
 * <p>A caller addressing a specific byte-level field offset (e.g. a field at a fixed byte offset
 * within a larger stride) must itself already know that offset is a multiple of 4 (for int-typed
 * methods) or 8 (for long-typed methods) -- true of every field this interface's existing callers
 * address -- and convert via a right-shift (`byteOffset &gt;&gt;&gt; 2` / `byteOffset &gt;&gt;&gt;
 * 3`) before calling in, exactly once, at the point where a byte-level constant is combined with a
 * slot's base index.
 *
 * <p>A store obtained directly from {@link OffHeapStores} is addressed from index 0. A caller
 * sharing one store across multiple regions (each with its own {@code final long baseIndex} into
 * the shared store, itself already in the same index units) adds that base index at each call site
 * itself -- a bare primitive field, never wrapped in an object, for JIT constant-folding of the
 * backing store reference.
 *
 * <p>Method semantics (plain vs. volatile vs. compare-and-swap/exchange) exactly match {@code
 * sun.misc.Unsafe}'s own naming and behavior, since every implementation must support the
 * identical access patterns callers already rely on.
 */
public interface OffHeapStore {
  /** Volatile read of the 4-byte int at {@code intIndex} (a 4-byte-word-granular index). */
  int getIntVolatile(long intIndex);

  /** Plain (non-atomic, non-volatile) write of the 4-byte int at {@code intIndex}. */
  void putInt(long intIndex, int value);

  /** Volatile write of the 4-byte int at {@code intIndex}. */
  void putIntVolatile(long intIndex, int value);

  /**
   * Atomically sets the int at {@code intIndex} to {@code update} if it currently equals {@code
   * expect}, returning {@code true} iff the exchange succeeded -- same semantics as {@code
   * VarHandle.compareAndSet}.
   */
  boolean compareAndSetInt(long intIndex, int expect, int update);

  /**
   * Atomically sets the int at {@code intIndex} to {@code update} if it currently equals {@code
   * expect}, returning the witness value (the value observed immediately before this call, which
   * equals {@code expect} iff the exchange succeeded) -- same semantics as {@code
   * VarHandle.compareAndExchange}.
   */
  int compareAndExchangeInt(long intIndex, int expect, int update);

  /** Volatile read of the 8-byte long at {@code longIndex} (an 8-byte-word-granular index). */
  long getLongVolatile(long longIndex);

  /** Plain (non-atomic, non-volatile) read of the 8-byte long at {@code longIndex}. */
  long getLong(long longIndex);

  /** Plain (non-atomic, non-volatile) write of the 8-byte long at {@code longIndex}. */
  void putLong(long longIndex, long value);

  /**
   * Atomically sets the long at {@code longIndex} to {@code value} and returns the previous value
   * -- same semantics as {@code VarHandle.getAndSet} (a full read-modify-write with volatile
   * memory ordering, i.e. equivalent to a {@link #getLongVolatile} immediately followed by a
   * volatile write, but as a single atomic operation).
   */
  long getAndSetLong(long longIndex, long value);
}
