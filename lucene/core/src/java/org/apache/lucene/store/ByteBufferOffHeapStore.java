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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * {@link OffHeapStore} backed by a direct {@link ByteBuffer} and {@link
 * MethodHandles#byteBufferViewVarHandle}. Always available on any JDK -- no {@code sun.misc.Unsafe}
 * and no {@code java.lang.foreign} dependency -- so this is both {@link OffHeapStores}' no-reflection
 * fallback (when a JDK21+ {@code MemorySegment}-backed implementation can't be loaded) and, once a
 * future Holder-pattern singleton factory is in place, the implementation handed to every caller
 * after the first (see {@link OffHeapStores}'s class javadoc).
 *
 * <p>{@code byteBufferViewVarHandle}'s own coordinate is a raw byte offset, unlike {@code
 * MemorySegmentOffHeapStore}'s {@code arrayElementVarHandle} (a scaled element index) -- so unlike
 * that implementation, every accessor here must convert {@link OffHeapStore}'s element-indexed
 * {@code intIndex}/{@code longIndex} parameters (see that interface's javadoc) to a byte offset via
 * a fixed left-shift before the VarHandle call, then narrow to {@code int} (a plain cast, not
 * {@code Math.toIntExact}: {@link OffHeapStores#createByteBuffer} already proves the buffer's whole
 * byte size fits in an {@code int} via one-time {@code Math.multiplyExact(slots, stride)} at
 * construction, so any {@code intIndex}/{@code longIndex} a caller validly passes in can never
 * shift to a byte offset outside that already-proven range -- an unconditional per-access check
 * here would be pure overhead on the hot path, re-proving an invariant already established once).
 *
 * <p>Allocates its own dedicated {@link ByteBuffer} per instance (one per {@link
 * OffHeapStores#create} call).
 */
final class ByteBufferOffHeapStore implements OffHeapStore {
  private static final VarHandle INT_VH =
      MethodHandles.byteBufferViewVarHandle(int[].class, ByteOrder.nativeOrder());
  private static final VarHandle LONG_VH =
      MethodHandles.byteBufferViewVarHandle(long[].class, ByteOrder.nativeOrder());

  /**
   * Retained so the buffer stays reachable for as long as this store is in use -- matches {@code
   * UnsafeOffHeapStore}'s equivalent field/rationale in {@code solr/core}.
   */
  private final ByteBuffer buf;

  ByteBufferOffHeapStore(ByteBuffer buf) {
    this.buf = buf;
  }

  @Override
  public int getIntVolatile(long intIndex) {
    return (int) INT_VH.getVolatile(buf, (int) (intIndex << 2));
  }

  @Override
  public void putInt(long intIndex, int value) {
    INT_VH.set(buf, (int) (intIndex << 2), value);
  }

  @Override
  public void putIntVolatile(long intIndex, int value) {
    INT_VH.setVolatile(buf, (int) (intIndex << 2), value);
  }

  @Override
  public boolean compareAndSetInt(long intIndex, int expect, int update) {
    return INT_VH.compareAndSet(buf, (int) (intIndex << 2), expect, update);
  }

  @Override
  public int compareAndExchangeInt(long intIndex, int expect, int update) {
    return (int) INT_VH.compareAndExchange(buf, (int) (intIndex << 2), expect, update);
  }

  @Override
  public long getLongVolatile(long longIndex) {
    return (long) LONG_VH.getVolatile(buf, (int) (longIndex << 3));
  }

  @Override
  public long getLong(long longIndex) {
    return (long) LONG_VH.get(buf, (int) (longIndex << 3));
  }

  @Override
  public void putLong(long longIndex, long value) {
    LONG_VH.set(buf, (int) (longIndex << 3), value);
  }

  @Override
  public long getAndSetLong(long longIndex, long value) {
    return (long) LONG_VH.getAndSet(buf, (int) (longIndex << 3), value);
  }
}
