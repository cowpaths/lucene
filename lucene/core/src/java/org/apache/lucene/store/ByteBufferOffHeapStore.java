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
  public int getIntVolatile(long offset) {
    return (int) INT_VH.getVolatile(buf, Math.toIntExact(offset));
  }

  @Override
  public void putInt(long offset, int value) {
    INT_VH.set(buf, Math.toIntExact(offset), value);
  }

  @Override
  public void putIntVolatile(long offset, int value) {
    INT_VH.setVolatile(buf, Math.toIntExact(offset), value);
  }

  @Override
  public boolean compareAndSetInt(long offset, int expect, int update) {
    return INT_VH.compareAndSet(buf, Math.toIntExact(offset), expect, update);
  }

  @Override
  public int compareAndExchangeInt(long offset, int expect, int update) {
    return (int) INT_VH.compareAndExchange(buf, Math.toIntExact(offset), expect, update);
  }

  @Override
  public long getLongVolatile(long offset) {
    return (long) LONG_VH.getVolatile(buf, Math.toIntExact(offset));
  }

  @Override
  public long getLong(long offset) {
    return (long) LONG_VH.get(buf, Math.toIntExact(offset));
  }

  @Override
  public void putLong(long offset, long value) {
    LONG_VH.set(buf, Math.toIntExact(offset), value);
  }
}
