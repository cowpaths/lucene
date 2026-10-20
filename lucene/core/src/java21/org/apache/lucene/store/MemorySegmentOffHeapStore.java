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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * {@link OffHeapStore} backed by {@code java.lang.foreign.MemorySegment}, using {@link
 * ValueLayout#arrayElementVarHandle} rather than {@code MethodHandles.memorySegmentViewVarHandle}:
 * the latter lives in {@code java.lang.invoke}, outside the {@code java.base/java/lang/foreign/*}
 * glob this module's MR-JAR tooling (`lucene/gradle/generation/extract-jdk-apis.gradle`) extracts
 * into the stripped, preview-free API jar this class is compiled against, while {@code
 * arrayElementVarHandle} is a default method on {@link ValueLayout} itself (inside that glob). It
 * is also performance-competitive: for compare-and-exchange -- the dominant traffic pattern for
 * this store's actual caller -- it closes nearly the entire gap against {@code sun.misc.Unsafe} in
 * the partitioned shape that matches real usage.
 *
 * <p><b>No index conversion needed:</b> {@code arrayElementVarHandle}'s {@code long} coordinate is
 * itself a scaled element index (index * elementSize == byte offset) -- exactly what {@link
 * OffHeapStore}'s {@code intIndex}/{@code longIndex} parameters already are (see that interface's
 * javadoc). Every accessor below therefore passes its index straight through to the VarHandle call,
 * with no shift at all -- unlike {@code UnsafeOffHeapStore}/{@code ByteBufferOffHeapStore}, which
 * are natively byte-addressed and must each convert at their own call sites instead.
 *
 * <p><b>One static final backing segment, JVM-wide:</b> this class has no instance state at all
 * (every field is {@code static final}) -- {@link #SEGMENT} is sized once, at class-initialization
 * time, by reading {@link #TOTAL_BYTES_SYSPROP}/{@link #ALIGNMENT_SYSPROP}. An instance-field
 * segment reference measurably regresses performance, since C2 loses the ability to treat it as a
 * compile-time constant once it's reached via an instance field; a genuinely {@code static final}
 * field does not have that problem. Because {@code static final} fields can only be set once, at
 * class-init, with no runtime parameters, sizing must happen via a side-channel (these two system
 * properties) set by {@link OffHeapStores#tryClaimMemorySegment} immediately before the one-time
 * {@code Class.forName} load that triggers this class's static initializer, then cleared -- see
 * that method's javadoc for the full claim protocol. This class is never meant to be loaded more
 * than once per JVM; instances are stateless (hold no fields of their own), so beyond the one real
 * claim, further {@code new MemorySegmentOffHeapStore()} calls are harmless (merely redundant) but
 * are not expected to occur as nothing in this codebase does so.
 *
 * <p>Allocates via {@link Arena#global()} -- a JVM-wide singleton arena with no {@code close()}
 * lifecycle, exactly matching this store's own life-of-JVM singleton design (no {@code close()} on
 * {@link OffHeapStore}), and matching this module's own existing convention for the same tradeoff
 * ({@code LinuxMadvise}'s {@code GLOBAL_ARENA} flag, true by default). Beyond matching the real
 * lifecycle, {@code Arena.global()} segments skip the per-access temporal (liveness/thread-
 * confinement) check that confined/shared/automatic arenas' segments require on every access -- a
 * real, avoidable per-access cost with no corresponding benefit here.
 *
 * <p>The class itself is package-private (loaded reflectively via {@link OffHeapStores}, like
 * {@code LinuxMadvise}), but its constructor is {@code public} so {@link Class#getConstructor}-
 * based reflective lookup succeeds -- the same split {@code LinuxMadvise}/{@code
 * BlockCacheMmapProvider#getDefault()} uses (package-private class, public entry point).
 */
final class MemorySegmentOffHeapStore implements OffHeapStore {
  // These two sysprop name literals MUST exactly match the ones OffHeapStores.java hardcodes
  // (TOTAL_BYTES_SYSPROP/ALIGNMENT_SYSPROP there) -- they cannot be shared via a real constant
  // reference, since this class lives in a separate MR-JAR source set (src/java21) compiled
  // independently of (and invisible to) OffHeapStores' own compilation unit (src/java). If you
  // rename either sysprop here, you must rename it there too, by hand.
  private static final String TOTAL_BYTES_SYSPROP =
      "org.apache.lucene.store.MemorySegmentOffHeapStore.totalBytes";
  private static final String ALIGNMENT_SYSPROP =
      "org.apache.lucene.store.MemorySegmentOffHeapStore.alignment";

  private static final VarHandle INT_VH =
      ValueLayout.JAVA_INT.withOrder(ByteOrder.nativeOrder()).arrayElementVarHandle();
  private static final VarHandle LONG_VH =
      ValueLayout.JAVA_LONG.withOrder(ByteOrder.nativeOrder()).arrayElementVarHandle();

  private static final MemorySegment SEGMENT = allocateSegment();

  private static MemorySegment allocateSegment() {
    long totalBytes = Long.parseLong(System.getProperty(TOTAL_BYTES_SYSPROP));
    int alignment = Integer.parseInt(System.getProperty(ALIGNMENT_SYSPROP));
    return Arena.global().allocate(totalBytes, alignment);
  }

  public MemorySegmentOffHeapStore() {}

  @Override
  public int getIntVolatile(long intIndex) {
    return (int) INT_VH.getVolatile(SEGMENT, intIndex);
  }

  @Override
  public void putInt(long intIndex, int value) {
    INT_VH.set(SEGMENT, intIndex, value);
  }

  @Override
  public void putIntVolatile(long intIndex, int value) {
    INT_VH.setVolatile(SEGMENT, intIndex, value);
  }

  @Override
  public boolean compareAndSetInt(long intIndex, int expect, int update) {
    return INT_VH.compareAndSet(SEGMENT, intIndex, expect, update);
  }

  @Override
  public int compareAndExchangeInt(long intIndex, int expect, int update) {
    return (int) INT_VH.compareAndExchange(SEGMENT, intIndex, expect, update);
  }

  @Override
  public long getLongVolatile(long longIndex) {
    return (long) LONG_VH.getVolatile(SEGMENT, longIndex);
  }

  @Override
  public long getLong(long longIndex) {
    return (long) LONG_VH.get(SEGMENT, longIndex);
  }

  @Override
  public void putLong(long longIndex, long value) {
    LONG_VH.set(SEGMENT, longIndex, value);
  }
}
