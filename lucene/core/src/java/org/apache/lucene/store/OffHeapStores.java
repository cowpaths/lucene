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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Factory helpers for {@link OffHeapStore} instances.
 *
 * <p>{@code MemorySegmentOffHeapStore} (JDK 21+, loaded reflectively -- not referenced by name
 * from this class's compiled bytecode, since it lives in this module's {@code src/java21} MR-JAR
 * source set) holds its single backing {@code MemorySegment} in a {@code static final} field --
 * see that class's javadoc for why a {@code static final} field (not an instance field) is
 * necessary for its benchmarked performance. A {@code static final} field can only be sized once,
 * at class-initialization time, with no constructor parameters -- so at most <em>one</em> caller,
 * JVM-wide, can ever have its requested size honored by a real {@code MemorySegment}. {@link
 * #tryClaimMemorySegment} implements that one-shot claim: the first caller to invoke it (if the
 * class loads successfully at all) wins and gets a working {@code MemorySegmentOffHeapStore} sized
 * exactly as requested; every other caller (whether before, during, or after that first successful
 * claim) gets {@code null} and must fall back to {@link #createByteBuffer} (always available, on
 * any JDK, any number of times).
 */
public final class OffHeapStores {
  private static final String MEMORY_SEGMENT_IMPL_CLASS =
      "org.apache.lucene.store.MemorySegmentOffHeapStore";

  // These two sysprop name literals MUST exactly match MemorySegmentOffHeapStore's own
  // TOTAL_BYTES_SYSPROP/ALIGNMENT_SYSPROP (src/java21) -- see that class's matching comment for
  // why they cannot be shared via a real constant reference across the two source sets.
  private static final String TOTAL_BYTES_SYSPROP =
      "org.apache.lucene.store.MemorySegmentOffHeapStore.totalBytes";
  private static final String ALIGNMENT_SYSPROP =
      "org.apache.lucene.store.MemorySegmentOffHeapStore.alignment";

  private static final AtomicBoolean CLAIMED = new AtomicBoolean();

  private OffHeapStores() {}

  /**
   * Attempts the one-shot, JVM-wide claim on a real {@code MemorySegment}-backed {@link
   * OffHeapStore} of exactly {@code totalBytes} bytes, aligned to {@code alignment}. Returns the
   * claimed store on success, or {@code null} if this JVM already has a (successful or attempted)
   * claim in progress/complete, or if the JDK21+ implementation class can't be loaded or
   * initialized for any reason (e.g. an older JDK missing the FFM API entirely, or a newer JDK
   * whose FFM API has since changed incompatibly -- see the {@code LinkageError} catch below) --
   * callers must fall back to {@link #createByteBuffer} in every {@code null} case.
   *
   * <p>Protocol: {@link #CLAIMED} is a single {@link AtomicBoolean} gate shared across every
   * caller in the JVM (there is deliberately only one -- see {@code
   * Cache2#allocateSharedGroup1And2Stores}'s javadoc in {@code solr/core} for why a single combined
   * claim, rather than one per logical region, is the right granularity). The winner of the
   * {@code compareAndSet} race sets {@link #TOTAL_BYTES_SYSPROP}/{@link #ALIGNMENT_SYSPROP} to its
   * own requested size, then triggers the implementation class's first-ever {@code Class.forName}
   * load -- since {@link #CLAIMED} guarantees only one thread, ever, reaches this point (combined
   * with the JVM's own guarantee that a class's static initializer runs at most once, synchronized
   * against concurrent initializers of the <em>same</em> class), the implementation class's {@code
   * static final} segment field reads exactly the one set of sysprop values meant for it, with no
   * possibility of a second caller's values racing in first. The sysprops are cleared in a {@code
   * finally} block immediately after the load attempt -- harmless either way by that point, since
   * the static initializer (if it ran) already captured the values it needed; clearing avoids
   * leaving stale/misleading system properties behind for unrelated later inspection.
   */
  public static OffHeapStore tryClaimMemorySegment(long totalBytes, int alignment) {
    if (!CLAIMED.compareAndSet(false, true)) {
      return null;
    }
    System.setProperty(TOTAL_BYTES_SYSPROP, Long.toString(totalBytes));
    System.setProperty(ALIGNMENT_SYSPROP, Integer.toString(alignment));
    try {
      Class<?> cls = Class.forName(MEMORY_SEGMENT_IMPL_CLASS);
      return (OffHeapStore) cls.getConstructor().newInstance();
    } catch (@SuppressWarnings("unused") ReflectiveOperationException e) {
      return null;
    } catch (@SuppressWarnings("unused") LinkageError e) {
      // TODO: ValueLayout.arrayElementVarHandle(int...) (used by MemorySegmentOffHeapStore) was
      // removed in JDK 22, replaced by MemoryLayout.arrayElementVarHandle(PathElement...) -- on
      // JDK 22+ this manifests as a NoSuchMethodError (a LinkageError, not a
      // ReflectiveOperationException) thrown from MemorySegmentOffHeapStore's static initializer,
      // which the catch clause above alone would not catch, crashing the caller instead of falling
      // back gracefully. Once this module targets JDK 22+ (a new src/java22 MR-JAR source set,
      // its own extracted apijar, etc.), update MemorySegmentOffHeapStore to use the new API and
      // this catch can be narrowed back down (or removed, if no other LinkageError is plausible by
      // then). Until then, catching LinkageError here ensures any such failure degrades to the
      // documented createByteBuffer fallback instead of propagating as an uncaught Error.
      return null;
    } finally {
      System.clearProperty(TOTAL_BYTES_SYSPROP);
      System.clearProperty(ALIGNMENT_SYSPROP);
    }
  }

  /**
   * Allocates a new {@link ByteBufferOffHeapStore} with at least {@code slots * stride} bytes of
   * backing storage, aligned to a full {@code alignment}-byte boundary (a hard guarantee -- see
   * {@code Cache2#allocateAlignedDirect}'s javadoc in {@code solr/core} for the margin/rounding
   * rationale this mirrors). Always available, on any JDK, any number of times -- the fallback for
   * every caller {@link #tryClaimMemorySegment} returns {@code null} to.
   */
  public static OffHeapStore createByteBuffer(int slots, int stride, int alignment) {
    int needed = Math.multiplyExact(slots, stride);
    ByteBuffer buf = ByteBuffer.allocateDirect(needed + 2 * (alignment - 1)).alignedSlice(alignment);
    return new ByteBufferOffHeapStore(buf);
  }
}
