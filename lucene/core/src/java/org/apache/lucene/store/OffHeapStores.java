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

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;

/**
 * Factory for {@link OffHeapStore} instances. Prefers a {@code MemorySegment}-backed
 * implementation (JDK 21+, loaded reflectively -- see below) and falls back to a plain {@link
 * ByteBuffer}-backed implementation ({@link ByteBufferOffHeapStore}, always available) when the
 * former can't be loaded, mirroring exactly {@link BlockCacheMmapProvider#getDefault()}'s
 * reflective-load-with-fallback shape for {@code LinuxMadvise}/{@link MappedByteBufferProvider}.
 *
 * <p><b>Current status (stepping stone):</b> this factory does not yet implement the eventual
 * Holder-pattern singleton selection (first caller gets a JIT-foldable {@code static final}
 * instance; subsequent callers get a generic, non-static-final-backed instance -- see {@code
 * solr/prodbench/FINDINGS.md} for why that split matters for a {@code MemorySegment}-backed store
 * specifically). {@link #create} simply constructs a fresh instance on every call. The Holder split
 * is deferred until real multi-partition usage (e.g. Solr's {@code BlockCache}) is wired up against
 * this factory -- premature here, since there is no real caller yet to validate the split against.
 */
public final class OffHeapStores {
  private static final String MEMORY_SEGMENT_IMPL_CLASS =
      "org.apache.lucene.store.MemorySegmentOffHeapStore";

  private static final Constructor<? extends OffHeapStore> MEMORY_SEGMENT_CTOR =
      lookupMemorySegmentCtor();

  @SuppressWarnings("unchecked")
  private static Constructor<? extends OffHeapStore> lookupMemorySegmentCtor() {
    try {
      Class<?> cls = Class.forName(MEMORY_SEGMENT_IMPL_CLASS);
      return (Constructor<? extends OffHeapStore>)
          cls.getConstructor(int.class, int.class, int.class);
    } catch (@SuppressWarnings("unused") ReflectiveOperationException e) {
      return null;
    }
  }

  private OffHeapStores() {}

  /**
   * Allocates a new {@link OffHeapStore} with at least {@code slots * stride} bytes of backing
   * storage, aligned to a full {@code alignment}-byte boundary (a hard guarantee -- see {@code
   * Cache2#allocateAlignedDirect}'s javadoc in {@code solr/core} for the margin/rounding rationale
   * this mirrors).
   */
  public static OffHeapStore create(int slots, int stride, int alignment) {
    if (MEMORY_SEGMENT_CTOR != null) {
      try {
        return MEMORY_SEGMENT_CTOR.newInstance(slots, stride, alignment);
      } catch (@SuppressWarnings("unused") ReflectiveOperationException ignored) {
        // Fall through to the ByteBuffer fallback below -- e.g. a JDK whose MR-JAR versioned
        // class loaded but whose runtime behavior didn't match expectations.
      }
    }
    return new ByteBufferOffHeapStore(allocateAlignedDirect(slots, stride, alignment));
  }

  private static ByteBuffer allocateAlignedDirect(int slots, int stride, int alignment) {
    int needed = Math.multiplyExact(slots, stride);
    return ByteBuffer.allocateDirect(needed + 2 * (alignment - 1)).alignedSlice(alignment);
  }
}
