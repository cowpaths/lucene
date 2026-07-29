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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A fully-initialized, immutable view of a {@link BlockCacheMmapProvider} mapping. Instances are
 * returned by the {@code open} factory methods on {@link BlockCacheMmapProvider} and own the full
 * lifecycle of the backing mapping — both the data region and, for persistent caches, the metadata
 * region.
 *
 * <p>Lifecycle: one of the {@link BlockCacheMmapProvider} {@code open} methods constructs an
 * instance and populates all state. Hint calls, {@link #force}, and {@link #close} operate on
 * the immutable retained state.
 */
public interface BlockCacheMapping extends Closeable {

  /** Data pool, indexed by partition. Always non-null after construction. */
  ByteBuffer[] dataPool();

  /**
   * Hints that block {@code blockIdx} will be needed soon ({@code MADV_WILLNEED}). No-op in the
   * default implementation.
   */
  default void loadHint(int blockIdx) {}

  /**
   * Hints that block {@code blockIdx} is unlikely to be needed soon ({@code MADV_COLD}). No-op in
   * the default implementation.
   */
  default void release(int blockIdx) {}

  /** Forces all mapped data pages to the underlying storage device. */
  void force() throws IOException;

  /** Releases resources held by this mapping. No-op in the default implementation. */
  @Override
  default void close() throws IOException {}
}
