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

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Factory that opens a {@link BlockCacheMapping} over a backing store. Implementations own
 * platform-specific mapping strategy (e.g., {@link MappedByteBuffer} partitions or a single
 * contiguous native mmap). The returned {@link BlockCacheMapping} is fully initialized and
 * immutable.
 *
 * <p>Loaded reflectively by {@link #getDefault()} on Java 21+ Linux.
 */
public interface BlockCacheMmapProvider {

  /**
   * Maps the data region of a backing file, covering exactly {@code nBlocks * blockSize} bytes
   * starting at offset 0. The caller is responsible for computing {@code nBlocks} and for mapping
   * any metadata region separately.
   */
  BlockCacheMapping open(Path path, int blockSize, int nBlocks) throws IOException;

  /**
   * Returns a new provider instance appropriate for this platform. Attempts to load {@code
   * LinuxMadvise}; falls back to {@link MappedByteBufferProvider}.
   */
  static BlockCacheMmapProvider getDefault() {
    try {
      Class<?> cls = Class.forName("org.apache.lucene.store.LinuxMadvise");
      @SuppressWarnings("unchecked")
      Optional<BlockCacheMmapProvider> opt =
          (Optional<BlockCacheMmapProvider>) cls.getMethod("getInstance").invoke(null);
      if (opt.isPresent()) {
        BlockCacheMmapProvider candidate = opt.get();
        MappedByteBufferProvider fallback = new MappedByteBufferProvider();
        return (path, blockSize, nBlocks) -> {
          try {
            return candidate.open(path, blockSize, nBlocks);
          } catch (UnsupportedOperationException e) {
            // Arena-based mmap not supported on this filesystem (e.g. overlayfs/Docker);
            // fall back to MappedByteBuffer partitions.
            return fallback.open(path, blockSize, nBlocks);
          }
        };
      }
    } catch (@SuppressWarnings("unused") Exception ignored) {
    }
    return new MappedByteBufferProvider();
  }
}
