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
 * <p>The backing store layout is:
 *
 * <pre>
 *   [0, nBlocks * blockSize)                  — data region
 *   [nBlocks * blockSize, total)               — metadata + trailer
 * </pre>
 *
 * <p>Loaded reflectively by {@link #getDefault()} on Java 21+ Linux.
 */
public interface BlockCacheMmapProvider {

  /**
   * Opens an existing backing store (file or block device), infers {@code nBlocks} from its total
   * size using the formula {@code (totalSize - trailerBytes) / (blockSize + metaBytesPerBlock)},
   * and maps both the data and metadata regions.
   *
   * <p>{@link BlockCacheMapping#metaBuf()} will be non-null and ordered {@link
   * java.nio.ByteOrder#LITTLE_ENDIAN} on the returned mapping.
   */
  BlockCacheMapping open(Path path, int blockSize, int metaBytesPerBlock, int trailerBytes)
      throws IOException;

  /**
   * Creates an ephemeral (non-persistent) backing file at {@code path}, sizes it to hold {@code
   * targetBytes / blockSize} blocks of data, maps the data region only, then deletes the file.
   * {@link BlockCacheMapping#metaBuf()} is {@code null} on the returned mapping.
   */
  BlockCacheMapping openEphemeral(Path path, int blockSize, long targetBytes) throws IOException;

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
        return opt.get();
      }
    } catch (@SuppressWarnings("unused") Exception ignored) {
    }
    return new MappedByteBufferProvider();
  }
}
