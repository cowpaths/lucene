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
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Default {@link BlockCacheMmapProvider} backed by {@link MappedByteBuffer}. Maps the data region
 * in partitions to stay within the {@code int}-indexed limit of {@link MappedByteBuffer}. No
 * contiguous base address is available, so hint methods are no-ops and only filesystem paths are
 * supported.
 */
final class MappedByteBufferProvider implements BlockCacheMmapProvider {

  @Override
  public BlockCacheMapping open(Path path, int blockSize, int nBlocks) throws IOException {
    long dataSize = (long) nBlocks * blockSize;
    try (FileChannel fc =
        FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
      MappedByteBuffer[] pool = mapPartitions(fc, nBlocks, blockSize, dataSize);
      return new Mapping(pool);
    }
  }


  private static MappedByteBuffer[] mapPartitions(
      FileChannel fc, int nBlocks, int blockSize, long dataSize) throws IOException {
    final long blockSizeL = blockSize;
    final int maxBlocksPerPartition = Integer.highestOneBit(Integer.MAX_VALUE / blockSize);
    final long partitionMaxBytes = ((maxBlocksPerPartition * blockSizeL) >> 21) << 21;
    final int effectiveMaxBlocksPerPartition = Math.toIntExact(partitionMaxBytes / blockSizeL);
    final int numPartitions = ((nBlocks - 1) / effectiveMaxBlocksPerPartition) + 1;
    final MappedByteBuffer[] pool = new MappedByteBuffer[numPartitions];
    // iterate from high to low so the (possibly smaller) remainder partition is handled first
    for (int i = numPartitions - 1,
            partitionNumBlocks = ((nBlocks - 1) % effectiveMaxBlocksPerPartition) + 1;
        i >= 0;
        i--) {
      pool[i] =
          fc.map(
              MapMode.READ_WRITE,
              (long) i * partitionMaxBytes,
              partitionNumBlocks * blockSizeL);
      partitionNumBlocks = effectiveMaxBlocksPerPartition;
    }
    return pool;
  }

  private static final class Mapping implements BlockCacheMapping {
    private final MappedByteBuffer[] pool;

    Mapping(MappedByteBuffer[] pool) {
      this.pool = pool;
    }

    @Override
    public ByteBuffer[] dataPool() {
      return pool;
    }

    @Override
    public void force() throws IOException {
      for (MappedByteBuffer bb : pool) bb.force();
    }

    @Override
    public String toString() {
      return "MappedByteBufferProvider.Mapping[partitions=" + pool.length + "]";
    }

    // MappedByteBuffers are released by the GC; no explicit unmap needed.
    @Override
    public void close() {}
  }
}
