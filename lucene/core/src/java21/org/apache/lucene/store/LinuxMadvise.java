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
import java.lang.foreign.Arena;
import java.util.concurrent.atomic.AtomicLong;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Files;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Locale;
import java.util.Optional;
import java.util.logging.Logger;
import org.apache.lucene.util.Constants;

/**
 * Linux-specific {@link BlockCacheMmapProvider} using a single contiguous native mapping per
 * backing store, per-block {@code madvise} hints, and {@code msync} for writeback.
 *
 * <p>Each call to {@link #getInstance()} returns a fresh factory instance. {@link #open}
 * constructs a fully-initialized {@link Mapping} with final fields.
 *
 * <p>Loaded reflectively by {@link BlockCacheMmapProvider#getDefault()} on Java 21+ Linux.
 */
@SuppressWarnings("preview")
final class LinuxMadvise implements BlockCacheMmapProvider {

  private static final Logger LOG = Logger.getLogger(LinuxMadvise.class.getName());

  // --- shared (class-level) method handles ---
  private static final MethodHandle MH$madvise;
  private static final MethodHandle MH$msync;
  private static final boolean AVAILABLE;

  // madvise advice values (used by Mapping)
  private static final int MADV_WILLNEED = 3;
  private static final int MADV_COLD = 20;
  private static final int MADV_REMOVE = 9;

  // msync flags
  private static final int MS_SYNC = 4;

  private LinuxMadvise() {}

  static {
    MethodHandle advise = null, sync = null;
    boolean available = false;
    if (Constants.LINUX) {
      try {
        Linker linker = Linker.nativeLinker();
        SymbolLookup stdlib = linker.defaultLookup();
        advise =
            findFunction(
                linker, stdlib, "madvise",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT));
        sync =
            findFunction(
                linker, stdlib, "msync",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT));
        available = true;
        LOG.info("LinuxMadvise: native madvise/msync available");
      } catch (UnsupportedOperationException uoe) {
        LOG.warning("LinuxMadvise unavailable: " + uoe.getMessage());
      } catch (
          @SuppressWarnings("unused")
          IllegalCallerException ice) {
        LOG.warning(
            String.format(
                Locale.ENGLISH,
                "LinuxMadvise requires native access; pass: --enable-native-access=%s",
                Optional.ofNullable(LinuxMadvise.class.getModule().getName())
                    .orElse("ALL-UNNAMED")));
      }
    }
    MH$madvise = advise;
    MH$msync = sync;
    AVAILABLE = available;
  }

  /** Returns a fresh {@link BlockCacheMmapProvider} factory instance, or empty if unavailable. */
  static Optional<BlockCacheMmapProvider> getInstance() {
    if (!AVAILABLE) return Optional.empty();
    return Optional.of(new LinuxMadvise());
  }

  private static MethodHandle findFunction(
      Linker linker, SymbolLookup lookup, String name, FunctionDescriptor desc,
      Linker.Option... options) {
    MemorySegment symbol =
        lookup
            .find(name)
            .orElseThrow(
                () -> new UnsupportedOperationException("No symbol '" + name + "' in libc"));
    return linker.downcallHandle(symbol, desc, options);
  }

  // --- BlockCacheMmapProvider implementation ---

  @Override
  public BlockCacheMapping open(Path path, int blockSize, int nBlocks) throws IOException {
    boolean removeSupported = probeRemove(path.getParent(), blockSize);
    long dataSize = (long) nBlocks * blockSize;
    Arena arena = Arena.ofShared();
    boolean success = false;
    try {
      MemorySegment seg = mapStore(path, dataSize, arena);
      ByteBuffer[] pool = buildPool(seg, nBlocks, blockSize);
      success = true;
      return new Mapping(arena, seg.address(), blockSize, dataSize, pool, removeSupported);
    } finally {
      if (!success) arena.close();
    }
  }

  @SuppressWarnings("restricted")
  private static boolean probeRemove(Path dir, int blockSize) {
    try {
      Path tmp = Files.createTempFile(dir, ".madv-probe-", null);
      try {
        try (FileChannel fc =
                FileChannel.open(tmp, StandardOpenOption.READ, StandardOpenOption.WRITE);
            Arena arena = Arena.ofConfined()) {
          fc.truncate(blockSize);
          MemorySegment seg = fc.map(MapMode.READ_WRITE, 0L, blockSize, arena);
          int ret = (int) MH$madvise.invokeExact(seg, (long) blockSize, MADV_REMOVE);
          if (ret != 0) LOG.info("MADV_REMOVE probe returned " + ret + "; falling back to MADV_WILLNEED for prepareWrite");
          return ret == 0;
        }
      } finally {
        Files.delete(tmp);
      }
    } catch (Throwable t) {
      LOG.info("MADV_REMOVE probe failed (" + t + "); falling back to MADV_WILLNEED for prepareWrite");
      return false;
    }
  }


  // --- internal helpers (static; used by both the factory and the static inner Mapping) ---

  @SuppressWarnings("restricted")
  private static MemorySegment mapStore(Path path, long totalSize, Arena arena) throws IOException {
    try (FileChannel fc =
        FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
      return fc.map(MapMode.READ_WRITE, 0L, totalSize, arena);
    }
  }

  /** Builds the partition pool from a fully-mapped segment. */
  private static ByteBuffer[] buildPool(MemorySegment seg, int nBlocks, int blockSize) {
    final long blockSizeL = blockSize;
    final int maxBlocksPerPartition = Integer.highestOneBit(Integer.MAX_VALUE / blockSize);
    final long partitionMaxBytes = ((maxBlocksPerPartition * blockSizeL) >> 21) << 21;
    final int effectiveMax = Math.toIntExact(partitionMaxBytes / blockSizeL);
    final int numPartitions = ((nBlocks - 1) / effectiveMax) + 1;
    ByteBuffer[] pool = new ByteBuffer[numPartitions];
    for (int i = numPartitions - 1, partBlocks = ((nBlocks - 1) % effectiveMax) + 1;
        i >= 0;
        i--) {
      long offset = (long) i * partitionMaxBytes;
      long bytes = (long) partBlocks * blockSizeL;
      pool[i] = seg.asSlice(offset, bytes).asByteBuffer();
      partBlocks = effectiveMax;
    }
    return pool;
  }

  // --- Mapping: fully-initialized, immutable; final fields ---

  @SuppressWarnings({"preview", "restricted"})
  private static final class Mapping implements BlockCacheMapping {

    private final Arena arena;
    private final long base;
    private final int blockSize;
    private final long dataSize;
    private final ByteBuffer[] pool;
    private final boolean removeSupported;
    private final AtomicLong prepareWriteCount = new AtomicLong();

    Mapping(Arena arena, long base, int blockSize, long dataSize, ByteBuffer[] pool, boolean removeSupported) {
      this.arena = arena;
      this.base = base;
      this.blockSize = blockSize;
      this.dataSize = dataSize;
      this.pool = pool;
      this.removeSupported = removeSupported;
    }

    @Override
    public ByteBuffer[] dataPool() {
      return pool;
    }

    @Override
    public void loadHint(int blockIdx) {
      madvise(blockIdx, MADV_WILLNEED);
    }

    @Override
    public void release(int blockIdx) {
      madvise(blockIdx, MADV_COLD);
    }

    @Override
    public void prepareWrite(int blockIdx) {
      int advice = removeSupported ? MADV_REMOVE : MADV_WILLNEED;
      long n = prepareWriteCount.incrementAndGet();
      if (n == 1 || n % 1000 == 0) {
        LOG.info(String.format(Locale.ENGLISH, "prepareWrite #%d blockIdx=%d advice=%d (removeSupported=%b)", n, blockIdx, advice, removeSupported));
      }
      madvise(blockIdx, advice);
    }

    @Override
    public void force() throws IOException {
      try {
        MemorySegment addr = MemorySegment.ofAddress(base);
        int ret = (int) MH$msync.invokeExact(addr, dataSize, MS_SYNC);
        if (ret != 0) {
          throw new IOException(
              String.format(
                  Locale.ENGLISH,
                  "msync(0x%08X, %d, MS_SYNC) failed with return code %d",
                  base, dataSize, ret));
        }
      } catch (IOException e) {
        throw e;
      } catch (Throwable t) {
        throw new AssertionError(t);
      }
    }

    @Override
    public void close() throws IOException {
      arena.close();
    }

    private void madvise(int blockIdx, int advice) {
      try {
        MemorySegment addr = MemorySegment.ofAddress(base + (long) blockIdx * blockSize);
        int ret = (int) MH$madvise.invokeExact(addr, (long) blockSize, advice);
        if (ret != 0) {
          LOG.info(
              String.format(
                  Locale.ENGLISH,
                  "madvise(blockIdx=%d, advice=%d) returned %d",
                  blockIdx, advice, ret));
        }
      } catch (Throwable t) {
        throw new AssertionError(t);
      }
    }
  }
}
