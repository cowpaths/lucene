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
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Optional;
import java.util.logging.Logger;
import org.apache.lucene.util.Constants;

/**
 * Linux-specific {@link BlockCacheMmapProvider} using a single contiguous native mapping per
 * backing store, per-block {@code madvise} hints, and {@code msync} for writeback. Supports both
 * filesystem files (via {@link FileChannel#map}) and raw block devices (via native {@code mmap}).
 *
 * <p>Block device size is queried via {@link RandomAccessFile#length()}, which uses {@code lseek}
 * and correctly returns the device size on Linux (JDK-8266610, fixed in JDK 17).
 *
 * <p>Each call to {@link #getInstance()} returns a fresh factory instance. {@link #open} and
 * {@link #openEphemeral} construct a fully-initialized {@link Mapping} with final fields.
 *
 * <p>Loaded reflectively by {@link BlockCacheMmapProvider#getDefault()} on Java 21+ Linux.
 */
@SuppressWarnings("preview")
final class LinuxMadvise implements BlockCacheMmapProvider {

  private static final Logger LOG = Logger.getLogger(LinuxMadvise.class.getName());

  // --- shared (class-level) method handles ---
  private static final MethodHandle MH$madvise;
  private static final MethodHandle MH$msync;
  private static final MethodHandle MH$open;
  private static final MethodHandle MH$mmap;
  private static final MethodHandle MH$munmap;
  private static final MethodHandle MH$close;
  private static final boolean AVAILABLE;

  // madvise advice values (used by Mapping)
  private static final int MADV_WILLNEED = 3;
  private static final int MADV_COLD = 20;

  // msync flags
  private static final int MS_SYNC = 4;

  // mmap constants
  private static final int O_RDWR = 2;
  private static final int PROT_READ = 1;
  private static final int PROT_WRITE = 2;
  private static final int MAP_SHARED = 1;
  private static final long MAP_FAILED = -1L;

  private LinuxMadvise() {}

  static {
    MethodHandle advise = null, sync = null, open = null, mmap = null, munmap = null, close = null;
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
        open =
            findFunction(
                linker, stdlib, "open",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT));
        mmap =
            findFunction(
                linker, stdlib, "mmap",
                FunctionDescriptor.of(
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
                    ValueLayout.JAVA_INT, ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG));
        munmap =
            findFunction(
                linker, stdlib, "munmap",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
        close =
            findFunction(
                linker, stdlib, "close",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
        available = true;
        LOG.info("LinuxMadvise: native madvise/msync/mmap available");
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
    MH$open = open;
    MH$mmap = mmap;
    MH$munmap = munmap;
    MH$close = close;
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
  public BlockCacheMapping open(Path path, int blockSize, int metaBytesPerBlock, int trailerBytes)
      throws IOException {
    long totalSize = backingStoreSize(path);
    long dataPerBlock = (long) blockSize + metaBytesPerBlock;
    int nBlocks = Math.toIntExact((totalSize - trailerBytes) / dataPerBlock);
    long ds = (long) nBlocks * blockSize;
    long metaSize = (long) nBlocks * metaBytesPerBlock + trailerBytes;
    Arena arena = Arena.ofShared();
    boolean success = false;
    try {
      MemorySegment seg = mapStore(path, totalSize, arena);
      ByteBuffer[] pool = buildPool(seg, nBlocks, blockSize);
      ByteBuffer metaBuf =
          seg.asSlice(ds, metaSize).asByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
      success = true;
      return new Mapping(nBlocks, arena, seg.address(), blockSize, ds, metaSize, pool, metaBuf);
    } finally {
      if (!success) arena.close();
    }
  }

  @Override
  public BlockCacheMapping openEphemeral(Path path, int blockSize, long targetBytes)
      throws IOException {
    int nBlocks = Math.toIntExact(targetBytes / blockSize);
    long ds = (long) nBlocks * blockSize;
    Arena arena = Arena.ofShared();
    boolean success = false;
    try {
      try (FileChannel fc =
          FileChannel.open(
              path,
              EnumSet.of(
                  StandardOpenOption.CREATE_NEW,
                  StandardOpenOption.READ,
                  StandardOpenOption.WRITE))) {
        fc.truncate(ds);
        MemorySegment seg = fc.map(MapMode.READ_WRITE, 0L, ds, arena);
        ByteBuffer[] pool = buildPool(seg, nBlocks, blockSize);
        success = true;
        return new Mapping(nBlocks, arena, seg.address(), blockSize, ds, 0L, pool, null);
      }
    } finally {
      Files.delete(path);
      if (!success) arena.close();
    }
  }

  // --- internal helpers (static; used by both the factory and the static inner Mapping) ---

  /**
   * Returns the byte size of the backing store. Works for both regular files and block devices:
   * {@link RandomAccessFile#length()} uses {@code lseek(SEEK_END)}, which correctly returns the
   * device size on Linux (JDK-8266610).
   */
  private static long backingStoreSize(Path path) throws IOException {
    try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
      return raf.length();
    }
  }

  /**
   * Maps the entire backing store as a single contiguous {@link MemorySegment}. Uses {@link
   * FileChannel#map} for regular files; native {@code mmap} for block devices (which {@link
   * FileChannel#map} cannot handle as it relies on {@code fstat} for size validation).
   */
  @SuppressWarnings("restricted")
  private static MemorySegment mapStore(Path path, long totalSize, Arena arena) throws IOException {
    if (Files.isRegularFile(path)) {
      try (FileChannel fc =
          FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
        return fc.map(MapMode.READ_WRITE, 0L, totalSize, arena);
      }
    }
    // block device: native open + mmap; fd can be closed immediately after mmap
    try {
      int fd;
      try (Arena tmp = Arena.ofConfined()) {
        MemorySegment pathStr = tmp.allocateUtf8String(path.toString());
        fd = (int) MH$open.invokeExact(pathStr, O_RDWR);
      }
      if (fd < 0) {
        throw new IOException("open(" + path + ") failed");
      }
      MemorySegment mapped;
      try {
        MemorySegment result =
            (MemorySegment)
                MH$mmap.invokeExact(
                    MemorySegment.NULL, totalSize,
                    PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0L);
        if (result.address() == MAP_FAILED) {
          throw new IOException("mmap(" + path + ", " + totalSize + ") failed");
        }
        final long addr = result.address();
        mapped =
            MemorySegment.ofAddress(addr)
                .reinterpret(
                    totalSize,
                    arena,
                    seg -> {
                      try {
                        MH$munmap.invokeExact(seg, totalSize);
                      } catch (Throwable t) {
                        LOG.warning("munmap failed: " + t);
                      }
                    });
      } finally {
        MH$close.invokeExact(fd);
      }
      return mapped;
    } catch (IOException e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError(t);
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

    private final int nBlocks;
    private final Arena arena;
    private final long base;
    private final int blockSize;
    private final long dataSize;
    private final long metaSize; // 0 for ephemeral
    private final ByteBuffer[] pool;
    private final ByteBuffer metaBuf; // null for ephemeral

    Mapping(
        int nBlocks,
        Arena arena,
        long base,
        int blockSize,
        long dataSize,
        long metaSize,
        ByteBuffer[] pool,
        ByteBuffer metaBuf) {
      this.nBlocks = nBlocks;
      this.arena = arena;
      this.base = base;
      this.blockSize = blockSize;
      this.dataSize = dataSize;
      this.metaSize = metaSize;
      this.pool = pool;
      this.metaBuf = metaBuf;
    }

    @Override
    public int nBlocks() {
      return nBlocks;
    }

    @Override
    public ByteBuffer[] dataPool() {
      return pool;
    }

    @Override
    public ByteBuffer metaBuf() {
      return metaBuf;
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
    public void force() throws IOException {
      try {
        MemorySegment addr = MemorySegment.ofAddress(base).reinterpret(dataSize);
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
    public void forceMetaBuf() throws IOException {
      if (metaSize == 0) return;
      try {
        long addr = base + dataSize;
        MemorySegment segment = MemorySegment.ofAddress(addr).reinterpret(metaSize);
        int ret = (int) MH$msync.invokeExact(segment, metaSize, MS_SYNC);
        if (ret != 0) {
          throw new IOException(
              String.format(
                  Locale.ENGLISH,
                  "msync(meta, 0x%08X, %d, MS_SYNC) failed with return code %d",
                  addr, metaSize, ret));
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
        long address = base + (long) blockIdx * blockSize;
        MemorySegment addr = MemorySegment.ofAddress(address).reinterpret(blockSize);
        int ret = (int) MH$madvise.invokeExact(addr, (long) blockSize, advice);
        if (ret != 0) {
          LOG.fine(
              () ->
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
