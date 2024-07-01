package org.apache.lucene.demo;

import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodType.methodType;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Jvm-only class (no dependencies) to illustrate performance of MMap.unmap vs. Arena.close().
 * <code>
 * java -cp [classpath_root] org.apache.lucene.demo.ConcurrentMmapOrArenaClose [bool_mmap] [n_threads] [n_idle_threads] [n_reps] [bool_semaphore]
 * </code>
 *
 * <p>where:
 *
 * <ol>
 *   <li>[bool_mmap] if false uses Arena, if true uses legacy mmap
 *   <li>[n_threads] number of threads concurrently opening and closing mmaps/shared arenas
 *   <li>[n_idle_threads] number of threads the hold a region open but idle, closing only once all
 *       [n_threads] have completed
 *   <li>[n_reps] the number of times that each of [n_threads] will open/close mmap/shared arena
 *   <li>[bool_semaphore] for Arena, should access to Arena.close() be protected by a global
 *       semaphore?
 * </ol>
 */
public class ConcurrentMmapOrArenaClose {
  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException {
    Path p = Path.of("/tmp/concurrentClose.txt");
    File f = p.toFile();
    if (!f.exists()) {
      f.deleteOnExit();
      try (FileOutputStream out = new FileOutputStream(f)) {
        // It seems that the size of the mapped file doesn't matter, so just do something
        // nominal. To experiment with other sizes, simply manually create a larger file
        // place it at the `/tmp/concurrentClose.txt` path.
        out.write("nominal\n".getBytes(StandardCharsets.UTF_8));
      }
    }
    final boolean mmap = Boolean.parseBoolean(args[0]);
    final int nThreads = Integer.parseInt(args[1]);
    final int nIdleThreads = Integer.parseInt(args[2]);
    final int nReps = Integer.parseInt(args[3]);
    final boolean semaphore = Boolean.parseBoolean(args[4]);

    final long length = f.length();
    try (ExecutorService exec = Executors.newFixedThreadPool(nThreads + nIdleThreads)) {
      CountDownLatch cdlReady = new CountDownLatch(nThreads + nIdleThreads);
      CountDownLatch cdlStart = new CountDownLatch(1);
      CountDownLatch cdl = new CountDownLatch(1);
      List<Future<?>> futures = new ArrayList<>(nThreads);
      long start;
      try (FileChannel fc = FileChannel.open(p, StandardOpenOption.READ)) {
        for (int i = 0; i < nIdleThreads; i++) {
          exec.submit(
              () -> {
                cdlReady.countDown();
                cdlStart.await();
                try (AutoCloseable c =
                    mmap ? doMmap(fc, length) : doMemorySegment(fc, length, semaphore)) {
                  cdl.await();
                }
                return null;
              });
        }
        for (int i = 0; i < nThreads; i++) {
          futures.add(
              exec.submit(
                  () -> {
                    cdlReady.countDown();
                    cdlStart.await();
                    for (int j = 0; j < nReps; j++) {
                      (mmap ? doMmap(fc, length) : doMemorySegment(fc, length, semaphore)).close();
                    }
                    return null;
                  }));
        }
        cdlReady.await();
        start = System.nanoTime();
        cdlStart.countDown();
        for (Future<?> future : futures) {
          future.get();
        }
      } finally {
        cdl.countDown();
      }
      System.err.println(
          "duration: " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + "ms");
    }
  }

  private static AutoCloseable doMmap(FileChannel fc, long length) throws IOException {
    BufferCleaner bufferCleaner = (BufferCleaner) unmapHackImpl();
    MappedByteBuffer map = fc.map(FileChannel.MapMode.READ_ONLY, 0, length);
    return () -> bufferCleaner.freeBuffer("", map);
  }

  private static final Semaphore CLOSE = new Semaphore(1);

  private static AutoCloseable doMemorySegment(FileChannel fc, long length, boolean semaphore)
      throws IOException {
    Arena a = Arena.ofShared();
    MemorySegment map = fc.map(FileChannel.MapMode.READ_ONLY, 0, length, a);
    if (semaphore) {
      return () -> {
        CLOSE.acquire();
        try {
          a.close();
        } finally {
          CLOSE.release();
        }
      };
    } else {
      return a;
    }
  }

  // Below is copied over from Lucene, but without any of the Lucene dependencies, to make this
  // a straight-jvm class with no dependencies.

  public static Object unmapHackImpl() {
    final MethodHandles.Lookup lookup = lookup();
    try {
      // *** sun.misc.Unsafe unmapping (Java 9+) ***
      final Class<?> unsafeClass = lookup.findClass("sun.misc.Unsafe");
      // first check if Unsafe has the right method, otherwise we can give up
      // without doing any security critical stuff:
      final MethodHandle unmapper =
          lookup.findVirtual(
              unsafeClass, "invokeCleaner", methodType(void.class, ByteBuffer.class));
      // fetch the unsafe instance and bind it to the virtual MH:
      final Field f = unsafeClass.getDeclaredField("theUnsafe");
      f.setAccessible(true);
      final Object theUnsafe = f.get(null);
      return newBufferCleaner(unmapper.bindTo(theUnsafe));
    } catch (SecurityException se) {
      return "Unmapping is not supported, because not all required permissions are given to the Lucene JAR file: "
          + se
          + " [Please grant at least the following permissions: RuntimePermission(\"accessClassInPackage.sun.misc\") "
          + " and ReflectPermission(\"suppressAccessChecks\")]";
    } catch (ReflectiveOperationException | RuntimeException e) {
      throw new RuntimeException(e);
    }
  }

  private static BufferCleaner newBufferCleaner(final MethodHandle unmapper) {
    assert Objects.equals(methodType(void.class, ByteBuffer.class), unmapper.type());
    return (String resourceDescription, ByteBuffer buffer) -> {
      if (!buffer.isDirect()) {
        throw new IllegalArgumentException("unmapping only works with direct buffers");
      }
      try {
        unmapper.invokeExact(buffer);
      } catch (Throwable t) {
        throw new IOException("Unable to unmap the mapped buffer: " + resourceDescription, t);
      }
    };
  }

  @FunctionalInterface
  private interface BufferCleaner {
    void freeBuffer(String resourceDescription, ByteBuffer b) throws IOException;
  }
}
