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
package org.apache.lucene.index;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.IOFunction;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.RamUsageEstimator;

public class TestUnloader extends LuceneTestCase {
  private static final class MyCloseable implements Closeable {

    private final LongAdder tracker;
    private final AtomicInteger count;
    private final LongAdder closedCt;
    private final AtomicInteger exceptionOnClose = new AtomicInteger();

    private MyCloseable(LongAdder tracker, AtomicInteger count, LongAdder closedCt) {
      if (random().nextInt(20) == 0) {
        throw new RuntimeException("exception opening");
      }
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      tracker.increment();
      this.tracker = tracker;
      this.count = count;
      this.closedCt = closedCt;
    }

    @Override
    public void close() throws IOException {
      int v = random().nextInt(20) == 0 ? 1 : 2;
      switch (exceptionOnClose.compareAndExchange(0, v)) {
        case 0:
          // first time here
          break;
        case 1:
          return; // no exception
        case 2:
          throw new RuntimeException("exception closing");
        default:
          throw new IllegalStateException();
      }
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      tracker.decrement();
      closedCt.increment();
      if (v == 2) {
        throw new RuntimeException("exception closing");
      }
    }
  }

  private static final IOFunction<MyCloseable, String> NO_OP = (c) -> null;

  public void test() throws IOException, InterruptedException {
    for (int i = 0; i < 10; i++) {
      System.out.println("do " + i);
      doTest();
    }
  }

  private static Unloader<MyCloseable> newInstance(
      LongAdder tracker, AtomicInteger count, LongAdder createdCt, LongAdder closedCt) {
    while (true) {
      try {
        return new Unloader<>(
            new Unloader.AbstractUnloadHelper(null, InfoStream.NO_OUTPUT) {},
            (unloader) -> {
              MyCloseable ret = new MyCloseable(tracker, count, closedCt);
              createdCt.increment();
              return ret;
            },
            0,
            NO_OP);
      } catch (
          @SuppressWarnings("unused")
          Exception ex) {
        // keep trying
      }
    }
  }

  public void doTest() throws IOException, InterruptedException {
    final int nThreadsPerOp = 2;
    final LongAdder tracker = new LongAdder();
    final AtomicInteger count = new AtomicInteger();
    final LongAdder createdCt = new LongAdder();
    final LongAdder closedCt = new LongAdder();
    Unloader<MyCloseable> u = newInstance(tracker, count, createdCt, closedCt);
    ExecutorService exec =
        Executors.newFixedThreadPool(nThreadsPerOp + 1, new NamedThreadFactory("testUnloader"));
    AtomicBoolean complete = new AtomicBoolean();
    LongAdder check = new LongAdder();
    try {
      List<Future<String>> futures = new ArrayList<>(nThreadsPerOp + 1);
      for (int i = nThreadsPerOp; i > 0; i--) {
        futures.add(
            exec.submit(
                () -> {
                  while (!complete.get()) {
                    Thread.sleep(20);
                    @SuppressWarnings("unused")
                    Object o =
                        u.execute(
                            (a, b) -> {
                              a.count.incrementAndGet();
                              return null;
                            },
                            null);
                    check.increment();
                  }
                  return "execute";
                }));
      }
      futures.add(
          exec.submit(
              () -> {
                while (!complete.get()) {
                  try {
                    u.maybeUnload();
                  } catch (
                      @SuppressWarnings("unused")
                      AlreadyClosedException ex) {
                    break;
                  } catch (Throwable t) {
                    if (!"exception closing".equals(t.getMessage())) {
                      t.printStackTrace(System.err);
                    }
                  }
                }
                return "unload";
              }));
      Thread.sleep(1000); // let it run for a while
      System.out.println("closing ...");
      try {
        u.close();
      } catch (RuntimeException ex) {
        if (!"exception closing".equals(ex.getMessage())) {
          throw ex;
        }
      }
      int iterations = 0;
      AssertionError deferred = null;
      boolean eventuallyOk = false;
      do {
        try {
          assertEquals(createdCt.sum(), closedCt.sum());
          assertEquals(0, tracker.sum());
          eventuallyOk = true;
        } catch (AssertionError er) {
          Thread.sleep(100);
          if (deferred == null) {
            deferred = er;
          }
        }
      } while (++iterations < 10);
      if (deferred != null) {
        System.err.println("eventually ok: " + eventuallyOk);
        throw deferred;
      }
      System.out.println(
          "closed; tracker="
              + tracker.sum()
              + ", count="
              + count.get()
              + ", "
              + createdCt.sum()
              + "?="
              + closedCt.sum());
      long now = System.nanoTime();
      long until = now + TimeUnit.SECONDS.toNanos(30);
      for (Future<String> f : futures) {
        try {
          System.out.println("\t" + f.get(until - now, TimeUnit.NANOSECONDS));
        } catch (Exception ex) {
          Throwable cause = ex.getCause();
          String msg = cause == null ? null : cause.getMessage();
          if (!(cause instanceof AlreadyClosedException)
              && !"exception opening".equals(msg)
              && !"exception closing".equals(msg)) {
            System.out.println("\t" + ex);
            ex.printStackTrace(System.out);
          }
        }
        now = System.nanoTime();
      }
    } finally {
      exec.shutdown();
      exec.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private static final int MAX_KB = 1024;
  private static final int MIN_KB = 1;
  private static final int MAX_KB_BASELINE = MAX_KB - MIN_KB + 1;

  private static final int N_SECONDS = 5;

  public void testRefQueueHandling() throws InterruptedException, ExecutionException {
    int nThreads = 20;
    final int batchSize = 1024;
    @SuppressWarnings({"unchecked", "rawtypes"})
    Consumer<Object>[] registerRef = new Consumer[1];
    LongSupplier[] outstandingSizeHolder = new LongSupplier[1];
    @SuppressWarnings({"unchecked", "rawtypes"})
    ReferenceQueue<Object>[][] removeOutstandingHolder = new ReferenceQueue[1][];
    @SuppressWarnings({"unchecked", "rawtypes"})
    AtomicReference<Boolean>[] handleRefQueueHolder = new AtomicReference[1];
    Unloader.configure(
        new Unloader.UnloadHelper() {
          @Override
          public ScheduledExecutorService onCreation(Unloader<?> u) {
            return null;
          }

          @Override
          public void maybeHandleRefQueues(
              ReferenceQueue<Object>[] queues,
              Consumer<Object> handler,
              AtomicReference<Boolean> handleRefQueue,
              LongSupplier outstandingSize) {
            handleRefQueue.set(true);
            handleRefQueueHolder[0] = handleRefQueue;
            registerRef[0] = handler;
            outstandingSizeHolder[0] = outstandingSize;
            removeOutstandingHolder[0] = queues;
          }
        });
    ReferenceQueue<Object>[] queues = removeOutstandingHolder[0];
    AtomicReference<Boolean> handleRefQueue = handleRefQueueHolder[0];
    Consumer<Object> handler = registerRef[0];
    LongSupplier outstandingSize = outstandingSizeHolder[0];

    int PARALLEL_HEAD_FACTOR = queues.length;
    ExecutorService exec =
        Executors.newFixedThreadPool(
            nThreads + PARALLEL_HEAD_FACTOR, new NamedThreadFactory("TestUnloader"));
    AtomicBoolean finished = new AtomicBoolean();
    @SuppressWarnings("rawtypes")
    Future<?>[] futures = new Future[nThreads];
    LongAdder total = new LongAdder();
    long start = System.nanoTime();
    for (int i = nThreads - 1; i >= 0; i--) {
      futures[i] =
          exec.submit(
              () -> {
                Random r = new Random(random().nextLong());
                try {
                  while (!finished.get()) {
                    for (int j = batchSize - 1; j >= 0; j--) {
                      // between 1k and 1m
                      Unloader.addDummyReference(1024 * (r.nextInt(MAX_KB_BASELINE) + MIN_KB));
                      total.increment();
                    }
                  }
                } catch (Throwable t) {
                  t.printStackTrace(System.err);
                  throw t;
                }
              });
    }
    LongAdder activeRefQueueProcessors = new LongAdder();
    LongAdder collectedRefs = new LongAdder();
    @SuppressWarnings("rawtypes")
    Future<?>[] refQueueFutures = new Future[queues.length];
    for (int i = queues.length - 1; i >= 0; i--) {
      ReferenceQueue<Object> q = queues[i];
      refQueueFutures[i] =
          exec.submit(
              () -> {
                activeRefQueueProcessors.increment();
                try {
                  while (handleRefQueue.get() == Boolean.TRUE) {
                    handler.accept(q.remove());
                    collectedRefs.increment();
                  }
                } catch (InterruptedException ex) {
                  if (handleRefQueue.get() == Boolean.TRUE) {
                    // unexpected -- we've been interrupted but are still
                    // supposed to be handling ref queue?
                    handleRefQueue.set(false);
                    System.err.println("unexpected interruption of ref queue processing");
                    ex.printStackTrace(System.err);
                    throw ex;
                  }
                } catch (Throwable t) {
                  handleRefQueue.set(false);
                  System.err.println("exception in ref queue processing");
                  t.printStackTrace(System.err);
                  throw t;
                } finally {
                  activeRefQueueProcessors.decrement();
                }
                return null;
              });
    }
    long endNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(N_SECONDS);
    long remainingNanos;
    while ((remainingNanos = endNanos - System.nanoTime()) > 0) {
      long sz = outstandingSize.getAsLong();
      System.out.println(
          "seconds remaining: "
              + TimeUnit.NANOSECONDS.toSeconds(remainingNanos)
              + ", outstandingSize="
              + sz
              + " ("
              + RamUsageEstimator.humanReadableUnits(sz * Unloader.RAMBYTES_PER_REF)
              + ")");
      Thread.sleep(Math.min(1000, TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
    }
    finished.set(true);
    long sum = total.sum();
    for (int i = nThreads - 1; i >= 0; i--) {
      futures[i].get();
    }
    System.out.println(
        "tasks completed " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
    start = System.nanoTime();
    int gcIterations = 0;
    long sz;
    while ((sz = outstandingSize.getAsLong()) > 0 || Unloader.nonEmptyRefQueueHeadCount() > 0) {
      gcIterations++;
      System.gc();
      Thread.sleep(250);
      System.err.println(
          "gc iteration "
              + gcIterations
              + ", "
              + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)
              + ", outstandingSize="
              + sz
              + ", nonEmptyRefQueueHeadCount="
              + Unloader.nonEmptyRefQueueHeadCount());
      if (gcIterations > 40) {
        fail("failed to converge");
      }
    }
    handleRefQueue.set(false);
    for (int i = refQueueFutures.length - 1; i >= 0; i--) {
      refQueueFutures[i].cancel(true);
    }
    for (int i = refQueueFutures.length - 1; i >= 0; i--) {
      int idx = i;
      expectThrows(CancellationException.class, () -> refQueueFutures[idx].get());
    }
    exec.shutdown();
    long createdSum = total.sum();
    long collectedSum = collectedRefs.sum();
    assertEquals(createdSum, collectedSum);
    System.out.println(
        "success! "
            + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)
            + " millis; throughput="
            + (sum / N_SECONDS)
            + "/s");
    System.out.println("total created=" + createdSum + ", collected=" + collectedSum);
  }
}
