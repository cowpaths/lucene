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

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.UnloaderCoordinationPoint;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOFunction;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.ThreadInterruptedException;

/** Handles thread-safe dynamic unloading and on-demand reloading of backing resource. */
public class Unloader<T extends Closeable> implements Closeable {

  private InfoStream out = InfoStream.getDefault();

  private static final DelegateFuture<Closeable> CLOSED = new DelegateFuture<>(null, null);

  static {
    CLOSED.complete(null);
  }

  private final IOFunction<Unloader<T>, T> reopen;

  private volatile long lastAccessNanos = System.nanoTime();

  private final AtomicReference<DelegateFuture<T>> backing;
  private final String description;
  private final UnloadHelper reporter;
  private final ScheduledExecutorService exec;

  private static final class DelegateFuture<T> extends CompletableFuture<WeakReference<T>>
      implements Closeable {
    private final boolean unloading;
    private final WeakReference<DelegateFuture<T>> prev;
    private final AtomicReference<WeakReference<Object>> sentinel;

    @SuppressWarnings("unused")
    private volatile T strongRef;

    @SuppressWarnings("unused")
    private DelegateFuture<T> hardRef; // kept to prevent collection

    public boolean completeStrong(T value) {
      if (super.complete(new WeakReference<>(value))) {
        strongRef = value;
        return true;
      } else {
        return false;
      }
    }

    public T getNowStrong(T valueIfAbsent) {
      T ret;
      WeakReference<T> extant = super.getNow(null);
      if (extant == null) {
        return valueIfAbsent;
      } else if ((ret = extant.get()) == null) {
        throw new NullPointerException();
      } else {
        return ret;
      }
    }

    public T getStrong(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      WeakReference<T> ref = super.get(timeout, unit);
      T ret = ref.get();
      if (ret == null) {
        throw new NullPointerException();
      } else {
        return ret;
      }
    }

    private DelegateFuture(Object initialSentinel, DelegateFuture<T> prev) {
      this.unloading = initialSentinel == null;
      this.prev = new WeakReference<>(prev);
      if (unloading) {
        this.sentinel = null;
        hardRef = prev;
        whenComplete(
            (r, e) -> {
              if (e == null) {
                // if we've completed normally (no exception), then release the hard reference
                // (we keep the reference if we complete exceptionally, because we may want to
                // retry unloading. This should be rare anyway.
                hardRef = null;
              }
            });
      } else {
        sentinel = new AtomicReference<>(new WeakReference<>(initialSentinel));
      }
    }

    /**
     * true if a reservation was acquired for this instance. Reservation release must be handled
     * elsewhere.
     */
    private Object acquire() {
      Object ret;
      boolean reusedSentinel = true;
      WeakReference<Object> candidate = sentinel.get();
      while ((ret = candidate.get()) == null) {
        if (candidate == UNLOADED_REF) {
          // unloaded while we're trying to acquire
          return null;
        }
        ret = new Object();
        WeakReference<Object> extant =
            sentinel.compareAndExchange(candidate, new WeakReference<>(ret));
        if (extant == candidate) {
          reusedSentinel = false;
          break;
        } else {
          candidate = extant;
        }
      }
      if (reusedSentinel) {
        INDIRECT_TRACK_COUNT.increment();
      } else {
        REFS_COLLECTED.increment();
      }
      return ret;
    }

    private boolean unload(boolean force) {
      assert !unloading;
      if (force) {
        sentinel.set(UNLOADED_REF);
        return true;
      } else {
        WeakReference<Object> candidate = sentinel.get();
        for (; ; ) {
          WeakReference<Object> extant;
          if (candidate == UNLOADED_REF || candidate.get() != null) {
            // either already unloaded, or still referenced
            return false;
          } else if ((extant = sentinel.compareAndExchange(candidate, UNLOADED_REF)) == candidate) {
            return true;
          } else {
            candidate = extant;
          }
        }
      }
    }

    @Override
    public void close() {
      // release so it can be GC'd
      strongRef = null;
    }
  }

  private static final WeakReference<Object> UNLOADED_REF = new WeakReference<>(null);

  @SuppressWarnings("unchecked")
  private final DelegateFuture<T> closedSentinel = (DelegateFuture<T>) CLOSED;

  private static final LongAdder REFS_COLLECTED = new LongAdder();

  private static final LongSupplier REFS_COLLECTED_COUNT_SUPPLIER = REFS_COLLECTED::sum;

  /**
   * Creates a new unloader to handle unloading and on-demand reloading a backing resource
   *
   * @param reopen function with <code>this</code> as parameter; returns a newly loaded instance of
   *     the backing resource, and (if applicable) schedules a task to check eligibility to unload
   *     at a point in the future (determined by `keepAliveNanos`)
   * @param keepAliveNanos the time threshold (in nanos) since last access, at which the backing
   *     resource will be eligible for unloading
   * @param receiveFirstInstance informs the calling (shim resource) of the first backing resource,
   *     returning a string description. This may be used to initialize state on the shim resource
   *     according to information about the backing resource. The caller should take care to not
   *     hold any references to the initial object that would prevent it from being GC'ed.
   * @throws IOException e.g., on error reading index
   */
  public Unloader(
      UnloadHelper unloadHelper,
      IOFunction<Unloader<T>, T> reopen,
      long keepAliveNanos,
      IOFunction<T, String> receiveFirstInstance)
      throws IOException {
    if (INITIALIZED_EXTERNAL.compareAndSet(false, true)) {
      unloadHelper.maybeHandleRefQueues(
          INITIALIZED_EXTERNAL, INDIRECT_TRACK_COUNT_SUPPLIER, REFS_COLLECTED_COUNT_SUPPLIER);
    }
    this.reporter = unloadHelper;
    this.reopen = reopen;
    this.keepAliveNanos = keepAliveNanos;
    Object sentinel = new Object();
    DelegateFuture<T> holder = new DelegateFuture<>(sentinel, null);
    backing = new AtomicReference<>(holder);
    this.exec = unloadHelper.onCreation(this);
    T in = reopen.apply(this);
    try {
      description = receiveFirstInstance.apply(in);
      holder.completeStrong(in);
    } catch (Throwable t) {
      try (in) {
        unloadHelper.onClose();
        throw t;
      }
    } finally {
      Reference.reachabilityFence(sentinel);
    }
  }

  private static final AtomicBoolean INITIALIZED_EXTERNAL = new AtomicBoolean(false);

  /** Sets the infostream for {@link Unloader}. */
  public void setInfoStream(InfoStream out) {
    this.out = out;
    List<String> deferred;
    if (out != InfoStream.NO_OUTPUT
        && out.isEnabled("UN")
        && (deferred = DEFERRED_INIT_MESSAGES.getAndSet(null)) != null) {
      for (String m : deferred) {
        out.message("UN", m);
      }
    }
  }

  private final long keepAliveNanos;

  /** This resource has already been unloaded */
  public static final long ALREADY_UNLOADED = -2;

  /** This resource was unloaded as a result of this invocation of {@link #maybeUnload()}. */
  public static final long UNLOADED = -1;

  /** This resource is still referenced, so was not unloaded. */
  public static final long STILL_REFERENCED = 0;

  private final Random unloadRandom = new Random(); // single-threaded access

  private static boolean injectDelay(Random r, int oneIn, int millis) {
    if (r.nextInt(oneIn) == 0) {
      try {
        Thread.sleep(millis);
      } catch (
          @SuppressWarnings("unused")
          InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    return true;
  }

  private static <T extends Closeable> DelegateFuture<T> unloadRef(
      AtomicReference<DelegateFuture<T>> ref, boolean[] unloading) {
    DelegateFuture<T> extant;
    while (!(extant = ref.get()).unloading) {
      if (!extant.unload(false)) {
        // still referenced
        return null;
      }
      DelegateFuture<T> candidate = new DelegateFuture<>(null, extant);
      if (ref.compareAndSet(extant, candidate)) {
        return candidate;
      }
    }
    if (extant == CLOSED) {
      throw new AlreadyClosedException("");
    }
    // already unloading
    unloading[0] = true;
    return null;
  }

  private static <T extends Closeable> Map.Entry<DelegateFuture<T>, Object> loadRef(
      AtomicReference<DelegateFuture<T>> ref, boolean[] weCompute) {
    DelegateFuture<T> extant;
    Object sentinel;
    while ((extant = ref.get()).unloading || (sentinel = extant.acquire()) == null) {
      if (extant.unloading) {
        if (extant == CLOSED) {
          throw new AlreadyClosedException("");
        }
        Object initialSentinel = new Object();
        DelegateFuture<T> candidate = new DelegateFuture<>(initialSentinel, extant);
        if (ref.compareAndSet(extant, candidate)) {
          weCompute[0] = true;
          return new AbstractMap.SimpleImmutableEntry<>(candidate, initialSentinel);
        }
      }
    }
    assert !extant.unloading;
    return new AbstractMap.SimpleImmutableEntry<>(extant, sentinel);
  }

  private static <T extends Closeable> Map.Entry<DelegateFuture<T>, Object> retry(
      AtomicReference<DelegateFuture<T>> ref,
      final DelegateFuture<T> replace,
      boolean[] weCompute) {
    DelegateFuture<T> prev = replace.prev.get();
    if (prev == null) {
      replace.unload(true);
      ref.compareAndSet(replace, new DelegateFuture<>(null, replace));
      return loadRef(ref, weCompute);
    }
    Object sentinel = new Object();
    DelegateFuture<T> candidate = new DelegateFuture<>(sentinel, prev);
    DelegateFuture<T> extant = ref.compareAndExchange(replace, candidate);
    if (extant == replace) {
      weCompute[0] = true;
      return new AbstractMap.SimpleImmutableEntry<>(candidate, sentinel);
    } else if (!extant.unloading && (sentinel = extant.acquire()) != null) {
      return new AbstractMap.SimpleImmutableEntry<>(extant, sentinel);
    } else {
      replace.unload(true);
      ref.compareAndSet(replace, new DelegateFuture<>(null, replace));
      return loadRef(ref, weCompute);
    }
  }

  /**
   * Defaults to true; if true, allows {@link #reporter} to defer unloading (e.g., based on
   * historical context, to avoid thrashing) via {@link UnloadHelper#deferUnload(long)}. This
   * behavior may be disabled by setting sysprop to false.
   */
  public static final boolean ADAPTIVE_DEFER =
      !"false".equals(System.getProperty("lucene.ttl.adaptiveDefer"));

  /**
   * Conditionally unloads (closes) the delegate {@link FieldsProducer}. Returns {@link #UNLOADED}
   * if resources were unloaded, otherwise returns the number of nanos remaining until the resources
   * might be eligible for unloading.
   *
   * <p>The special value {@link #STILL_REFERENCED} indicates that based on last known access time,
   * this resource <i>should</i> be eligible for unloading -- but for some reason (e.g., refCount?)
   * did not permit unloading. If this happens a lot, it probably indicates an error in logic
   * somewhere.
   */
  public long maybeUnload() throws IOException {
    long nanosSinceLastAccess = System.nanoTime() - lastAccessNanos;
    long deferNanos;
    if (nanosSinceLastAccess < keepAliveNanos) {
      // don't unload
      return keepAliveNanos - nanosSinceLastAccess;
    } else if (ADAPTIVE_DEFER && (deferNanos = reporter.deferUnload(nanosSinceLastAccess)) > 0) {
      return deferNanos;
    }
    final boolean[] unloaded = new boolean[1];
    DelegateFuture<T> holder = unloadRef(backing, unloaded);
    if (holder == null) {
      return unloaded[0] ? ALREADY_UNLOADED : STILL_REFERENCED;
    }
    // try to unload
    try {
      T weUnloaded = doUnload(holder, unloaded);
      holder.complete(new WeakReference<>(weUnloaded));
      if (weUnloaded != null) {
        return UNLOADED;
      } else {
        return unloaded[0] ? ALREADY_UNLOADED : STILL_REFERENCED;
      }
    } catch (Throwable t) {
      holder.completeExceptionally(t);
      throw t;
    }
  }

  private T doUnload(DelegateFuture<T> holder, boolean[] unloaded) throws IOException {
    assert injectDelay(unloadRandom, 5, 20);
    final DelegateFuture<T> active;
    T toClose;
    try {
      active = holder.prev.get();
      assert active != null;
      toClose = active.getNowStrong(null);
    } catch (
        @SuppressWarnings("unused")
        Exception ex) {
      // exception during loading means there's nothing to unload
      unloaded[0] = true;
      return null;
    }
    if (toClose == null) {
      // this can happen if (and should be _only_ if) we're trying to close
      // a value that's still loading for some reason. This might cause a
      // problem for foreground threads (resource access or close), but from
      // background `maybeUnload()` perspective we don't _want_ to wait for
      // it to finish loading. But it's our responsibility to ensure that the
      // value _does_ get closed: either by putting it back onto `backing`,
      // or as a last resort, waiting for it to load and then closing it.

      // first try to put our value back
      if (backing.compareAndSet(holder, active)) {
        // if CAS succeeds, it's guaranteed that state has not changed since
        // we fetched this value for closing, so we have safely put it back
        // as "still referenced"
        return null;
      } else {
        // state has changed; it's our responsibility to wait for and close
        // the resource we already pulled. Here we wait an _absurdly_ long
        // time, because it's a leak at this point if we don't close the
        // resource we've pulled.
        try {
          toClose = active.getStrong(10, TimeUnit.MINUTES);
        } catch (InterruptedException ex) {
          // we're probably shutting down
          throw new ThreadInterruptedException(ex);
        } catch (
            @SuppressWarnings("unused")
            TimeoutException ex) {
          if (out.isEnabled("UN"))
            out.message("UN", "ERROR: stuck waiting to close loading resource!");
          // TODO: we could put this into a queue somewhere that retries closing?
          //  that said, if properly implemented, this should literally _never_ happen.
          return null;
        } catch (
            @SuppressWarnings("unused")
            ExecutionException ex) {
          // exception during loading means there's nothing to unload
          unloaded[0] = true;
          return null;
        }
      }
    }
    try (active) {
      toClose.close();
    }
    unloaded[0] = true;
    return toClose;
  }

  private static final long CLOSE_WAIT_FOR_LOAD_SECONDS = 10;
  private static final long CLOSE_WAIT_FOR_BACKGROUND_UNLOAD_SECONDS = 1;

  @Override
  @SuppressWarnings("try")
  public void close() throws IOException {
    this.reporter.onClose();
    DelegateFuture<T> holder = backing.getAndSet(closedSentinel);
    if (holder == CLOSED) {
      throw new AlreadyClosedException("");
    }
    if (holder.unloading) {
      closeUnloading(holder);
    } else {
      // it's an active or loading instance
      DelegateFuture<T> maybeUnloadingHolder = holder.prev.get();
      assert maybeUnloadingHolder == null || maybeUnloadingHolder.unloading;
      try (T toClose =
          interruptProtectedGet(holder, CLOSE_WAIT_FOR_LOAD_SECONDS, TimeUnit.SECONDS)) {
        closeUnloading(maybeUnloadingHolder);
        // if we're closing ourselves, it should also count as an unload.
        reporter.onUnload(System.nanoTime() - lastAccessNanos);
        return;
      } catch (
          @SuppressWarnings("unused")
          ExecutionException e) {
        // an exception while loading means there's nothing to close, and that's ok
      } catch (TimeoutException e) {
        // probably deadlock; we have no way to get the value to close
        throw new IOException("timed out waiting to close loading value ", e);
      }
      closeUnloading(maybeUnloadingHolder);
    }
  }

  @SuppressWarnings("try")
  private void closeUnloading(DelegateFuture<T> unloading) throws IOException {
    if (unloading == null) {
      // must be done
      return;
    }
    DelegateFuture<T> toClose = unloading.prev.get();
    try {
      // first, wait for unloading to complete
      interruptProtectedGet(unloading, CLOSE_WAIT_FOR_BACKGROUND_UNLOAD_SECONDS, TimeUnit.SECONDS);
      return;
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      // fallthrough to try to close it ourselves
    } catch (
        @SuppressWarnings("unused")
        TimeoutException e) {
      if (out.isEnabled("UN"))
        out.message(
            "UN", "WARN: timeout out waiting for background unload to complete " + description);
      // fallthrough to try to close it ourselves
    }
    if (toClose != null) {
      // (if `toClose == null`, we've done all we can)
      // otherwise, `toClose` represents the resource that `unloading` was trying
      // to close. As a last-ditch effort, we'll try to close it ourselves here.
      // The risk here is double-close, but that's preferable to a resource leak.
      try {
        interruptProtectedGet(toClose, CLOSE_WAIT_FOR_LOAD_SECONDS, TimeUnit.SECONDS).close();
        reporter.onUnload(System.nanoTime() - lastAccessNanos);
      } catch (
          @SuppressWarnings("unused")
          ExecutionException e) {
        // exception during compute means there's nothing to close
      } catch (TimeoutException e) {
        throw new IOException("timeout out waiting to close loading value " + description, e);
      }
    }
  }

  /**
   * Circumvents {@link InterruptedException} and forces the calling thread to block for the full
   * allotted time. This should only be invoked from synchronous close, where the risk of a resource
   * leak outweighs the risk from delayed thread exit.
   *
   * <p>If this method swallows an {@link InterruptedException}, it will re-set the thread's
   * interrupted status before returning.
   *
   * <p>TODO: evaluate whether this behavior is actually desired in non-test context.
   *
   * <p>This method may return null! It should only be called (directly or indirectly) from within
   * top-level {@link Unloader#close()} code. It is guaranteed to block for the specified amount of
   * time, and is thus appropriate for coordination; but it should be considered "best-effort" in
   * terms of returning an actual {@link Closeable} value.
   */
  private static <T extends Closeable> T interruptProtectedGet(
      DelegateFuture<T> future, long longWaitSeconds, TimeUnit timeUnit)
      throws ExecutionException, TimeoutException {
    boolean interrupted = false;
    long now = System.nanoTime();
    final long until = now + timeUnit.toNanos(longWaitSeconds);
    long waitNanos;
    try {
      while ((waitNanos = until - now) >= 0) {
        try {
          return future.get(waitNanos, TimeUnit.NANOSECONDS).get();
        } catch (
            @SuppressWarnings("unused")
            InterruptedException ex) {
          interrupted = true;
        }
        now = System.nanoTime();
      }
      throw new TimeoutException();
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private static final class CloseableVal<T> implements Supplier<T>, Closeable {

    private final T val;
    private Object sentinel;

    private CloseableVal(T val, Object sentinel) {
      this.val = val;
      this.sentinel = sentinel;
    }

    @Override
    public T get() {
      return val;
    }

    @Override
    public void close() throws IOException {
      Object toRemove = sentinel;
      try {
        sentinel = null;
      } finally {
        Reference.reachabilityFence(toRemove);
      }
    }
  }

  private static final long TOTAL_BLOCK_NANOS = TimeUnit.SECONDS.toNanos(10);

  private CloseableVal<T> backing() throws IOException {
    boolean[] weCompute = new boolean[1];
    Map.Entry<DelegateFuture<T>, Object> holder = loadRef(backing, weCompute);
    long now = System.nanoTime();
    long until = now + TOTAL_BLOCK_NANOS;
    while (!weCompute[0]) {
      try {
        return new CloseableVal<>(
            holder.getKey().getStrong(until - now, TimeUnit.NANOSECONDS), holder.getValue());
      } catch (ExecutionException e) {
        Throwable t = e.getCause();
        if (t instanceof IOException) {
          throw (IOException) t;
        }
        holder = retry(backing, holder.getKey(), weCompute);
      } catch (InterruptedException e) {
        throw new ThreadInterruptedException(e);
      } catch (
          @SuppressWarnings("unused")
          TimeoutException e) {
        throw new IOException("timed out waiting to load backing resource");
      }
      now = System.nanoTime();
    }
    // we compute the result
    boolean successfullyComputed = false;
    T candidate = null;
    try {
      candidate = reopen.apply(this);
      holder.getKey().completeStrong(candidate);
      successfullyComputed = true;
      return new CloseableVal<>(candidate, holder.getValue());
    } catch (Throwable t) {
      holder.getKey().completeExceptionally(t);
      throw t;
    } finally {
      if (candidate != null && !successfullyComputed) {
        candidate.close();
      }
    }
  }

  private static final AtomicReference<List<String>> DEFERRED_INIT_MESSAGES =
      new AtomicReference<>(new ArrayList<>());

  private static final LongAdder INDIRECT_TRACK_COUNT = new LongAdder();
  private static final LongSupplier INDIRECT_TRACK_COUNT_SUPPLIER = INDIRECT_TRACK_COUNT::sum;

  static PointValues.PointTree wrap(PointValues.PointTree pt, Object sentinel) throws IOException {
    return new PointValues.PointTree() {
      @Override
      public PointValues.PointTree clone() {
        try {
          return wrap(pt.clone(), sentinel);
        } catch (IOException e) {
          throw new UncheckedIOException("this should never happen", e);
        }
      }

      @Override
      public boolean moveToChild() throws IOException {
        return pt.moveToChild();
      }

      @Override
      public boolean moveToSibling() throws IOException {
        return pt.moveToSibling();
      }

      @Override
      public boolean moveToParent() throws IOException {
        return pt.moveToParent();
      }

      @Override
      public byte[] getMinPackedValue() {
        return pt.getMinPackedValue();
      }

      @Override
      public byte[] getMaxPackedValue() {
        return pt.getMaxPackedValue();
      }

      @Override
      public long size() {
        return pt.size();
      }

      @Override
      public void visitDocIDs(PointValues.IntersectVisitor visitor) throws IOException {
        try {
          pt.visitDocIDs(visitor);
        } finally {
          Reference.reachabilityFence(sentinel);
        }
      }

      @Override
      public void visitDocValues(PointValues.IntersectVisitor visitor) throws IOException {
        try {
          pt.visitDocValues(visitor);
        } finally {
          Reference.reachabilityFence(sentinel);
        }
      }
    };
  }

  private static final class TrackingPostingsEnum extends FilterLeafReader.FilterPostingsEnum {
    private final Object sentinel;

    private TrackingPostingsEnum(PostingsEnum in, Object sentinel) {
      super(in);
      this.sentinel = sentinel;
    }

    @Override
    public long cost() {
      try {
        return super.cost();
      } finally {
        Reference.reachabilityFence(sentinel);
      }
    }
  }

  static TermsEnum wrap(TermsEnum te, Object sentinel) throws IOException {
    return new FilterLeafReader.FilterTermsEnum(te) {
      @Override
      public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
        INDIRECT_TRACK_COUNT.increment();
        if (reuse instanceof TrackingPostingsEnum) {
          PostingsEnum extantIn = ((TrackingPostingsEnum) reuse).in;
          PostingsEnum check = super.postings(extantIn, flags);
          if (check == extantIn) {
            // same backing, with updated state
            return reuse;
          } else {
            return new TrackingPostingsEnum(check, sentinel);
          }
        } else {
          return new TrackingPostingsEnum(super.postings(reuse, flags), sentinel);
        }
      }

      @Override
      public ImpactsEnum impacts(int flags) throws IOException {
        INDIRECT_TRACK_COUNT.increment();
        ImpactsEnum raw = super.impacts(flags);
        return new ImpactsEnum() {
          @Override
          public void advanceShallow(int target) throws IOException {
            raw.advanceShallow(target);
          }

          @Override
          public Impacts getImpacts() throws IOException {
            return raw.getImpacts();
          }

          @Override
          public int freq() throws IOException {
            return raw.freq();
          }

          @Override
          public int nextPosition() throws IOException {
            return raw.nextPosition();
          }

          @Override
          public int startOffset() throws IOException {
            return raw.startOffset();
          }

          @Override
          public int endOffset() throws IOException {
            return raw.endOffset();
          }

          @Override
          public BytesRef getPayload() throws IOException {
            return raw.getPayload();
          }

          @Override
          public int docID() {
            return raw.docID();
          }

          @Override
          public int nextDoc() throws IOException {
            return raw.nextDoc();
          }

          @Override
          public int advance(int target) throws IOException {
            return raw.advance(target);
          }

          @Override
          public long cost() {
            try {
              return raw.cost();
            } finally {
              Reference.reachabilityFence(sentinel);
            }
          }
        };
      }
    };
  }

  interface RefTrackShim<V> {
    V shim(V in, Object sentinel);
  }

  <K, V> V execute(FPIOFunction<T, K, V> function) throws IOException {
    return execute(function, null, null);
  }

  /**
   * <code>trackRaw</code> deserves special explanation: where possible, we want to track the raw
   * instance, not the shimmed instance. There is an exception however (e.g. for {@link
   * PointsReader#getValues(String)} and {@link FieldsProducer#terms(String)}), where shared
   * instances may be returned. In order to leverage refcounting to determine when a resource is
   * eligible for unloading, in such cases we must register/track the reference of our one-off
   * shim/wrapper instance.
   *
   * <p>When this method is called with <code>trackRaw=false</code>, the shim implementation should
   * (out of an abundance of caution) override all methods to wrap with <code>try/finally</code> and
   * call {@link Reference#reachabilityFence(Object)} in the <code>finally</code> block; i.e.:
   * <code>Reference.reachabilityFence(this)</code>. This will prevent dead reference analysis from
   * collecting the wrapper and unloading the resource after entering the shim method, but before
   * completing the call to the raw/backing method.
   */
  <K, V> V execute(FPIOFunction<T, K, V> function, K arg, RefTrackShim<V> shim) throws IOException {
    try (CloseableVal<T> active = backing()) {
      V raw = function.apply(active.get(), arg);
      if (raw == null) {
        return null;
      } else {
        return shim.shim(raw, active.sentinel);
      }
    } finally {
      lastAccessNanos = System.nanoTime();
    }
  }

  interface FPIOFunction<T, K, V> {
    V apply(T fp, K arg) throws IOException;
  }

  /**
   * This should be set to <code>true</code> for Lucene tests (where the only lifecycle hook we have
   * is per-{@link Directory}), and set to <code>false</code> from contexts that call {@link
   * UnloaderCoordinationPoint#setUnloadHelperSupplier(Supplier)} with an external executor whose
   * lifecycle is managed by other means (e.g., from Solr).
   */
  public static final boolean EXECUTOR_PER_DIRECTORY;

  /**
   * Time threshold at which a resource becomes eligible for unloading. Set this very low (0 or 1)
   * for stress testing.
   */
  public static final long KEEP_ALIVE_NANOS;

  private static final String DEFAULT_KEEP_ALIVE_SPEC = "60m";
  private static final long DEFAULT_KEEP_ALIVE_NANOS = TimeUnit.MINUTES.toNanos(60);

  /**
   * Additional time allowance for the first use of resource after load/reload. Set this very low
   * (e.g., 0) for stress testing.
   */
  private static final long INITIAL_NANOS;

  private static final String DEFAULT_INITIAL_SPEC = "1m";
  private static final long DFEAULT_INITIAL_NANOS = TimeUnit.MINUTES.toNanos(1);

  /**
   * Visible for testing. This may be used to override/disable resource unloading for specific tests
   * that are known to not play well with unloading.
   *
   * <p>NOTE: this is known to be the case only for tests that evaluate opening index over a
   * directory <i>without communication with the {@link IndexWriter}</i>. In such cases, with an
   * active {@link IndexWriter}, there is no way to "refcount" index files to prevent them from
   * being deleted. Lucene is ok with this by design, since, once opened, segment readers will hold
   * filehandles to files even if they are deleted; but it doesn't play well with {@link Unloader},
   * which has to re-load from disk. This would be problematic for some supported uses of Lucene,
   * <i>but not for Lucene as used by Solr, e.g.</i>, where {@link DirectoryReader} instances are
   * always acquired from (and incRef) {@link IndexWriter}.
   */
  static boolean DISABLE;

  static {
    DISABLE = "true".equals(System.getProperty("lucene.unload.disable"));
  }

  static {
    List<String> deferred = DEFERRED_INIT_MESSAGES.get();
    EXECUTOR_PER_DIRECTORY =
        "true".equals(System.getProperty("lucene.unload.executorPerDirectory")); // default to false
    deferred.add("INFO: set static property EXECUTOR_PER_DIRECTORY=" + EXECUTOR_PER_DIRECTORY);
    KEEP_ALIVE_NANOS =
        getNanos("lucene.unload.ttl", DEFAULT_KEEP_ALIVE_SPEC, DEFAULT_KEEP_ALIVE_NANOS, deferred);
    deferred.add(
        "INFO: set static property DEFAULT_KEEP_ALIVE_MILLIS="
            + TimeUnit.NANOSECONDS.toMillis(KEEP_ALIVE_NANOS));
    INITIAL_NANOS =
        getNanos("lucene.unload.initial", DEFAULT_INITIAL_SPEC, DFEAULT_INITIAL_NANOS, deferred);
    deferred.add(
        "INFO: set static property DEFAULT_INITIAL_MILLIS="
            + TimeUnit.NANOSECONDS.toMillis(INITIAL_NANOS));
  }

  private static long getNanos(
      String syspropName, String defaultSpec, long defaultNanos, List<String> deferred) {
    try {
      String unloadSpec = System.getProperty(syspropName, defaultSpec);
      int endIdx = unloadSpec.length() - 1;
      if (unloadSpec.isEmpty()) {
        deferred.add("WARN: empty " + syspropName + " spec");
        return defaultNanos;
      } else {
        TimeUnit t;
        char c = unloadSpec.charAt(endIdx);
        switch (c) {
          case 's':
            t = TimeUnit.SECONDS;
            break;
          case 'm':
            t = TimeUnit.MINUTES;
            break;
          case 'h':
            t = TimeUnit.HOURS;
            break;
          case 'd':
            t = TimeUnit.DAYS;
            break;
          default:
            if (c >= '0' && c <= '9') {
              endIdx++;
              t = TimeUnit.MILLISECONDS;
            } else {
              deferred.add("WARN: bad " + syspropName + " spec: " + unloadSpec);
              return defaultNanos;
            }
        }
        try {
          int v = Integer.parseInt(unloadSpec, 0, endIdx, 10);
          return t.toNanos(v);
        } catch (NumberFormatException ex) {
          deferred.add("WARN: bad " + syspropName + " spec: " + unloadSpec + " " + ex);
          throw ex;
        }
      }
    } catch (
        @SuppressWarnings("unused")
        Exception ex) {
      return defaultNanos;
    }
  }

  /**
   * Passed to {@link Unloader#Unloader(UnloadHelper, IOFunction, long, IOFunction)} ctor. This
   * provides a means for reporting unload/reload lifecycle events to higher-level components. This
   * can be useful if used in a framework that wants to track metrics around load/unload, or manage
   * the underlying components that handle load/unload according to framework lifecycles.
   *
   * <p>e.g., framework may supply its own {@link ScheduledExecutorService} for running unload
   * checks, and may (via {@link #maybeHandleRefQueues(AtomicBoolean, LongSupplier, LongSupplier)}
   * manage the handling of reference tracking as well.
   */
  public interface UnloadHelper {
    /**
     * Called once, from the {@link Unloader#Unloader(UnloadHelper, IOFunction, long, IOFunction)}
     * ctor, to provide the {@link ScheduledExecutorService} for scheduling periodic unload tasks.
     * The {@link Unloader} under construction is passed as an arg. Implementations should call
     * {@link Unloader#setInfoStream(InfoStream)} on the specified instance, and may also be used to
     * update metrics about number of created instances, etc.
     */
    ScheduledExecutorService onCreation(Unloader<?> u);

    /**
     * Called for each load/reload of backing resource
     *
     * @param nanosSincePriorAccess how long it's been since this resource was last accessed before
     *     reload
     * @param loadTime how long did it take to load this resource (nanos)
     */
    default void onLoad(long nanosSincePriorAccess, long loadTime) {}

    /**
     * Called for each unload of backing resource
     *
     * @param nanosSinceLastAccess how long it's bene since this resource was last accessed before
     *     unload
     */
    default void onUnload(long nanosSinceLastAccess) {}

    /**
     * called when the associated top-level resource is closed. Any backing resources held open at
     * time of close will also be unloaded (closed); this will be separately reported via {@link
     * #onUnload(long)}.
     */
    default void onClose() {}

    /**
     * A callback that allows a framework to handle refQueue management (and provides a window into
     * the size of the refQueue(s) for metrics purposes.
     *
     * <p>handle the refQueues, and should set it back to <code>false</code> if/when they stop
     * handling any of the provided refQueues.
     *
     * @param indirectTrackedCount for metrics; the number of references not tracked directly, but
     *     tracked via reference to top-level tracked object.
     */
    default void maybeHandleRefQueues(
        AtomicBoolean initialized, LongSupplier indirectTrackedCount, LongSupplier refsCollected) {
      // no-op default impl
    }

    default long deferUnload(long nanosSinceLastAccess) {
      return -1;
    }
  }

  /**
   * {@link UnloadHelper} base impl that handles setting {@link ScheduledExecutorService} and {@link
   * InfoStream} on {@link Unloader} callers of {@link #onCreation(Unloader)}.
   */
  public abstract static class AbstractUnloadHelper implements UnloadHelper {
    private volatile ScheduledExecutorService exec;
    private volatile InfoStream infoStream;

    /**
     * Provides executor (for scheduling periodic unload tasks) and infoStream for setting on
     * calling {@link Unloader}s. Both args must be non-null.
     */
    public AbstractUnloadHelper(ScheduledExecutorService exec, InfoStream infoStream) {
      this.exec = exec;
      this.infoStream = infoStream;
    }

    @Override
    public ScheduledExecutorService onCreation(Unloader<?> u) {
      ScheduledExecutorService ret = exec;
      InfoStream infoStream = this.infoStream;
      u.setInfoStream(infoStream);
      exec = null;
      this.infoStream = null;
      return ret;
    }
  }

  /**
   * Returns a {@link PointsReader} over the specified {@link SegmentReadState}, conditionally
   * wrapped to allow dynamic unloading and on-demand reloading of the backing resource.
   *
   * <p>The backing resource is initially loaded, and will be reloaded if applicable, via the
   * provided `open` {@link IOSupplier}. The {@link Directory} is passed only to be used as an
   * {@link UnloaderCoordinationPoint}.
   *
   * <p>NOTE: the segment files specified by {@link SegmentReadState}, which must be present upon
   * initialization, must still be accessible on disk if/when the backing resource is reloaded
   * (after having been unloaded). In practice, this means that {@link
   * IndexWriter#incRefDeleter(SegmentInfos)} must have been called for the {@link SegmentInfos}
   * associated with the specified {@link SegmentReadState}. This happens organically in many
   * contexts, but not all -- particularly in tests.
   */
  public static PointsReader pointsReader(
      IOSupplier<PointsReader> open, Directory dir, SegmentReadState srs) throws IOException {
    UnloadHelper unloadHelper;
    if (srs.context.mergeInfo != null
        || srs.context.flushInfo != null
        || DISABLE
        || (unloadHelper = UnloaderCoordinationPoint.getUnloadHelper(dir)) == null) {
      return open.get();
    }
    String type = PointsReader.class.getSimpleName();
    return new UnloadingPointsReader(
        unloadHelper,
        (u) -> {
          long start = System.nanoTime();
          PointsReader pr = open.get();
          try {
            u.exec.schedule(
                maybeUnloadTask(u, type, u.reporter),
                KEEP_ALIVE_NANOS + INITIAL_NANOS,
                TimeUnit.NANOSECONDS);
          } catch (
              @SuppressWarnings("unused")
              RejectedExecutionException ex) {
            // shutting down; log and swallow
            if (u.out.isEnabled("UN"))
              u.out.message("UN", "WARN: new PointsReader while shutting down");
          } catch (Throwable t) {
            try (pr) {
              throw t;
            }
          }
          u.reporter.onLoad(start - u.lastAccessNanos, System.nanoTime() - start);
          return pr;
        },
        KEEP_ALIVE_NANOS);
  }

  /**
   * Returns a {@link FieldsProducer} over the specified {@link SegmentReadState}, conditionally
   * wrapped to allow dynamic unloading and on-demand reloading of the backing resource.
   *
   * <p>The backing resource is initially loaded, and will be reloaded if applicable, via the
   * provided `open` {@link IOSupplier}. The {@link Directory} is passed only to be used as an
   * {@link UnloaderCoordinationPoint}.
   *
   * <p>NOTE: the segment files specified by {@link SegmentReadState}, which must be present upon
   * initialization, must still be accessible on disk if/when the backing resource is reloaded
   * (after having been unloaded). In practice, this means that {@link
   * IndexWriter#incRefDeleter(SegmentInfos)} must have been called for the {@link SegmentInfos}
   * associated with the specified {@link SegmentReadState}. This happens organically in many
   * contexts, but not all -- particularly in tests.
   */
  public static FieldsProducer fieldsProducer(
      IOSupplier<FieldsProducer> open, Directory dir, SegmentReadState srs) throws IOException {
    UnloadHelper unloadHelper;
    if (srs.context.mergeInfo != null
        || srs.context.flushInfo != null
        || DISABLE
        || (unloadHelper = UnloaderCoordinationPoint.getUnloadHelper(dir)) == null) {
      return open.get();
    }
    String type = FieldsProducer.class.getSimpleName();
    return new UnloadingFieldsProducer(
        unloadHelper,
        (u) -> {
          long start = System.nanoTime();
          FieldsProducer fp = open.get();
          try {
            u.exec.schedule(
                maybeUnloadTask(u, type, u.reporter),
                KEEP_ALIVE_NANOS + INITIAL_NANOS,
                TimeUnit.NANOSECONDS);
          } catch (
              @SuppressWarnings("unused")
              RejectedExecutionException ex) {
            // shutting down; log and swallow
            if (u.out.isEnabled("UN"))
              u.out.message("UN", "WARN: new FieldsProducer while shutting down");
          } catch (Throwable t) {
            try (fp) {
              throw t;
            }
          }
          u.reporter.onLoad(start - u.lastAccessNanos, System.nanoTime() - start);
          return fp;
        },
        KEEP_ALIVE_NANOS);
  }

  /**
   * Returns a {@link DocValuesProducer} over the specified {@link SegmentReadState}, conditionally
   * wrapped to allow dynamic unloading and on-demand reloading of the backing resource.
   *
   * <p>The backing resource is initially loaded, and will be reloaded if applicable, via the
   * provided `open` {@link IOSupplier}. The {@link Directory} is passed only to be used as an
   * {@link UnloaderCoordinationPoint}.
   *
   * <p>NOTE: the segment files specified by {@link SegmentReadState}, which must be present upon
   * initialization, must still be accessible on disk if/when the backing resource is reloaded
   * (after having been unloaded). In practice, this means that {@link
   * IndexWriter#incRefDeleter(SegmentInfos)} must have been called for the {@link SegmentInfos}
   * associated with the specified {@link SegmentReadState}. This happens organically in many
   * contexts, but not all -- particularly in tests.
   */
  public static DocValuesProducer docValuesProducer(
      IOSupplier<DocValuesProducer> open, Directory dir, SegmentReadState srs) throws IOException {
    UnloadHelper unloadHelper;
    if (srs.context.mergeInfo != null
        || srs.context.flushInfo != null
        || DISABLE
        || (unloadHelper = UnloaderCoordinationPoint.getUnloadHelper(dir)) == null) {
      return open.get();
    }
    String type = DocValuesProducer.class.getSimpleName();
    return new UnloadingDocValuesProducer(
        unloadHelper,
        (u) -> {
          long start = System.nanoTime();
          DocValuesProducer dvp = open.get();
          try {
            u.exec.schedule(
                maybeUnloadTask(u, type, u.reporter),
                KEEP_ALIVE_NANOS + INITIAL_NANOS,
                TimeUnit.NANOSECONDS);
          } catch (
              @SuppressWarnings("unused")
              RejectedExecutionException ex) {
            // shutting down; log and swallow
            if (u.out.isEnabled("UN"))
              u.out.message("UN", "WARN: new DocValuesProducer while shutting down");
          } catch (Throwable t) {
            try (dvp) {
              throw t;
            }
          }
          u.reporter.onLoad(start - u.lastAccessNanos, System.nanoTime() - start);
          return dvp;
        },
        KEEP_ALIVE_NANOS);
  }

  private static void printStackTrace(Throwable t, InfoStream out) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    t.printStackTrace(new PrintStream(baos, true, StandardCharsets.UTF_8));
    if (out.isEnabled("UN")) out.message("UN", baos.toString(StandardCharsets.UTF_8));
  }

  private static Callable<?> maybeUnloadTask(Unloader<?> u, String type, UnloadHelper reporter) {
    return () -> {
      long remaining;
      try {
        if (u.backing.get() == CLOSED || u.exec.isShutdown()) {
          return null;
        }
        remaining = u.maybeUnload();
      } catch (Throwable t) {
        if (!(t instanceof AlreadyClosedException)) {
          if (u.out.isEnabled("UN"))
            u.out.message(
                "UN",
                "WARN: exception in maybeUnload(); recheck in "
                    + TimeUnit.NANOSECONDS.toMillis(KEEP_ALIVE_NANOS)
                    + "ms "
                    + t);
          printStackTrace(t, u.out);
          u.exec.schedule(
              maybeUnloadTask(u, type, reporter), KEEP_ALIVE_NANOS, TimeUnit.NANOSECONDS);
        }
        throw t;
      }
      if (remaining > 0) {
        u.exec.schedule(maybeUnloadTask(u, type, reporter), remaining, TimeUnit.NANOSECONDS);
      } else if (remaining == STILL_REFERENCED) {
        u.exec.schedule(maybeUnloadTask(u, type, reporter), KEEP_ALIVE_NANOS, TimeUnit.NANOSECONDS);
      } else if (remaining == UNLOADED) {
        reporter.onUnload(System.nanoTime() - u.lastAccessNanos);
      } else if (remaining == ALREADY_UNLOADED) {
        // already unloaded
      } else {
        if (u.out.isEnabled("UN"))
          u.out.message("UN", "ERROR: unexpected return value: " + remaining);
      }
      return null;
    };
  }
}
