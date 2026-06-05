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

import java.io.IOException;
import java.text.NumberFormat;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DocumentsWriterDeleteQueue.DeleteSlice;
import org.apache.lucene.internal.hppc.LongObjectHashMap;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.SetOnce;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.lucene.util.Version;

final class DocumentsWriterPerThread implements Accountable, Lock {

  private Throwable abortingException;

  private void onAbortingException(Throwable throwable) {
    assert throwable != null : "aborting exception must not be null";
    assert abortingException == null : "aborting exception has already been set";
    abortingException = throwable;
  }

  final boolean isAborted() {
    return aborted;
  }

  static final class FlushedSegment {
    final SegmentCommitInfo segmentInfo;
    final FieldInfos fieldInfos;
    final FrozenBufferedUpdates segmentUpdates;
    final FixedBitSet liveDocs;
    final Sorter.DocMap sortMap;
    final int delCount;

    private FlushedSegment(
        InfoStream infoStream,
        SegmentCommitInfo segmentInfo,
        FieldInfos fieldInfos,
        BufferedUpdates segmentUpdates,
        FixedBitSet liveDocs,
        int delCount,
        Sorter.DocMap sortMap) {
      this.segmentInfo = segmentInfo;
      this.fieldInfos = fieldInfos;
      this.segmentUpdates =
          segmentUpdates != null && segmentUpdates.any()
              ? new FrozenBufferedUpdates(infoStream, segmentUpdates, segmentInfo)
              : null;
      this.liveDocs = liveDocs;
      this.delCount = delCount;
      this.sortMap = sortMap;
    }
  }

  /**
   * Called if we hit an exception at a bad time (when updating the index files) and must discard
   * all currently buffered docs. This resets our state, discarding any docs added since last flush.
   */
  void abort() throws IOException {
    aborted = true;
    pendingNumDocs.addAndGet(-numDocsInRAM);
    try {
      if (infoStream.isEnabled("DWPT")) {
        infoStream.message("DWPT", "now abort");
      }
      try {
        indexingChain.abort();
      } finally {
        pendingUpdates.clear();
      }
    } finally {
      if (infoStream.isEnabled("DWPT")) {
        infoStream.message("DWPT", "done abort");
      }
    }
  }

  private static final boolean INFO_VERBOSE = false;
  final Codec codec;
  final TrackingDirectoryWrapper directory;
  private final IndexingChain indexingChain;

  // Updates for our still-in-RAM (to be flushed next) segment
  private final BufferedUpdates pendingUpdates;
  private final SegmentInfo segmentInfo; // Current segment we are working on
  private boolean aborted = false; // True if we aborted
  private SetOnce<Boolean> flushPending = new SetOnce<>();
  private volatile long lastCommittedBytesUsed;
  private SetOnce<Boolean> hasFlushed = new SetOnce<>();

  private final FieldInfos.Builder fieldInfos;
  private final InfoStream infoStream;
  private int numDocsInRAM;
  final DocumentsWriterDeleteQueue deleteQueue;
  private final DeleteSlice deleteSlice;
  private final NumberFormat nf = NumberFormat.getInstance(Locale.ROOT);
  private final AtomicLong pendingNumDocs;
  private final LiveIndexWriterConfig indexWriterConfig;
  private final boolean enableTestPoints;
  private final ReentrantLock lock = new ReentrantLock();
  private int[] deleteDocIDs = new int[0];
  private int numDeletedDocIds = 0;
  private final IndexingChain.ReservedField<NumericDocValuesField> parentField;

  final long bucket;
  private final ExecutorService exec;
  private final DocumentsWriter dw;

  DocumentsWriterPerThread(
      long bucket,
      ExecutorService exec,
      DocumentsWriter dw,
      int indexMajorVersionCreated,
      String segmentName,
      Directory directoryOrig,
      Directory directory,
      LiveIndexWriterConfig indexWriterConfig,
      DocumentsWriterDeleteQueue deleteQueue,
      FieldInfos.Builder fieldInfos,
      AtomicLong pendingNumDocs,
      boolean enableTestPoints) {
    this.bucket = bucket;
    this.exec = exec;
    this.dw = dw;
    this.directory = new TrackingDirectoryWrapper(directory);
    this.fieldInfos = fieldInfos;
    this.indexWriterConfig = indexWriterConfig;
    this.infoStream = indexWriterConfig.getInfoStream();
    this.codec = indexWriterConfig.getCodec();
    this.pendingNumDocs = pendingNumDocs;
    pendingUpdates = new BufferedUpdates(segmentName);
    this.deleteQueue = Objects.requireNonNull(deleteQueue);
    assert numDocsInRAM == 0 : "num docs " + numDocsInRAM;
    deleteSlice = deleteQueue.newSlice();

    segmentInfo =
        new SegmentInfo(
            directoryOrig,
            Version.LATEST,
            Version.LATEST,
            segmentName,
            -1,
            false,
            false,
            codec,
            Collections.emptyMap(),
            StringHelper.randomId(),
            Collections.emptyMap(),
            indexWriterConfig.getIndexSort());
    assert numDocsInRAM == 0;
    if (INFO_VERBOSE && infoStream.isEnabled("DWPT")) {
      infoStream.message(
          "DWPT",
          Thread.currentThread().getName()
              + " init seg="
              + segmentName
              + " delQueue="
              + deleteQueue);
    }
    this.enableTestPoints = enableTestPoints;
    indexingChain =
        new IndexingChain(
            indexMajorVersionCreated,
            segmentInfo,
            this.directory,
            fieldInfos,
            indexWriterConfig,
            this::onAbortingException);
    if (indexWriterConfig.getParentField() != null) {
      this.parentField =
          indexingChain.markAsReserved(
              new NumericDocValuesField(indexWriterConfig.getParentField(), -1));
    } else {
      this.parentField = null;
    }
  }

  final void testPoint(String message) {
    if (enableTestPoints) {
      assert infoStream.isEnabled("TP"); // don't enable unless you need them.
      infoStream.message("TP", message);
    }
  }

  /** Anything that will add N docs to the index should reserve first to make sure it's allowed. */
  private void reserveOneDoc() {
    if (pendingNumDocs.incrementAndGet() > IndexWriter.getActualMaxDocs()) {
      // Reserve failed: put the one doc back and throw exc:
      pendingNumDocs.decrementAndGet();
      throw new IllegalArgumentException(
          "number of documents in the index cannot exceed " + IndexWriter.getActualMaxDocs());
    }
  }

  long updateDocuments(
      boolean delegate,
      Iterable<? extends Iterable<? extends IndexableField>> docs,
      DocumentsWriterDeleteQueue.Node<?> deleteNode,
      DocumentsWriter.FlushNotifications flushNotifications,
      Runnable onNewDocOnRAM)
      throws IOException {
    try {
      testPoint("DocumentsWriterPerThread addDocuments start");
      assert abortingException == null : "DWPT has hit aborting exception but is still indexing";
      if (INFO_VERBOSE && infoStream.isEnabled("DWPT")) {
        infoStream.message(
            "DWPT",
            Thread.currentThread().getName()
                + " update delTerm="
                + deleteNode
                + " docID="
                + numDocsInRAM
                + " seg="
                + segmentInfo.name);
      }
      final int docsInRamBefore = numDocsInRAM;
      boolean allDocsIndexed = false;
      long now = System.currentTimeMillis() - TEMPORAL_ADJUST_MILLIS;
      final LongObjectHashMap<Map.Entry<BlockingQueue<Iterable<? extends IndexableField>>, Future<?>>> delegates;

      // Phaser to enforce execution order in different phases, all jobs in a phase need to be completed before
      // safely advance to next phase:
      // Phase 1: primary DWPT fully iterates the docs and enqueues them (or delegates). All delegated DWPTs also finishes
      // streaming the last doc in their own invocation to this updateDocuments method BUT before existing the loop (before finishDocuments)
      // Phase 2: primary DWPT calls finishDocuments, which publish the deleteNode to the global delete queue, update
      // its deletion slice and apply the deleteNode to its pendingUpdates (updates/deletions only). Delegate DWPTs do
      // nothing as they immediately arrive at the end of phase 2 and waits for phase 3 to be released by primary DWPT.
      // Phase 3: primary DWPT signals delegate DWPTs to proceed. Each delegate DWPT exits the loop and calls finishDocuments,
      // take note that deleteNode for delegates is always null as the main DWPT has already published the deletion to
      // global queue. It is important that delegate DWPTs call finishDocuments after the main DWPT's published deletion,
      // as it would mark the deletion of current batch with its own docIdUpTo boundary.
      // Otherwise, if delegates call finishDocuments before the main DWPT, delegates would not pick up this batch's
      // deleteNode and a later DWPT (next batch) would then be the first to encounter it and apply it with a higher
      // docIdUpTo, potentially deleting docs that should have been kept.
      // After the delegate DWPTs finish, the primary DWPT will be unblocked from collectDelegateResults
      // and proceed to return the final result/exception.
      final Phaser p;
      // this doc was delegated from the primary DW to this DWPT, so we should handle it directly without further delegation
      if (delegate) {
        delegates = null;
        p = null;
      } else { // this is the main DW, so we should prepare to delegate to other DWPTs as needed
        delegates = new LongObjectHashMap<>();
        p = new Phaser(1);
      }
      AtomicBoolean failed = new AtomicBoolean();
      long seqNo;
      Throwable delegateException = null;
      try {
        final Iterator<? extends Iterable<? extends IndexableField>> iterator = docs.iterator();
        try {
          while (iterator.hasNext()) {
            Iterable<? extends IndexableField> doc = iterator.next();
            if (parentField != null) {
              if (iterator.hasNext() == false) {
                doc = addParentField(doc, parentField);
              }
            } else if (!delegate && dw != null) { //TODO: perhaps even if parentField != null, we should still delegate?
              long bucketId = mapToBucket(doc, now, bucket);
              if (bucketId != bucket) {
                delegate(bucketId, doc, delegates, p, failed);
                continue;
              }
            }
            // Even on exception, the document is still added (but marked
            // deleted), so we don't need to un-reserve at that point.
            // Aborting exceptions will actually "lose" more than one
            // document, so the counter will be "wrong" in that case, but
            // it's very hard to fix (we can't easily distinguish aborting
            // vs non-aborting exceptions):
            reserveOneDoc();
            try {
              indexingChain.processDocument(numDocsInRAM++, doc);
            } finally {
              onNewDocOnRAM.run();
            }
            if (failed.get()) {
              // exit early from our loop
              throw new PropagatedException(null);
            }
          }
        } catch (PropagatedException ex) {
          assert failed.get(); // swallow in favor of throwing original exception
        } catch (Throwable t) {
          failed.set(true);
          throw t;
        } finally {
          if (delegates != null && !delegates.isEmpty()) {
            // Phase 1: signal delegates that all docs have been enqueued, then wait for
            // delegates to finish adding their docs (but not yet finishDocuments).
            try {
              for (LongObjectHashMap.LongObjectCursor<Map.Entry<BlockingQueue<Iterable<? extends IndexableField>>, Future<?>>> bucket : delegates) {
                bucket.value.getKey().put(SENTINEL);
              }
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
            } finally {
              p.arriveAndAwaitAdvance();
              // Phase 2: All delegates have finished adding their doc, entering phase 2 for primary DWPT to call finishDocuments
              // while all delegate DWPTs are blocked in Phase 2
            }
          }
        }
        if (failed.get()) {
          seqNo = -1;
        } else {
          final int numDocs = numDocsInRAM - docsInRamBefore;
          if (numDocs > 1) {
            segmentInfo.setHasBlocks();
          }
          allDocsIndexed = true;
          // Call finishDocuments here, between phases, so that the deleteNode is in the
          // global queue before delegates call their own finishDocuments. This ensures
          // delegates consume it with the correct docIdUpTo boundary, protecting their
          // newly-added docs from being deleted by their own batch's delete term.
          seqNo = finishDocuments(deleteNode, docsInRamBefore);
        }
      } finally {
        if (delegates != null && !delegates.isEmpty()) {
          try {
            // Phase 3: release delegates (always, if phase 1 happened) so they can complete
            // their own finishDocuments call, which will consume the deleteNode from the
            // global queue with the correct per-delegate docIdUpTo boundary.
            p.arrive();
            List<Throwable> delegateExceptions = collectDelegateResults(delegates);
            if (!delegateExceptions.isEmpty()) {
              delegateException = delegateExceptions.get(0);
            }
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          }
        }
        if (!allDocsIndexed && !aborted) {
          // the iterator threw an exception that is not aborting
          // go and mark all docs from this block as deleted
          deleteLastDocs(numDocsInRAM - docsInRamBefore);
        }
      }
      if (delegateException != null) {
        if (delegateException instanceof IOException) {
          throw (IOException) delegateException;
        } else {
          throw new RuntimeException(delegateException);
        }
      }
      if (failed.get()) {
        throw new RuntimeException("failed (fallback)");
      }
      assert seqNo != -1;
      return seqNo;
    } finally {
      maybeAbort("updateDocuments", flushNotifications);
    }
  }

  private static List<Throwable> collectDelegateResults(LongObjectHashMap<Map.Entry<BlockingQueue<Iterable<? extends IndexableField>>, Future<?>>> delegates) throws InterruptedException {
    List<Throwable> exceptions = new ArrayList<>();
    for (LongObjectHashMap.LongObjectCursor<Map.Entry<BlockingQueue<Iterable<? extends IndexableField>>, Future<?>>> bucket : delegates) {
      try {
        bucket.value.getValue().get();
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (!(cause instanceof PropagatedException)) {
          exceptions.add(cause);
        }
      }
    }
    return exceptions;
  }

  static long defaultBucket() {
    return DEFAULT_BUCKET;
  }

  private void delegate(long bucketId, Iterable<? extends IndexableField> doc, LongObjectHashMap<Map.Entry<BlockingQueue<Iterable<? extends IndexableField>>, Future<?>>> delegates, Phaser p, AtomicBoolean failed) {
    int i = delegates.indexOf(bucketId);
    BlockingQueue<Iterable<? extends IndexableField>> queue;
    if (i >= 0) {
      queue = delegates.indexGet(i).getKey();
    } else {
      //each queue allows buffering up to this # of docs for delegate of this bucket to be streamed as iterator to DW
      queue = new ArrayBlockingQueue<>(256);
      DocumentsWriter dw = this.dw;
      p.register();
      Future<?> f = exec.submit(() -> { // submit a background job to start streaming doc to DW as iterator which reads the queue
        boolean[] exhausted = new boolean[1];
        try {
          dw.updateDocuments(true, bucketId, new Iterable<Iterable<? extends IndexableField>>() {
            @Override
            public Iterator<Iterable<? extends IndexableField>> iterator() {
              return new Iterator<Iterable<? extends IndexableField>>() {
                private Iterable<? extends IndexableField> nextDoc;

                @Override
                public boolean hasNext() {
                  //the coordinating primary DW sends this signal to each DWPT when the original iterator is exhausted, so we know there's no more doc for this bucket
                  if (nextDoc == SENTINEL) {
                    return false;
                  } else if (nextDoc != null) {
                    return true;
                  }
                  try {
                    nextDoc = queue.take();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new ThreadInterruptedException(e);
                  }
                  if (nextDoc == SENTINEL) {
                    exhausted[0] = true;
                    p.arriveAndAwaitAdvance(); // Finished phase 1 for this delegate, block until phase 2
                    p.arriveAndAwaitAdvance(); // phase 2 for delegates does nothing, block until phase 3

                    // phase 3: primary DWPT has published deleteNode via finishDocuments in phase 2. Safe to
                    // unblock caller(delegate DWPT)'s iteration on this and eventually calls finishDocuments too.
                    if (failed.get()) {
                      throw new PropagatedException(null);
                    }
                    return false;
                  } else {
                    return true;
                  }
                }

                @Override
                public Iterable<? extends IndexableField> next() {
                  if (!hasNext()) {
                    throw new NoSuchElementException();
                  }
                  Iterable<? extends IndexableField> ret = nextDoc;
                  nextDoc = null;
                  return ret;
                }
              };
            }
          }, null);
        } catch (PropagatedException t) {
          // swallow since it originated from elsewhere
        } catch (Throwable t) {
          if (failed.compareAndSet(false, true)) {
            throw t;
          } else {
            throw new PropagatedException(t);
          }
        } finally {
          if (!exhausted[0]) {
            p.arriveAndDeregister();
          }
        }
        return null;
      });
      delegates.indexInsert(i, bucketId, new AbstractMap.SimpleImmutableEntry<>(queue, f));
    }
    try {
      queue.put(doc);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new ThreadInterruptedException(ex);
    }
  }

  private static final class PropagatedException extends RuntimeException {
    public PropagatedException(Throwable cause) {
      super(cause);
    }
  }

  private static final Iterable<? extends IndexableField> SENTINEL = (Iterable<IndexableField>) () -> {
    throw new UnsupportedOperationException();
  };

  private static final String TEMPORAL_FIELD_NAME; // e.g., EventStart

  /**
   * If {@link #TEMPORAL_FIELD_NAME} field not present, use a value from one of these fields as a fallback
   * (in descending order of priority).
   */
  private static final String[] FALLBACK_FIELD_NAMES;

  static {
    String spec = System.getProperty("lucene.temporalField.name");
    if (spec == null) {
      TEMPORAL_FIELD_NAME = null;
      FALLBACK_FIELD_NAMES = null;
    } else {
      String[] fields = spec.split(", *");
      TEMPORAL_FIELD_NAME = fields[0];
      if (fields.length < 2) {
        FALLBACK_FIELD_NAMES = null;
      } else {
        FALLBACK_FIELD_NAMES = new String[fields.length - 1];
        System.arraycopy(fields, 1, FALLBACK_FIELD_NAMES, 0, FALLBACK_FIELD_NAMES.length);
      }
    }
  }

  private static final long TEMPORAL_ADJUST_MILLIS = TimeUnit.DAYS.toMillis(Long.parseLong(System.getProperty("lucene.temporalField.adjust", "0")));
  private static final long[] BOUNDARIES;
  private static final long DEFAULT_BUCKET;

  /**
   * Default boundaries correspond to natural boundaries (-1w, 1d, 1w, 1m, 3m, 6m) with some padding.
   * The initial "-1w" is designed to catch timestamps that are pathologically far in the future.
   * These will be batched in their own bucket. Such segments may naturally age into normal buckets --
   * but this protects against the case where a single doc absurdly far in the future (like, thousands
   * of years) might prevent its associated segment (and subsequent merged segments) from <i>ever</i>
   * aging out of the "most recent" bucket.
   */
  private static final long[] DEFAULT_BOUNDARIES = new long[] {-9, 3, 9, 32, 94, 184}; // <- days (converted to millis)

  static {
    for (int i = DEFAULT_BOUNDARIES.length - 1; i >= 0; i--) {
      // convert days to millis
      DEFAULT_BOUNDARIES[i] = TimeUnit.DAYS.toMillis(DEFAULT_BOUNDARIES[i]);
    }
  }

  static {
    String boundariesSpec = System.getProperty("lucene.temporalField.boundaries");
    long[] array;
    long last;
    if (boundariesSpec == null) {
      array = DEFAULT_BOUNDARIES;
      last = array[0]; // we can guarantee no AIOOBE
    } else {
      try {
        array = Arrays.stream(boundariesSpec.split(", *")).mapToLong((v) -> TimeUnit.DAYS.toMillis(Long.parseLong(v))).toArray();
        last = array[0]; // maybe AIOOBE
      } catch (Throwable t) {
        throw new IllegalArgumentException("bad boundariesSpec: " + boundariesSpec, t);
      }
    }
    long defaultBucket = last;
    for (int i = 1; i < array.length; i++) {
      long v = array[i];
      if (v <= last) {
        throw new IllegalArgumentException("boundariesSpec must be in-order; found: " + boundariesSpec);
      }
      if (defaultBucket <= 0) {
        // ideally the default bucket will be the smallest _positive_ bucket
        defaultBucket = v;
      }
      last = v;
    }
    BOUNDARIES = array;
    DEFAULT_BUCKET = defaultBucket;
  }

  static long mapToBucket(Iterable<? extends IndexableField> doc) {
    return mapToBucket(doc, System.currentTimeMillis() - TEMPORAL_ADJUST_MILLIS, defaultBucket());
  }

  private static long mapToBucket(Iterable<? extends IndexableField> doc, long now, long defaultBucket) {
    if (TEMPORAL_FIELD_NAME == null) {
      // default for test coverage
      return defaultBucket + (System.identityHashCode(doc) % 4);
    } else {
      int fallbackIdx;
      IndexableField[] fallbacks;
      if (FALLBACK_FIELD_NAMES == null) {
        fallbackIdx = -1;
        fallbacks = null;
      } else {
        fallbackIdx = FALLBACK_FIELD_NAMES.length - 1;
        fallbacks = new IndexableField[FALLBACK_FIELD_NAMES.length];
      }
      for (IndexableField f : doc) {
        String name = f.name();
        if (TEMPORAL_FIELD_NAME.equals(name)) {
          return mapToBucket(f.numericValue().longValue(), now);
        } else if (FALLBACK_FIELD_NAMES != null) {
          for (int i = fallbackIdx; i >= 0; i--) {
            if (FALLBACK_FIELD_NAMES[i].equals(name)) {
              fallbacks[i] = f;
              fallbackIdx = i - 1;
            }
          }
        }
      }
      if (FALLBACK_FIELD_NAMES != null) {
        for (int i = fallbackIdx + 1, lim = FALLBACK_FIELD_NAMES.length; i < lim; i++) {
          IndexableField f = fallbacks[i];
          if (f != null) {
            return mapToBucket(f.numericValue().longValue(), now);
          }
        }
      }
      return defaultBucket;
    }
  }

  public static long mapToBucket(long timestamp, long now) {
    long diff = now - timestamp;
    for (long v : BOUNDARIES) {
      if (diff <= v) {
        return v;
      }
    }
    return Long.MAX_VALUE;
  }

  private Iterable<? extends IndexableField> addParentField(
      Iterable<? extends IndexableField> doc, IndexableField parentField) {
    return () -> {
      final Iterator<? extends IndexableField> first = doc.iterator();
      return new Iterator<>() {
        IndexableField additionalField = parentField;

        @Override
        public boolean hasNext() {
          return additionalField != null || first.hasNext();
        }

        @Override
        public IndexableField next() {
          if (additionalField != null) {
            IndexableField field = additionalField;
            additionalField = null;
            return field;
          }
          if (first.hasNext()) {
            return first.next();
          }
          throw new NoSuchElementException();
        }
      };
    };
  }

  private long finishDocuments(DocumentsWriterDeleteQueue.Node<?> deleteNode, int docIdUpTo) {
    /*
     * here we actually finish the document in two steps 1. push the delete into
     * the queue and update our slice. 2. increment the DWPT private document
     * id.
     *
     * the updated slice we get from 1. holds all the deletes that have occurred
     * since we updated the slice the last time.
     */
    // Apply delTerm only after all indexing has
    // succeeded, but apply it only to docs prior to when
    // this batch started:
    long seqNo;
    if (deleteNode != null) {
      seqNo = deleteQueue.add(deleteNode, deleteSlice);
      assert deleteSlice.isTail(deleteNode) : "expected the delete term as the tail item";
      deleteSlice.apply(pendingUpdates, docIdUpTo);
      return seqNo;
    } else {
      seqNo = deleteQueue.updateSlice(deleteSlice);
      if (seqNo < 0) {
        seqNo = -seqNo;
        deleteSlice.apply(pendingUpdates, docIdUpTo);
      } else {
        deleteSlice.reset();
      }
    }

    return seqNo;
  }

  // This method marks the last N docs as deleted. This is used
  // in the case of a non-aborting exception. There are several cases
  // where we fail a document ie. due to an exception during analysis
  // that causes the doc to be rejected but won't cause the DWPT to be
  // stale nor the entire IW to abort and shutdown. In such a case
  // we only mark these docs as deleted and turn it into a livedocs
  // during flush
  private void deleteLastDocs(int docCount) {
    int from = numDocsInRAM - docCount;
    int to = numDocsInRAM;
    deleteDocIDs = ArrayUtil.grow(deleteDocIDs, numDeletedDocIds + (to - from));
    for (int docId = from; docId < to; docId++) {
      deleteDocIDs[numDeletedDocIds++] = docId;
    }
    // NOTE: we do not trigger flush here.  This is
    // potentially a RAM leak, if you have an app that tries
    // to add docs but every single doc always hits a
    // non-aborting exception.  Allowing a flush here gets
    // very messy because we are only invoked when handling
    // exceptions so to do this properly, while handling an
    // exception we'd have to go off and flush new deletes
    // which is risky (likely would hit some other
    // confounding exception).
  }

  /** Returns the number of RAM resident documents in this {@link DocumentsWriterPerThread} */
  public int getNumDocsInRAM() {
    // public for FlushPolicy
    return numDocsInRAM;
  }

  /**
   * Prepares this DWPT for flushing. This method will freeze and return the {@link
   * DocumentsWriterDeleteQueue}s global buffer and apply all pending deletes to this DWPT.
   */
  FrozenBufferedUpdates prepareFlush() {
    assert numDocsInRAM > 0;
    final FrozenBufferedUpdates globalUpdates = deleteQueue.freezeGlobalBuffer(deleteSlice);
    /* deleteSlice can possibly be null if we have hit non-aborting exceptions during indexing and never succeeded
    adding a document. */
    if (deleteSlice != null) {
      // apply all deletes before we flush and release the delete slice
      deleteSlice.apply(pendingUpdates, numDocsInRAM);
      assert deleteSlice.isEmpty();
      deleteSlice.reset();
    }
    return globalUpdates;
  }

  /** Flush all pending docs to a new segment */
  FlushedSegment flush(DocumentsWriter.FlushNotifications flushNotifications) throws IOException {
    assert flushPending.get() == Boolean.TRUE;
    assert numDocsInRAM > 0;
    assert deleteSlice.isEmpty() : "all deletes must be applied in prepareFlush";
    segmentInfo.setMaxDoc(numDocsInRAM);
    final SegmentWriteState flushState =
        new SegmentWriteState(
            infoStream,
            directory,
            segmentInfo,
            fieldInfos.finish(),
            pendingUpdates,
            new IOContext(new FlushInfo(numDocsInRAM, lastCommittedBytesUsed)));
    final double startMBUsed = lastCommittedBytesUsed / 1024. / 1024.;

    // Apply delete-by-docID now (delete-byDocID only
    // happens when an exception is hit processing that
    // doc, eg if analyzer has some problem w/ the text):
    if (numDeletedDocIds > 0) {
      flushState.liveDocs = new FixedBitSet(numDocsInRAM);
      flushState.liveDocs.set(0, numDocsInRAM);
      for (int i = 0; i < numDeletedDocIds; i++) {
        flushState.liveDocs.clear(deleteDocIDs[i]);
      }
      flushState.delCountOnFlush = numDeletedDocIds;
      deleteDocIDs = new int[0];
    }

    if (aborted) {
      if (infoStream.isEnabled("DWPT")) {
        infoStream.message("DWPT", "flush: skip because aborting is set");
      }
      return null;
    }

    long t0 = System.nanoTime();

    if (infoStream.isEnabled("DWPT")) {
      infoStream.message(
          "DWPT",
          "flush postings as segment " + flushState.segmentInfo.name + " numDocs=" + numDocsInRAM);
    }
    final Sorter.DocMap sortMap;
    try {
      DocIdSetIterator softDeletedDocs;
      if (indexWriterConfig.getSoftDeletesField() != null) {
        softDeletedDocs = indexingChain.getHasDocValues(indexWriterConfig.getSoftDeletesField());
      } else {
        softDeletedDocs = null;
      }
      sortMap = indexingChain.flush(flushState);
      if (softDeletedDocs == null) {
        flushState.softDelCountOnFlush = 0;
      } else {
        flushState.softDelCountOnFlush =
            PendingSoftDeletes.countSoftDeletes(softDeletedDocs, flushState.liveDocs);
        assert flushState.segmentInfo.maxDoc()
            >= flushState.softDelCountOnFlush + flushState.delCountOnFlush;
      }
      // We clear this here because we already resolved them (private to this segment) when writing
      // postings:
      pendingUpdates.clearDeleteTerms();
      segmentInfo.setFiles(new HashSet<>(directory.getCreatedFiles()));

      final SegmentCommitInfo segmentInfoPerCommit =
          new SegmentCommitInfo(
              segmentInfo,
              0,
              flushState.softDelCountOnFlush,
              -1L,
              -1L,
              -1L,
              StringHelper.randomId());
      if (infoStream.isEnabled("DWPT")) {
        infoStream.message(
            "DWPT",
            "new segment has "
                + (flushState.liveDocs == null ? 0 : flushState.delCountOnFlush)
                + " deleted docs");
        infoStream.message(
            "DWPT", "new segment has " + flushState.softDelCountOnFlush + " soft-deleted docs");
        infoStream.message(
            "DWPT",
            "new segment has "
                + (flushState.fieldInfos.hasVectors() ? "vectors" : "no vectors")
                + "; "
                + (flushState.fieldInfos.hasNorms() ? "norms" : "no norms")
                + "; "
                + (flushState.fieldInfos.hasDocValues() ? "docValues" : "no docValues")
                + "; "
                + (flushState.fieldInfos.hasProx() ? "prox" : "no prox")
                + "; "
                + (flushState.fieldInfos.hasFreq() ? "freqs" : "no freqs"));
        infoStream.message("DWPT", "flushedFiles=" + segmentInfoPerCommit.files());
        infoStream.message("DWPT", "flushed codec=" + codec);
      }

      final BufferedUpdates segmentDeletes;
      if (pendingUpdates.deleteQueries.isEmpty() && pendingUpdates.numFieldUpdates.get() == 0) {
        pendingUpdates.clear();
        segmentDeletes = null;
      } else {
        segmentDeletes = pendingUpdates;
      }

      if (infoStream.isEnabled("DWPT")) {
        final double newSegmentSize = segmentInfoPerCommit.sizeInBytes() / 1024. / 1024.;
        infoStream.message(
            "DWPT",
            "flushed: segment="
                + segmentInfo.name
                + " ramUsed="
                + nf.format(startMBUsed)
                + " MB"
                + " newFlushedSize="
                + nf.format(newSegmentSize)
                + " MB"
                + " docs/MB="
                + nf.format(flushState.segmentInfo.maxDoc() / newSegmentSize));
      }

      assert segmentInfo != null;

      FlushedSegment fs =
          new FlushedSegment(
              infoStream,
              segmentInfoPerCommit,
              flushState.fieldInfos,
              segmentDeletes,
              flushState.liveDocs,
              flushState.delCountOnFlush,
              sortMap);
      sealFlushedSegment(fs, sortMap, flushNotifications);
      if (infoStream.isEnabled("DWPT")) {
        infoStream.message(
            "DWPT",
            "flush time "
                + ((System.nanoTime() - t0) / (double) TimeUnit.MILLISECONDS.toNanos(1))
                + " ms");
      }
      return fs;
    } catch (Throwable t) {
      onAbortingException(t);
      throw t;
    } finally {
      maybeAbort("flush", flushNotifications);
      hasFlushed.set(Boolean.TRUE);
    }
  }

  private void maybeAbort(String location, DocumentsWriter.FlushNotifications flushNotifications)
      throws IOException {
    if (abortingException != null && aborted == false) {
      // if we are already aborted don't do anything here
      try {
        abort();
      } finally {
        // whatever we do here we have to fire this tragic event up.
        flushNotifications.onTragicEvent(abortingException, location);
      }
    }
  }

  private final Set<String> filesToDelete = new HashSet<>();

  Set<String> pendingFilesToDelete() {
    return filesToDelete;
  }

  private FixedBitSet sortLiveDocs(Bits liveDocs, Sorter.DocMap sortMap) {
    assert liveDocs != null && sortMap != null;
    FixedBitSet sortedLiveDocs = new FixedBitSet(liveDocs.length());
    sortedLiveDocs.set(0, liveDocs.length());
    for (int i = 0; i < liveDocs.length(); i++) {
      if (liveDocs.get(i) == false) {
        sortedLiveDocs.clear(sortMap.oldToNew(i));
      }
    }
    return sortedLiveDocs;
  }

  /**
   * Seals the {@link SegmentInfo} for the new flushed segment and persists the deleted documents
   * {@link FixedBitSet}.
   */
  void sealFlushedSegment(
      FlushedSegment flushedSegment,
      Sorter.DocMap sortMap,
      DocumentsWriter.FlushNotifications flushNotifications)
      throws IOException {
    assert flushedSegment != null;
    SegmentCommitInfo newSegment = flushedSegment.segmentInfo;

    IndexWriter.setDiagnostics(newSegment.info, IndexWriter.SOURCE_FLUSH);

    IOContext context =
        new IOContext(new FlushInfo(newSegment.info.maxDoc(), newSegment.sizeInBytes()));

    boolean success = false;
    try {

      if (indexWriterConfig.getUseCompoundFile()) {
        Set<String> originalFiles = newSegment.info.files();
        // TODO: like addIndexes, we are relying on createCompoundFile to successfully cleanup...
        IndexWriter.createCompoundFile(
            infoStream,
            new TrackingDirectoryWrapper(directory),
            newSegment.info,
            context,
            flushNotifications::deleteUnusedFiles);
        filesToDelete.addAll(originalFiles);
        newSegment.info.setUseCompoundFile(true);
      }

      // Have codec write SegmentInfo.  Must do this after
      // creating CFS so that 1) .si isn't slurped into CFS,
      // and 2) .si reflects useCompoundFile=true change
      // above:
      codec.segmentInfoFormat().write(directory, newSegment.info, context);

      // TODO: ideally we would freeze newSegment here!!
      // because any changes after writing the .si will be
      // lost...

      // Must write deleted docs after the CFS so we don't
      // slurp the del file into CFS:
      if (flushedSegment.liveDocs != null) {
        final int delCount = flushedSegment.delCount;
        assert delCount > 0;
        if (infoStream.isEnabled("DWPT")) {
          infoStream.message(
              "DWPT",
              "flush: write "
                  + delCount
                  + " deletes gen="
                  + flushedSegment.segmentInfo.getDelGen());
        }

        // TODO: we should prune the segment if it's 100%
        // deleted... but merge will also catch it.

        // TODO: in the NRT case it'd be better to hand
        // this del vector over to the
        // shortly-to-be-opened SegmentReader and let it
        // carry the changes; there's no reason to use
        // filesystem as intermediary here.

        SegmentCommitInfo info = flushedSegment.segmentInfo;
        Codec codec = info.info.getCodec();
        final FixedBitSet bits;
        if (sortMap == null) {
          bits = flushedSegment.liveDocs;
        } else {
          bits = sortLiveDocs(flushedSegment.liveDocs, sortMap);
        }
        codec.liveDocsFormat().writeLiveDocs(bits, directory, info, delCount, context);
        newSegment.setDelCount(delCount);
        newSegment.advanceDelGen();
      }

      success = true;
    } finally {
      if (!success) {
        if (infoStream.isEnabled("DWPT")) {
          infoStream.message(
              "DWPT",
              "hit exception creating compound file for newly flushed segment "
                  + newSegment.info.name);
        }
      }
    }
  }

  /** Get current segment info we are writing. */
  SegmentInfo getSegmentInfo() {
    return segmentInfo;
  }

  @Override
  public long ramBytesUsed() {
    assert lock.isHeldByCurrentThread();
    return (deleteDocIDs.length * (long) Integer.BYTES)
        + pendingUpdates.ramBytesUsed()
        + indexingChain.ramBytesUsed();
  }

  @Override
  public Collection<Accountable> getChildResources() {
    assert lock.isHeldByCurrentThread();
    return List.of(pendingUpdates, indexingChain);
  }

  @Override
  public String toString() {
    return "DocumentsWriterPerThread [pendingDeletes="
        + pendingUpdates
        + ", segment="
        + segmentInfo.name
        + ", aborted="
        + aborted
        + ", numDocsInRAM="
        + numDocsInRAM
        + ", deleteQueue="
        + deleteQueue
        + ", "
        + numDeletedDocIds
        + " deleted docIds"
        + "]";
  }

  /** Returns true iff this DWPT is marked as flush pending */
  boolean isFlushPending() {
    return flushPending.get() == Boolean.TRUE;
  }

  /** Sets this DWPT as flush pending. This can only be set once. */
  void setFlushPending() {
    flushPending.set(Boolean.TRUE);
  }

  /**
   * Returns the last committed bytes for this DWPT. This method can be called without acquiring the
   * DWPTs lock.
   */
  long getLastCommittedBytesUsed() {
    return lastCommittedBytesUsed;
  }

  /**
   * Commits the current {@link #ramBytesUsed()} and stores it's value for later reuse. The last
   * committed bytes used can be retrieved via {@link #getLastCommittedBytesUsed()}
   */
  void commitLastBytesUsed(long delta) {
    assert isHeldByCurrentThread();
    assert getCommitLastBytesUsedDelta() == delta : "delta has changed";
    lastCommittedBytesUsed += delta;
  }

  /**
   * Calculates the delta between the last committed bytes used and the currently used ram.
   *
   * @see #commitLastBytesUsed(long)
   * @return the delta between the current {@link #ramBytesUsed()} and the current {@link
   *     #getLastCommittedBytesUsed()}
   */
  long getCommitLastBytesUsedDelta() {
    assert isHeldByCurrentThread();
    long delta = ramBytesUsed() - lastCommittedBytesUsed;
    return delta;
  }

  @Override
  public void lock() {
    lock.lock();
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    lock.lockInterruptibly();
  }

  @Override
  public boolean tryLock() {
    return lock.tryLock();
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
    return lock.tryLock(time, unit);
  }

  /**
   * Returns true if the DWPT's lock is held by the current thread
   *
   * @see ReentrantLock#isHeldByCurrentThread()
   */
  boolean isHeldByCurrentThread() {
    return lock.isHeldByCurrentThread();
  }

  @Override
  public void unlock() {
    lock.unlock();
  }

  @Override
  public Condition newCondition() {
    throw new UnsupportedOperationException();
  }

  /** Returns <code>true</code> iff this DWPT has been flushed */
  boolean hasFlushed() {
    return hasFlushed.get() == Boolean.TRUE;
  }
}
