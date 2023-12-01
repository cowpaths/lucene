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
package org.apache.lucene.codecs.pooling;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;

public class PoolingPostingsFormat extends PostingsFormat {

  public static final String NAME = "Pooling90";
  private final PostingsFormat delegate;
  private final int postingsLimit;
  private final Set<String> fieldNames;

  public PoolingPostingsFormat() {
    this(NAME, new Lucene90PostingsFormat(), 1, null);
  }

  public PoolingPostingsFormat(String name, PostingsFormat delegate, int postingsLimit, Set<String> fieldNames) {
    super(name);
    this.delegate = delegate;
    this.postingsLimit = postingsLimit;
    this.fieldNames = fieldNames;
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    return delegate.fieldsConsumer(state);
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new PoolingFieldsProducer(delegate.fieldsProducer(state), postingsLimit, fieldNames);
  }

  public static PoolingFieldsProducer wrap(FieldsProducer delegate, int postingsLimit, Set<String> fieldNames) {
    return new PoolingFieldsProducer(delegate, postingsLimit, fieldNames);
  }

  public static class PoolingFieldsProducer extends FieldsProducer {
    private final FieldsProducer delegate;
    private final int postingsLimit;
    private final Set<String> fieldNames;

    private PoolingFieldsProducer(FieldsProducer delegate, int postingsLimit, Set<String> fieldNames) {
      this.delegate = delegate;
      this.postingsLimit = postingsLimit;
      this.fieldNames = fieldNames;
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }

    @Override
    public void checkIntegrity() throws IOException {
      delegate.checkIntegrity();
    }

    @Override
    public Iterator<String> iterator() {
      return delegate.iterator();
    }

    @Override
    public void forEach(Consumer<? super String> action) {
      delegate.forEach(action);
    }

    @Override
    public Spliterator<String> spliterator() {
      return delegate.spliterator();
    }

    @Override
    public Terms terms(String field) throws IOException {
      return terms(field, postingsLimit, null);
    }

    public Terms terms(String field, int postingsLimit, Set<String> poolPostingsFields) throws IOException {
      Terms ret = delegate.terms(field);
      if (ret == null) {
        return null;
      }
      if (postingsLimit == -1) {
        return ret;
      } else {
        if (poolPostingsFields != null) {
          if (!poolPostingsFields.contains(field)) {
            return ret;
          }
        } else if (fieldNames != null && !fieldNames.contains(field)) {
          return ret;
        }
        return new PostingsPoolingTerms(ret, postingsLimit);
      }
    }

    @Override
    public int size() {
      return delegate.size();
    }
  }

  /**
   * Terms that might apply pooling on {@code PostingsEnum} from {@code intersect} operation
   */
  static class PostingsPoolingTerms extends FilterLeafReader.FilterTerms {

    private final int postingsLimit;

    protected PostingsPoolingTerms(Terms in, int postingsLimit) {
      super(in);
      this.postingsLimit = postingsLimit;
    }

    @Override
    public TermsEnum iterator() throws IOException {
      return new PostingsPoolingTermsEnum(in.iterator());
    }


    /**
     * Performs intersection and returns a wrapped {@link TermsEnum} which produces values in collating format.
     * <p>
     * These values can then be used for seeking and {@link PostingsEnum} retrieval with {@link PostingsPoolingTermsEnum}
     * @throws IOException
     */
    @Override
    public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
      return new FilteredTermsEnum(in.intersect(compiled, startTerm)) {
        private final PostingsManager lookup = new PostingsManager(postingsLimit);
        @Override
        public BytesRef next() throws IOException {
          return tenum.next();
        }

        @Override
        public TermState termState() throws IOException {
          return new PostingsPoolingTermState(lookup, tenum.termState());
        }

        @Override
        protected AcceptStatus accept(BytesRef term) throws IOException {
          throw new UnsupportedOperationException();
        }
      };
    }
  }

  private interface PostingsPuller {
    /**
     * Pulls the {@code PostingsEnum} of the specified raw term and set it to the provided wrapper {@code dest}
     * <p>
     * The {@code PostingsEnum} will also be restored by advancing to the document ID greater or equal to the provided
     * {@code docId} and advancing to the last read document position if applicable
     *
     * @param raw
     * @param ts
     * @param term
     * @param dest
     * @param docId
     * @param flags
     * @return
     * @throws IOException
     */
    PostingsEnum transferPostingsEnum(TermsEnum raw, TermState ts, BytesRef term, ThinPostingsEnum dest, int docId, int flags) throws IOException;
  }

  private static class PostingsPoolingTermState extends TermState {
    private PostingsManager lookup;
    private TermState delegate;
    private PostingsPoolingTermState(PostingsManager lookup, TermState delegate) {
      this.lookup = lookup;
      this.delegate = delegate;
    }
    @Override
    public void copyFrom(TermState other) {
      PostingsPoolingTermState o = (PostingsPoolingTermState) other;
      this.lookup = o.lookup;
      this.delegate = o.delegate;
    }
  }

  /**
   * Provides ability to seek on terms in collating format,
   * see {@link PostingsPoolingTerms#intersect(CompiledAutomaton, BytesRef)}
   * and then upon call to {@code postings}, return a pooled {@code PostingsEnum} managed by {@code PostingsManager}
   */
  private static class PostingsPoolingTermsEnum extends FilteredTermsEnum {
    private PostingsManager lookup;
    private PostingsPoolingTermState termState;

    @Override
    protected AcceptStatus accept(BytesRef term) throws IOException {
      throw new UnsupportedOperationException();
    }

    public PostingsPoolingTermsEnum(TermsEnum tenum) {
      super(tenum);
    }

    @Override
    public SeekStatus seekCeil(BytesRef term) throws IOException {
      lookup = null;
      return tenum.seekCeil(term);
    }

    @Override
    public void seekExact(long ord) throws IOException {
      lookup = null;
      tenum.seekExact(ord);
    }

    @Override
    public BytesRef next() throws IOException {
      lookup = null;
      return tenum.next();
    }

    @Override
    public boolean seekExact(BytesRef term) throws IOException {
      lookup = null;
      return tenum.seekExact(term);
    }

    @Override
    public void seekExact(BytesRef term, TermState state) throws IOException {
      if (state instanceof PostingsPoolingTermState) {
        PostingsPoolingTermState s = (PostingsPoolingTermState) state;
        termState = s;
        lookup = s.lookup;
        state = s.delegate;
      } else {
        lookup = null;
      }
      tenum.seekExact(term, state);
    }

    @Override
    public TermState termState() throws IOException {
      return lookup == null ? tenum.termState() : termState;
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
      if (lookup == null || reuse != null) {
        return tenum.postings(reuse, flags);
      } else {
        // TODO: do we really need `deepCopyOf` here?
        return lookup.register(tenum, termState.delegate, BytesRef.deepCopyOf(tenum.term()), flags);
      }
    }
  }

  /**
   * Manages a pool of active {@code PostingsEnum}s for a specific TermsEnum instance.
   * <p>
   * Many {@code PostingsEnum}s can be registered via the
   * {@link PostingsManager#register(TermsEnum, TermState, BytesRef, int)} method but this manager only keeps
   * {@code postingsLimit} of active {@code PostingsEnum}s by strong reference of the wrapper {@code ThinPostingsEnum}
   * <p>
   * Removed(inactive) {@code PostingEnums}from the pool will no longer maintain such reference, therefore made
   * available for Garbage Collection.
   * <p>
   */
  private static class PostingsManager {
    private final PriorityQueue<ThinPostingsEnum> reuseQueue;
    private PostingsEnum scratch;
    private final PostingsPuller puller;
    private PostingsManager(int postingsLimit) {
      reuseQueue = new PriorityQueue<ThinPostingsEnum>(postingsLimit) {
        @Override
        protected boolean lessThan(ThinPostingsEnum a, ThinPostingsEnum b) {
          return a.docId > b.docId || (a.docId == b.docId && a.trackedPosition > b.trackedPosition);
        }
      };
      this.puller = (raw, termState, term, dest, targetDocId, flags) -> {
        raw.seekExact(term, termState);
        ThinPostingsEnum top = reuseQueue.top();
        PostingsEnum ret = raw.postings(top.postingsEnum, flags);
        top.postingsEnum = null;
        dest.setPostingsEnum(ret, targetDocId);
        reuseQueue.updateTop(dest);
        return ret;
      };
    }

    /**
     * Registers the {@code PostingsEnum} from the current state of {@code te} to this manager.
     *
     * @return a {@code ThinPostingsEnum} which references the actual {@code PostingsEnum}, the manager might
     * set/unset the reference to maintain the pool size (strong reference count to the {@code PostingsEnum})
     * @throws IOException
     */
    private PostingsEnum register(TermsEnum te, TermState ts, BytesRef term, int flags) throws IOException {
      PostingsEnum pe = te.postings(scratch, flags);
      int initialDoc = pe.nextDoc();
      assert initialDoc != PostingsEnum.NO_MORE_DOCS;
      ThinPostingsEnum ret = new ThinPostingsEnum(te, ts, term, pe, flags, puller);
      ThinPostingsEnum removed = reuseQueue.insertWithOverflow(ret);
      if (removed == null) {
        scratch = null;
      } else {
        scratch = removed.postingsEnum;
        removed.postingsEnum = null;
      }
      return ret;
    }
  }

  /**
   * Wraps a {@code PostingsEnum} as a field reference. Take note that this field could be set/unset by
   * the {@link PostingsPuller}.
   *
   */
  private static class ThinPostingsEnum extends PostingsEnum {
    private final TermsEnum raw;

    //the index to look up the TermState for fast seeking on the raw {@code TermsEnum}, this index refers to the value
    //returned by the {@code PostingsManager#add} method
    private final TermState termState;
    private final BytesRef term;
    private final PostingsPuller pullPostings;
    PostingsEnum postingsEnum;
    final int flags;
    final boolean positionRequested;
    private final boolean offsetsRequested;
    private final boolean payloadsRequested;
    private int startOffset = -1;
    private int endOffset = -1;
    private BytesRef payload;

    int docId;
    int freq;
    int trackedPosition = -1;
    int positionsRemaining; //to resume the position if the posting enum is restored
    final long cost;
    boolean initialPeekDoc = true;

    private ThinPostingsEnum(TermsEnum raw, TermState ts, BytesRef term, PostingsEnum postingsEnum, int flags, PostingsPuller pullPostings) throws IOException {
      this.raw = raw;
      this.termState = ts;
      this.term = term;
      this.postingsEnum = postingsEnum;
      this.flags = flags;
      this.positionRequested = PostingsEnum.featureRequested(flags, POSITIONS);
      this.offsetsRequested = PostingsEnum.featureRequested(flags, OFFSETS);
      this.payloadsRequested = PostingsEnum.featureRequested(flags, PAYLOADS);
      this.pullPostings = pullPostings;
      this.docId = postingsEnum.docID();
      this.freq = postingsEnum.freq();
      this.positionsRemaining = this.freq;
      if (positionRequested) {
        currentPositionAndAdvance(postingsEnum);
      }
      this.cost = postingsEnum.cost();
    }

    @Override
    public int nextDoc() throws IOException {
      if (initialPeekDoc) {
        initialPeekDoc = false;
        return docId;
      }
      if (postingsEnum == null) {
        //pull the PostingsEnum back. this.docId + 1 to advance to docId >= such id
        pullPostings.transferPostingsEnum(raw, termState, term, this, this.docId + 1, flags);
        return this.docId;
      } else {
        return initDoc(postingsEnum, postingsEnum.nextDoc());
      }
    }

    @Override
    public int freq() throws IOException {
      return freq;
    }

    @Override
    public int nextPosition() throws IOException {
      return currentPositionAndAdvance(getPostingsEnum());
    }

    @Override
    public int startOffset() throws IOException {
      return startOffset;
    }

    @Override
    public int endOffset() throws IOException {
      return endOffset;
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return payload;
    }

    @Override
    public int docID() {
      return initialPeekDoc ? -1 : docId;
    }

    @Override
    public int advance(int target) throws IOException {
      if (initialPeekDoc) {
        initialPeekDoc = false;
        if (target <= docId) {
          return docId;
        }
      }
      if (postingsEnum == null) {
        pullPostings.transferPostingsEnum(raw, termState, term, this, target, flags);
        return this.docId;
      } else {
        return initDoc(postingsEnum, postingsEnum.advance(target));
      }
    }

    @Override
    public long cost() {
      return cost;
    }

    /**
     * Call this on advancing to new docId, this updates various internal fields which are useful for
     * sorting and restoring PostingsEnum state
     * @param pe
     * @param docId
     * @return
     * @throws IOException
     */
    private int initDoc(PostingsEnum pe, int docId) throws IOException {
      if (positionRequested && docId != DocIdSetIterator.NO_MORE_DOCS) {
        freq = pe.freq();
        positionsRemaining = freq;
        currentPositionAndAdvance(pe);
      }
      return this.docId = docId;
    }

    private PostingsEnum getPostingsEnum() throws IOException {
      if (postingsEnum == null) {
        int targetDocId = docId;
        PostingsEnum ret = pullPostings.transferPostingsEnum(raw, termState, term, this, targetDocId, flags);
        assert docId == targetDocId : "nope1 "+docId+" != "+targetDocId;
        return ret;
      } else {
        assert postingsEnum.docID() == docId;
        return postingsEnum;
      }
    }

    void setPostingsEnum(PostingsEnum pe, int docIdTarget) throws IOException {
      this.postingsEnum = pe;
      int docId = pe.advance(docIdTarget);
      if (docId != this.docId) {
        initDoc(pe, docId);
      } else if (positionRequested) {
        assert pe.freq() == freq;
        int preAdvanceCount = freq - positionsRemaining;
        if (preAdvanceCount > 0) {
          for (int i = preAdvanceCount - 1; i > 0; i--) {
            pe.nextPosition();
          }
          if (offsetsRequested) {
            startOffset = pe.startOffset();
            endOffset = pe.endOffset();
          }
          if (payloadsRequested) {
            BytesRef payload = pe.getPayload();
            this.payload = payload == null ? null : BytesRef.deepCopyOf(payload);
          }
          int atPosition = pe.nextPosition();
          assert atPosition == trackedPosition : "nope2 "+atPosition+" != "+trackedPosition;
        }
      }
    }

    int currentPositionAndAdvance(PostingsEnum pe) throws IOException {
      if (!positionRequested) {
        throw new UnsupportedOperationException();
      }

      int result = trackedPosition;
      if (result != -1) {
        if (offsetsRequested) {
          startOffset = pe.startOffset();
          endOffset = pe.endOffset();
        }
        if (payloadsRequested) {
          BytesRef payload = pe.getPayload();
          this.payload = payload == null ? null : BytesRef.deepCopyOf(payload);
        }
      }
      if (positionsRemaining-- <= 0) {
        trackedPosition = Integer.MAX_VALUE;
      } else {
        trackedPosition = pe.nextPosition();
      }
      return result;
    }
  }
}
