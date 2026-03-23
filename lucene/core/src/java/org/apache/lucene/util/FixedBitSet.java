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
package org.apache.lucene.util;

import org.apache.lucene.internal.vectorization.FixedBitSetSupport;
import org.apache.lucene.internal.vectorization.VectorizationProvider;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

/**
 * BitSet of fixed length (numBits), backed by accessible ({@link #getBits}) long[], accessed with
 * an int index, implementing {@link Bits} and {@link DocIdSet}. If you need to manage more than
 * 2.1B bits, use {@link LongBitSet}.
 *
 * @lucene.internal
 */
public final class FixedBitSet extends BitSet {

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(FixedBitSet.class);

  private static final FixedBitSetSupport SUPPORT;
  public static final int VECTOR_BYTE_SIZE;

  static {
    SUPPORT = VectorizationProvider.getInstance().getFixedBitSetSupport();
    VECTOR_BYTE_SIZE = SUPPORT.vectorByteSize();
  }

  public static final ByteOrder BYTE_ORDER;

  static {
    String bigE = System.getProperty("lucene.bitset.bigendian");
    if (bigE == null) {
      BYTE_ORDER = ByteOrder.nativeOrder();
    } else {
      switch (bigE) {
        case "true":
          BYTE_ORDER = ByteOrder.BIG_ENDIAN;
          break;
        case "false":
          BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;
          break;
        default:
          throw new IllegalArgumentException();
      }
    }
  }

  private final LongBuffer bits; // Array of longs holding the bits
  private final Object memorySegment;
  private final int numBits; // The number of bits in use
  private final int numWords; // The exact number of longs needed to hold numBits (<= bits.length)

  public static final class ByteBufferStruct {
    public final ByteBuffer buf;
    public final Object m;

    public ByteBufferStruct(ByteBuffer buf) {
      this.buf = buf;
      this.m = (VECTOR_BYTE_SIZE > 0 && buf.remaining() >= VECTOR_BYTE_SIZE)
          ? SUPPORT.wrapBuffer(buf) : null;
    }

    public ByteBufferStruct(ByteBuffer buf, boolean withMemorySegment) {
      this.buf = buf;
      this.m = (withMemorySegment && VECTOR_BYTE_SIZE > 0 && buf.remaining() >= VECTOR_BYTE_SIZE)
          ? SUPPORT.wrapBuffer(buf) : null;
    }

    public LongBufferStruct asLongBufferStruct() {
      return new LongBufferStruct(this);
    }
  }

  public static final class LongBufferStruct {
    public final LongBuffer buf;
    public final Object m;

    private LongBufferStruct(ByteBufferStruct buf) {
      this.buf = buf.buf.asLongBuffer();
      this.m = buf.m;
    }

    public LongBufferStruct(LongBuffer buf) {
      this.buf = buf;
      this.m = VECTOR_BYTE_SIZE > 0 ? SUPPORT.wrapBuffer(buf) : null;
    }

    private LongBufferStruct(LongBuffer buf, Object m) {
      this.buf = buf;
      this.m = m;
    }
  }

  public interface Modifier extends Closeable {
    default LongBufferStruct allocate(int numWords) {
      return allocateBytes(numWords << 3, true).asLongBufferStruct();
    }
    default LongBufferStruct grow(LongBufferStruct arr, int minSize) {
      assert minSize >= 0 : "size must be positive (got " + minSize + "): likely integer overflow?";
      if (arr.buf.remaining() < minSize) {
        LongBufferStruct ret = allocate(ArrayUtil.oversize(minSize, Long.BYTES));
        ret.buf.put(arr.buf);
        ret.buf.clear();
        arr.buf.flip(); // restore
        return ret;
      } else {
        return arr;
      }
    }
    default ByteBufferStruct allocateBytes(int size, boolean withMemorySegment) {
      return new ByteBufferStruct(ByteBuffer.allocate(size).order(BYTE_ORDER), withMemorySegment);
    }
    default ByteBufferStruct[] allocateBytesArr(int numBytes, Object sentinel, boolean withMemorySegment) {
      throw new UnsupportedOperationException();
    }
    default Modifier partitioned(int bitShift) {
      int byteShift = bitShift - 3;
      int maxBytes = 1 << byteShift;
      int byteMask = maxBytes - 1;
      return new Modifier() {
        @Override
        public ByteBufferStruct[] allocateBytesArr(int numBytes, Object sentinel, boolean withMemorySegment) {
          int lastIdx = (numBytes - 1) >> byteShift;
          ByteBufferStruct[] ret = new ByteBufferStruct[lastIdx + 1];
          int len = ((numBytes - 1) & byteMask) + 1;
          for (int i = lastIdx; i >= 0; i--) {
            ret[i] = allocateBytes(len, withMemorySegment);
            len = maxBytes;
          }
          return ret;
        }
      };
    }
    @Override
    default void close() {
      // no-op
    }
  }

  public static final Modifier DEFAULT_MODIFIER = new Modifier() {};

  /**
   * If the given {@link FixedBitSet} is large enough to hold {@code numBits+1}, returns the given
   * bits, otherwise returns a new {@link FixedBitSet} which can hold the requested number of bits.
   *
   * <p><b>NOTE:</b> the returned bitset reuses the underlying {@code long[]} of the given {@code
   * bits} if possible. Also, calling {@link #length()} on the returned bits may return a value
   * greater than {@code numBits}.
   */
  public static FixedBitSet ensureCapacity(FixedBitSet bits, int numBits) {
    if (numBits < bits.numBits) {
      return bits;
    } else {
      // Depends on the ghost bits being clear!
      // (Otherwise, they may become visible in the new instance)
      int numWords = bits2words(numBits);
      LongBufferStruct arr = new LongBufferStruct(bits.bits, bits.memorySegment);
      if (numWords >= arr.buf.remaining()) {
        arr = DEFAULT_MODIFIER.grow(arr, numWords + 1);
      }
      return new FixedBitSet(arr, arr.buf.remaining() << 6);
    }
  }

  /** returns the number of 64 bit words it would take to hold numBits */
  public static int bits2words(int numBits) {
    // I.e.: get the word-offset of the last bit and add one (make sure to use >> so 0
    // returns 0!)
    return ((numBits - 1) >> 6) + 1;
  }

  /**
   * Returns the popcount or cardinality of the intersection of the two sets. Neither set is
   * modified.
   */
  public static long intersectionCount(FixedBitSet a, FixedBitSet b) {
    // Depends on the ghost bits being clear!
    return SUPPORT.intersectionPopCount(
        a.memorySegment, a.bits,
        b.memorySegment, b.bits,
        Math.min(a.numWords, b.numWords));
  }

  /** Returns the popcount or cardinality of the union of the two sets. Neither set is modified. */
  public static long unionCount(FixedBitSet a, FixedBitSet b) {
    // Depends on the ghost bits being clear!
    return SUPPORT.unionPopCount(
        a.memorySegment, a.bits, a.numWords,
        b.memorySegment, b.bits, b.numWords);
  }

  /**
   * Returns the popcount or cardinality of "a and not b" or "intersection(a, not(b))". Neither set
   * is modified.
   */
  public static long andNotCount(FixedBitSet a, FixedBitSet b) {
    // Depends on the ghost bits being clear!
    return SUPPORT.andNotPopCount(
        a.memorySegment, a.bits, a.numWords,
        b.memorySegment, b.bits, b.numWords);
  }

  /**
   * Creates a new FixedBitSet. The internally allocated long array will be exactly the size needed
   * to accommodate the numBits specified.
   *
   * @param numBits the number of bits needed
   */
  public FixedBitSet(int numBits) {
    this(numBits, DEFAULT_MODIFIER);
  }

  public FixedBitSet(int numBits, Modifier m) {
    this.numBits = numBits;
    LongBufferStruct lbs = m.allocate(bits2words(numBits));
    bits = lbs.buf;
    memorySegment = lbs.m;
    numWords = bits.remaining();
  }

  /**
   * Creates a new FixedBitSet using the provided long[] array as backing store. The storedBits
   * array must be large enough to accommodate the numBits specified, but may be larger. In that
   * case the 'extra' or 'ghost' bits must be clear (or they may provoke spurious side-effects)
   *
   * @param storedBits the array to use as backing store
   * @param numBits the number of bits actually needed
   */
  public FixedBitSet(LongBufferStruct storedBits, int numBits) {
    this(storedBits.buf, storedBits.m, numBits);
  }

  private FixedBitSet(LongBuffer storedBits, Object m, int numBits) {
    this.numWords = bits2words(numBits);
    if (numWords > storedBits.remaining()) {
      throw new IllegalArgumentException(
          "The given long array is too small  to hold " + numBits + " bits");
    }
    this.numBits = numBits;
    this.bits = storedBits;
    this.memorySegment = m;

    assert verifyGhostBitsClear();
  }

  @Override
  public void clear() {
    for (int i = bits.remaining() - 1; i >= 0; i--) {
      bits.put(i, 0L);
    }
  }

  /**
   * Checks if the bits past numBits are clear. Some methods rely on this implicit assumption:
   * search for "Depends on the ghost bits being clear!"
   *
   * @return true if the bits past numBits are clear.
   */
  private boolean verifyGhostBitsClear() {
    for (int i = numWords, lim = bits.remaining(); i < lim; i++) {
      if (bits.get(i) != 0) return false;
    }

    if ((numBits & 0x3f) == 0) return true;

    long mask = -1L << numBits;

    return (bits.get(numWords - 1) & mask) == 0;
  }

  @Override
  public int length() {
    return numBits;
  }

  @Override
  public long ramBytesUsed() {
    // for now, pretend we're a long[]
    return BASE_RAM_BYTES_USED + RamUsageEstimator.alignObjectSize((long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) Long.BYTES * bits.remaining());
  }

  /** Expert. */
  public LongBuffer getBits() {
    return bits;
  }

  /**
   * Returns number of set bits. NOTE: this visits every long in the backing bits array, and the
   * result is not internally cached!
   */
  @Override
  public int cardinality() {
    // Depends on the ghost bits being clear!
    return Math.toIntExact(SUPPORT.popCount(memorySegment, bits, 0, numWords));
  }

  @Override
  public int approximateCardinality() {
    // Naive sampling: compute the number of bits that are set on the first 16 longs every 1024
    // longs and scale the result by 1024/16.
    // This computes the pop count on ranges instead of single longs in order to take advantage of
    // vectorization.

    final int rangeLength = 16;
    final int interval = 1024;

    if (numWords <= interval) {
      return cardinality();
    }

    long popCount = 0;
    int maxWord;
    for (maxWord = 0; maxWord + interval < numWords; maxWord += interval) {
      popCount += SUPPORT.popCount(memorySegment, bits, maxWord, rangeLength);
    }

    popCount *= (interval / rangeLength) * numWords / maxWord;
    return (int) popCount;
  }

  @Override
  public boolean get(int index) {
    assert index >= 0 && index < numBits : "index=" + index + ", numBits=" + numBits;
    int i = index >> 6; // div 64
    // signed shift will keep a negative index and force an
    // array-index-out-of-bounds-exception, removing the need for an explicit check.
    long bitmask = 1L << index;
    return (bits.get(i) & bitmask) != 0;
  }

  @Override
  public void set(int index) {
    assert index >= 0 && index < numBits : "index=" + index + ", numBits=" + numBits;
    int wordNum = index >> 6; // div 64
    long bitmask = 1L << index;
    bits.put(wordNum, bits.get(wordNum) | bitmask);
  }

  @Override
  public boolean getAndSet(int index) {
    assert index >= 0 && index < numBits : "index=" + index + ", numBits=" + numBits;
    int wordNum = index >> 6; // div 64
    long bitmask = 1L << index;
    long extantWord = bits.get(wordNum);
    bits.put(wordNum, extantWord | bitmask);
    return (extantWord & bitmask) != 0;
  }

  @Override
  public void clear(int index) {
    assert index >= 0 && index < numBits : "index=" + index + ", numBits=" + numBits;
    int wordNum = index >> 6;
    long bitmask = 1L << index;
    bits.put(wordNum, bits.get(wordNum) & ~bitmask);
  }

  public boolean getAndClear(int index) {
    assert index >= 0 && index < numBits : "index=" + index + ", numBits=" + numBits;
    int wordNum = index >> 6; // div 64
    long bitmask = 1L << index;
    long extantWord = bits.get(wordNum);
    bits.put(wordNum, extantWord & ~bitmask);
    return (extantWord & bitmask) != 0;
  }

  @Override
  public int nextSetBit(int index) {
    // Depends on the ghost bits being clear!
    assert index >= 0 && index < numBits : "index=" + index + ", numBits=" + numBits;
    int i = index >> 6;
    long word = bits.get(i) >> index; // skip all the bits to the right of index

    if (word != 0) {
      return index + Long.numberOfTrailingZeros(word);
    }

    while (++i < numWords) {
      word = bits.get(i);
      if (word != 0) {
        return (i << 6) + Long.numberOfTrailingZeros(word);
      }
    }

    return DocIdSetIterator.NO_MORE_DOCS;
  }

  @Override
  public int prevSetBit(int index) {
    assert index >= 0 && index < numBits : "index=" + index + " numBits=" + numBits;
    int i = index >> 6;
    final int subIndex = index & 0x3f; // index within the word
    long word = (bits.get(i) << (63 - subIndex)); // skip all the bits to the left of index

    if (word != 0) {
      return (i << 6) + subIndex - Long.numberOfLeadingZeros(word); // See LUCENE-3197
    }

    while (--i >= 0) {
      word = bits.get(i);
      if (word != 0) {
        return (i << 6) + 63 - Long.numberOfLeadingZeros(word);
      }
    }

    return -1;
  }

  @Override
  public void or(DocIdSetIterator iter) throws IOException {
    if (BitSetIterator.getFixedBitSetOrNull(iter) != null) {
      checkUnpositioned(iter);
      final FixedBitSet bits = BitSetIterator.getFixedBitSetOrNull(iter);
      or(bits);
    } else if (iter instanceof DocBaseBitSetIterator) {
      checkUnpositioned(iter);
      DocBaseBitSetIterator baseIter = (DocBaseBitSetIterator) iter;
      or(baseIter.getDocBase() >> 6, baseIter.getBitSet());
    } else {
      super.or(iter);
    }
  }

  /** this = this OR other */
  public void or(FixedBitSet other) {
    or(0, other);
  }

  private void or(final int otherOffsetWords, FixedBitSet other) {
    assert other.numWords + otherOffsetWords <= numWords
        : "numWords=" + numWords + ", otherNumWords=" + other.numWords;
    int len = Math.min(numWords - otherOffsetWords, other.numWords);
    SUPPORT.or(memorySegment, bits, otherOffsetWords, other.memorySegment, other.bits, len);
  }

  /** Does in-place XOR of the bits provided by the iterator. */
  public void xor(DocIdSetIterator iter) throws IOException {
    checkUnpositioned(iter);
    if (BitSetIterator.getFixedBitSetOrNull(iter) != null) {
      final FixedBitSet bits = BitSetIterator.getFixedBitSetOrNull(iter);
      xor(bits);
    } else {
      int doc;
      while ((doc = iter.nextDoc()) < numBits) {
        flip(doc);
      }
    }
  }

  /** this = this XOR other */
  public void xor(FixedBitSet other) {
    assert other.numWords <= numWords : "numWords=" + numWords + ", other.numWords=" + other.numWords;
    int len = Math.min(numWords, other.numWords);
    SUPPORT.xor(memorySegment, bits, other.memorySegment, other.bits, len);
  }

  /** returns true if the sets have any elements in common */
  public boolean intersects(FixedBitSet other) {
    // Depends on the ghost bits being clear!
    int pos = Math.min(numWords, other.numWords);
    while (--pos >= 0) {
      if ((bits.get(pos) & other.bits.get(pos)) != 0) return true;
    }
    return false;
  }

  /** this = this AND other */
  public void and(FixedBitSet other) {
    int len = Math.min(this.numWords, other.numWords);
    SUPPORT.and(memorySegment, bits, other.memorySegment, other.bits, len);
    if (this.numWords > other.numWords) {
      fill(other.numWords, this.numWords, 0L);
    }
  }

  public void andNot(DocIdSetIterator iter) throws IOException {
    if (BitSetIterator.getFixedBitSetOrNull(iter) != null) {
      checkUnpositioned(iter);
      final FixedBitSet bits = BitSetIterator.getFixedBitSetOrNull(iter);
      assert bits != null;
      andNot(bits);
    } else if (iter instanceof DocBaseBitSetIterator) {
      checkUnpositioned(iter);
      DocBaseBitSetIterator baseIter = (DocBaseBitSetIterator) iter;
      andNot(baseIter.getDocBase() >> 6, baseIter.getBitSet());
    } else {
      checkUnpositioned(iter);
      for (int doc = iter.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iter.nextDoc()) {
        clear(doc);
      }
    }
  }

  /** this = this AND NOT other */
  public void andNot(FixedBitSet other) {
    andNot(0, other);
  }

  private void andNot(final int otherOffsetWords, FixedBitSet other) {
    int len = Math.min(numWords - otherOffsetWords, other.numWords);
    SUPPORT.andNot(memorySegment, bits, otherOffsetWords, other.memorySegment, other.bits, len);
  }

  /**
   * Scans the backing store to check if all bits are clear. The method is deliberately not called
   * "isEmpty" to emphasize it is not low cost (as isEmpty usually is).
   *
   * @return true if all bits are clear.
   */
  public boolean scanIsEmpty() {
    // This 'slow' implementation is still faster than any external one could be
    // (e.g.: (bitSet.length() == 0 || bitSet.nextSetBit(0) == -1))
    // especially for small BitSets
    // Depends on the ghost bits being clear!
    final int count = numWords;

    for (int i = 0; i < count; i++) {
      if (bits.get(i) != 0) return false;
    }

    return true;
  }

  /**
   * Flips a range of bits
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to flip
   */
  public void flip(int startIndex, int endIndex) {
    assert startIndex >= 0 && startIndex < numBits;
    assert endIndex >= 0 && endIndex <= numBits;
    if (endIndex <= startIndex) {
      return;
    }

    int startWord = startIndex >> 6;
    int endWord = (endIndex - 1) >> 6;

    /* Grrr, java shifting uses only the lower 6 bits of the count so -1L>>>64 == -1
     * for that reason, make sure not to use endmask if the bits to flip will
     * be zero in the last word (redefine endWord to be the last changed...)
     * long startmask = -1L << (startIndex & 0x3f);     // example: 11111...111000
     * long endmask = -1L >>> (64-(endIndex & 0x3f));   // example: 00111...111111
     */

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex;

    if (startWord == endWord) {
      bits.put(startWord, bits.get(startWord) ^ (startmask & endmask));
      return;
    }

    bits.put(startWord, bits.get(startWord) ^ startmask);

    SUPPORT.flipWords(memorySegment, bits, startWord + 1, endWord);

    bits.put(endWord, bits.get(endWord) ^ endmask);
  }

  /** Flip the bit at the provided index. */
  public void flip(int index) {
    assert index >= 0 && index < numBits : "index=" + index + " numBits=" + numBits;
    int wordNum = index >> 6; // div 64
    long bitmask = 1L << index; // mod 64 is implicit
    bits.put(wordNum, bits.get(wordNum) ^ bitmask);
  }

  /**
   * Sets a range of bits
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to set
   */
  public void set(int startIndex, int endIndex) {
    assert startIndex >= 0 && startIndex < numBits
        : "startIndex=" + startIndex + ", numBits=" + numBits;
    assert endIndex >= 0 && endIndex <= numBits : "endIndex=" + endIndex + ", numBits=" + numBits;
    if (endIndex <= startIndex) {
      return;
    }

    int startWord = startIndex >> 6;
    int endWord = (endIndex - 1) >> 6;

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex;

    if (startWord == endWord) {
      bits.put(startWord, bits.get(startWord) | (startmask & endmask));
      return;
    }

    bits.put(startWord, bits.get(startWord) | startmask);
    fill(startWord + 1, endWord, -1L);
    bits.put(endWord, bits.get(endWord) | endmask);
  }

  private void fill(int startWord, int endWord, long val) {
    SUPPORT.fill(memorySegment, bits, startWord, endWord, val);
  }

  public static boolean madvise(Object ms, int advice) throws Throwable {
    return SUPPORT.madvise(ms, advice);
  }

  @Override
  public void clear(int startIndex, int endIndex) {
    assert startIndex >= 0 && startIndex < numBits
        : "startIndex=" + startIndex + ", numBits=" + numBits;
    assert endIndex >= 0 && endIndex <= numBits : "endIndex=" + endIndex + ", numBits=" + numBits;
    if (endIndex <= startIndex) {
      return;
    }

    int startWord = startIndex >> 6;
    int endWord = (endIndex - 1) >> 6;

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex;

    // invert masks since we are clearing
    startmask = ~startmask;
    endmask = ~endmask;

    if (startWord == endWord) {
      bits.put(startWord, bits.get(startWord) & (startmask | endmask));
      return;
    }

    bits.put(startWord, bits.get(startWord) & startmask);
    fill(startWord + 1, endWord, 0L);
    bits.put(endWord, bits.get(endWord) & endmask);
  }

  @Override
  public FixedBitSet clone() {
    return clone(DEFAULT_MODIFIER.allocate(numWords));
  }

  public FixedBitSet clone(LongBufferStruct bits) {
    LongBuffer template = this.bits.slice();
    bits.buf.put(template);
    bits.buf.clear();
    return new FixedBitSet(bits, numBits);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FixedBitSet)) {
      return false;
    }
    FixedBitSet other = (FixedBitSet) o;
    if (numBits != other.numBits) {
      return false;
    }
    // Depends on the ghost bits being clear!
    return bits.equals(other.bits);
  }

  @Override
  public int hashCode() {
    // Depends on the ghost bits being clear!
    long h = 0;
    for (int i = numWords; --i >= 0; ) {
      h ^= bits.get(i);
      h = (h << 1) | (h >>> 63); // rotate left
    }
    // fold leftmost bits into right and add a constant to prevent
    // empty sets from returning 0, which is too common.
    return (int) ((h >> 32) ^ h) + 0x98761234;
  }

  /**
   * If the specified {@link Bits} is backed by (or is itself) an instance of {@link FixedBitSet},
   * this will return a {@link FixedBitSet} view over the same underlying data. This circumvents the
   * safety protections inherent in the {@link Bits} interface, so the returned object must not be
   * modified.
   *
   * <p>If the specified {@link Bits} is <i>not</i> known to be backed by a {@link FixedBitSet},
   * this method returns <code>null</code>.
   */
  public static FixedBitSet unsafeReadOnlyViewOf(Bits bits) {
    if (bits instanceof FixedBits) {
      // restore the original FixedBitSet
      FixedBits fixedBits = (FixedBits) bits;
      return new FixedBitSet(fixedBits.bits, fixedBits.m, fixedBits.length);
    } else if (bits instanceof FixedBitSet) {
      return (FixedBitSet) bits;
    } else {
      return null;
    }
  }

  /** Make a copy of the given bits. */
  public static FixedBitSet copyOf(Bits bits) {
    return copyOf(bits, DEFAULT_MODIFIER);
  }

  public static FixedBitSet copyOf(Bits bits, Modifier m) {
    if (bits instanceof FixedBits) {
      // restore the original FixedBitSet
      FixedBits fixedBits = (FixedBits) bits;
      bits = new FixedBitSet(fixedBits.bits, fixedBits.m, fixedBits.length);
    }

    if (bits instanceof FixedBitSet) {
      return ((FixedBitSet) bits).clone(m.allocate(((FixedBitSet) bits).numWords));
    } else {
      int length = bits.length();
      FixedBitSet bitSet = new FixedBitSet(length, m);
      bitSet.set(0, length);
      for (int i = 0; i < length; ++i) {
        if (bits.get(i) == false) {
          bitSet.clear(i);
        }
      }
      return bitSet;
    }
  }

  public static void copyTo(Bits bits, FixedBitSet[] dest) {
    LongBuffer buf;
    int len;
    if (bits instanceof FixedBits) {
      // restore the original FixedBitSet
      FixedBits fixedBits = (FixedBits) bits;
      buf = fixedBits.bits.slice();
      len = fixedBits.length;
    } else if (bits instanceof FixedBitSet) {
      FixedBitSet fbs = (FixedBitSet) bits;
      buf = fbs.bits.slice();
      len = fbs.length();
    } else {
      len = bits.length();
      int i = 0;
      for (FixedBitSet subDest : dest) {
        int subLen = subDest.length();
        subDest.set(0, subLen);
        int base = i;
        for (int lim = Math.min(i + subLen, len); i < lim; i++) {
          if (!bits.get(i)) {
            subDest.clear(i - base);
          }
        }
      }
      return;
    }
    int srcLim = bits2words(len);
    for (FixedBitSet fixedBitSet : dest) {
      LongBuffer subDest = fixedBitSet.bits;
      buf.limit(Math.min(buf.position() + subDest.remaining(), srcLim));
      subDest.put(buf).flip();
    }
  }

  /**
   * Convert this instance to read-only {@link Bits}. This is useful in the case that this {@link
   * FixedBitSet} is returned as a {@link Bits} instance, to make sure that consumers may not get
   * write access back by casting to a {@link FixedBitSet}. NOTE: Changes to this {@link
   * FixedBitSet} will be reflected on the returned {@link Bits}.
   */
  public Bits asReadOnlyBits() {
    return new FixedBits(bits, memorySegment, numBits);
  }
}
