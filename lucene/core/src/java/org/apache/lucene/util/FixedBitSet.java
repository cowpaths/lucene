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

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;

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
  /**
   * Normally this is set to 17, making for max 1M blocks. May set lower for improved test coverage
   * of edge cases. TODO: maybe set this dynamically according to configured G1HeapRegionSize?
   * <ul>
   *   <li>17 -&gt; 1M blocks
   *   <li>16 -&gt; 512K blocks
   *   <li>15 -&gt; 256K blocks
   *   <li>14 -&gt; 128K blocks
   *   <li>13 -&gt; 64K blocks
   *   <li>12 -&gt; 32K blocks
   *   <li>11 -&gt; 16K blocks
   *   <li>10 -&gt; 8K blocks
   *   <li>9 -&gt; 4K blocks
   *   <li>8 -&gt; 2K blocks
   *   <li>7 -&gt; 1K blocks
   *   <li>6 -&gt; 512b blocks
   *   <li>5 -&gt; 256b blocks
   *   <li>4 -&gt; 128b blocks
   * </ul>
   */
  public static final int WORDS_SHIFT;
  static {
    // here we work based on the assumption that the min and max heap sizes will be the same, and
    // will guide the heap region sizing
    long maxMemory = Runtime.getRuntime().maxMemory();
    int exp = 64 - Long.numberOfLeadingZeros(maxMemory) - 1; // round down to nearest power of 2
    int adjust = -1;
    // `- 16` below to map exponent to corresponding `long[]` size
    WORDS_SHIFT = Math.min(17, Math.max(7, (exp - 16) + adjust)); // 1K <= MAX_BLOCK_SIZE <= 1M
    System.err.println("exp="+exp);
    System.err.println("MEM="+(maxMemory >> 20)+"m, maxBlockSize="+(1 << WORDS_SHIFT)+" ("+(((1 << WORDS_SHIFT) * 8) >> 10)+"k) WORDS_SHIFT="+WORDS_SHIFT+" / "+RamUsageEstimator.humanReadableUnits(RamUsageEstimator.sizeOf(new long[1 << WORDS_SHIFT])));
  }

  public static void main(String[] args) {

  }

  private static final int MAX_BLOCK_SIZE = 1 << WORDS_SHIFT;
  public static final int BLOCK_MASK = MAX_BLOCK_SIZE - 1;

  private final long[][] bits; // Array of longs holding the bits
  private final int numBits; // The number of bits in use
  private final int numWords; // The exact number of longs needed to hold numBits (<= bits.length)

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
      // TODO: we should oversize here at high level
      long[][] arr = bits.getBits();
      if (arr.length == 0) {
        return new FixedBitSet(numBits + 1);
      }
      int numWords = bits2words(numBits + 1);
      int lastIdx = arr.length - 1;
      long[] a = arr[lastIdx];
      int extantWords = (MAX_BLOCK_SIZE * lastIdx) + a.length;
      int shortfall = numWords - extantWords;
      if (shortfall < 1) {
        lastIdx = arr.length - 1;
        int newNumWords = (lastIdx * MAX_BLOCK_SIZE) + arr[lastIdx].length;
        return new FixedBitSet(new BitsBuilder(arr), newNumWords << 6);
      }
      arr = ArrayUtil.growExact(arr, ((numWords - 1) >> WORDS_SHIFT) + 1);
      if (a.length < MAX_BLOCK_SIZE) {
        // room to grow last extant array
        int grow = Math.min(shortfall, MAX_BLOCK_SIZE - a.length);
        arr[lastIdx] = ArrayUtil.growExact(a, a.length + grow);
        shortfall -= grow;
      }
      if (shortfall > 0) {
        int innerInit = ((shortfall - 1) & BLOCK_MASK) + 1;
        for (int i = arr.length - 1; i > lastIdx; i--) {
          arr[i] = new long[innerInit];
          innerInit = MAX_BLOCK_SIZE;
        }
      }
      lastIdx = arr.length - 1;
      int newNumWords = (lastIdx * MAX_BLOCK_SIZE) + arr[lastIdx].length;
      assert (newNumWords << 6) > numBits;
      return new FixedBitSet(new BitsBuilder(arr), newNumWords << 6);
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
    long tot = 0;
    final int numCommonWords = Math.min(a.numWords, b.numWords);
    if (numCommonWords < 1) return 0;
    int subInit = (numCommonWords - 1) & BLOCK_MASK;
    int maxCommonBlock = (numCommonWords - 1) >> WORDS_SHIFT;
    for (int j = maxCommonBlock; j >= 0; j--) {
      long[] a1 = a.bits[j];
      long[] b1 = b.bits[j];
      for (int i = subInit; i >= 0; --i) {
        tot += Long.bitCount(a1[i] & b1[i]);
      }
      subInit = BLOCK_MASK;
    }
    return tot;
  }

  /** Returns the popcount or cardinality of the union of the two sets. Neither set is modified. */
  public static long unionCount(FixedBitSet a, FixedBitSet b) {
    // Depends on the ghost bits being clear!
    long tot = 0;
    final int numCommonWords = Math.min(a.numWords, b.numWords);
    if (numCommonWords > 0) {
      int subInit = (numCommonWords - 1) & BLOCK_MASK;
      int maxCommonBlock = (numCommonWords - 1) >> WORDS_SHIFT;
      for (int j = maxCommonBlock; j >= 0; j--) {
        long[] a1 = a.bits[j];
        long[] b1 = b.bits[j];
        for (int i = subInit; i >= 0; --i) {
          tot += Long.bitCount(a1[i] | b1[i]);
        }
        subInit = BLOCK_MASK;
      }
    }
    if (a.numWords == b.numWords) {
      return tot;
    } else if (a.numWords < b.numWords) {
      // from here on we only deal with `a` (as possibly re-assigned)
      a = b;
    }
    int minRelevantBlock = numCommonWords >> WORDS_SHIFT;
    int subInit = (a.numWords - 1) & BLOCK_MASK;
    for (int j = (a.numWords - 1) >> WORDS_SHIFT; j >= minRelevantBlock; j--) {
      long[] a1 = a.bits[j];
      for (int i = subInit, limit = (j > minRelevantBlock ? 0 : (numCommonWords & BLOCK_MASK)); i >= limit; --i) {
        tot += Long.bitCount(a1[i]);
      }
      subInit = BLOCK_MASK;
    }
    return tot;
  }

  /**
   * Returns the popcount or cardinality of "a and not b" or "intersection(a, not(b))". Neither set
   * is modified.
   */
  public static long andNotCount(FixedBitSet a, FixedBitSet b) {
    // Depends on the ghost bits being clear!
    long tot = 0;
    final int numCommonWords = Math.min(a.numWords, b.numWords);
    if (numCommonWords > 0) {
      int subInit = (numCommonWords - 1) & BLOCK_MASK;
      int maxCommonBlock = (numCommonWords - 1) >> WORDS_SHIFT;
      for (int j = maxCommonBlock; j >= 0; j--) {
        long[] a1 = a.bits[j];
        long[] b1 = b.bits[j];
        for (int i = subInit; i >= 0; --i) {
          tot += Long.bitCount(a1[i] & ~b1[i]);
        }
        subInit = BLOCK_MASK;
      }
    }
    int minRelevantBlock = numCommonWords >> WORDS_SHIFT;
    int subInit = (a.numWords - 1) & BLOCK_MASK;
    for (int j = (a.numWords - 1) >> WORDS_SHIFT; j >= minRelevantBlock; j--) {
      long[] a1 = a.bits[j];
      for (int i = subInit, limit = (j > minRelevantBlock ? 0 : (numCommonWords & BLOCK_MASK)); i >= limit; --i) {
        tot += Long.bitCount(a1[i]);
      }
      subInit = BLOCK_MASK;
    }
    return tot;
  }

  public void writeTo(DataOutput out, boolean writeNumWords) throws IOException {
    if (writeNumWords) {
      out.writeInt(numWords);
    }
    for (int i = 0, lim = (numWords - 1) >> WORDS_SHIFT; i <= lim; i++) {
      long[] a = bits[i];
      for (int j = 0, innerLim = i == lim ? ((numWords - 1) & BLOCK_MASK) : BLOCK_MASK; j <= innerLim; j++) {
        // Can't used VLong encoding because cant cope with negative numbers
        // output by FixedBitSet
        out.writeLong(a[j]);
      }
    }
  }

  public static final class BitsBuilder {
    private final long[][] bits;
    public BitsBuilder(int numWords) {
      this.bits = new long[((numWords - 1) >> WORDS_SHIFT) + 1][];
      initBits(numWords, bits);
    }
    private BitsBuilder(long[][] bits) {
      for (long[] a : bits) {
        assert a.length <= MAX_BLOCK_SIZE;
      }
      this.bits = bits;
    }
    public void set(int idx, long val) {
      bits[idx >> WORDS_SHIFT][idx & BLOCK_MASK] = val;
    }
    public void or(int idx, long val) {
      bits[idx >> WORDS_SHIFT][idx & BLOCK_MASK] |= val;
    }
    public void read(IndexInput in, int off, int len) throws IOException {
      // TODO: fix this to not need this assertion
      assert off == 0;
      int innerOff = 0;
      for (long[] a : bits) {
        int toRead = Math.min(len, MAX_BLOCK_SIZE);
        in.readLongs(a, innerOff, toRead);
        if ((len -= toRead) <= 0) {
          break;
        }
        innerOff = 0;
      }
    }
    public void and(int idx, long val) {
      bits[idx >> WORDS_SHIFT][idx & BLOCK_MASK] &= val;
    }
  }

  private static void initBits(int numWords, long[][] bits) {
    int innerLen = ((numWords - 1) & BLOCK_MASK) + 1;
    for (int i = bits.length - 1; i >= 0; i--) {
      bits[i] = new long[innerLen];
      innerLen = MAX_BLOCK_SIZE;
    }
  }

  /**
   * Creates a new LongBitSet. The internally allocated long array will be exactly the size needed
   * to accommodate the numBits specified.
   *
   * @param numBits the number of bits needed
   */
  public FixedBitSet(int numBits) {
    this.numBits = numBits;
    numWords = bits2words(numBits);
    bits = new long[((numWords - 1) >> WORDS_SHIFT) + 1][];
    initBits(numWords, bits);
  }

  /**
   * Creates a new LongBitSet using the provided long[] array as backing store. The storedBits array
   * must be large enough to accommodate the numBits specified, but may be larger. In that case the
   * 'extra' or 'ghost' bits must be clear (or they may provoke spurious side-effects)
   *
   * @param storedBits the array to use as backing store
   * @param numBits the number of bits actually needed
   */
  public FixedBitSet(BitsBuilder storedBits, int numBits) {
    this.numWords = bits2words(numBits);
    if (numWords > Arrays.stream(storedBits.bits).mapToInt((a) -> a.length).sum()) {
      throw new IllegalArgumentException(
          "The given long array is too small  to hold " + numBits + " bits");
    }
    this.numBits = numBits;
    this.bits = storedBits.bits;

    assert verifyGhostBitsClear();
  }

  @Override
  public void clear() {
    for (long[] a : bits) {
      Arrays.fill(a, 0L);
    }
  }

  /**
   * Checks if the bits past numBits are clear. Some methods rely on this implicit assumption:
   * search for "Depends on the ghost bits being clear!"
   *
   * @return true if the bits past numBits are clear.
   */
  private boolean verifyGhostBitsClear() {
    for (int i = Math.max(numWords - 1, 0) >> WORDS_SHIFT; i < bits.length; i++) {
      long[] a = bits[i];
      for (int j = ((numWords - 1) & BLOCK_MASK) + 1; j < a.length; j++) {
        if (a[j] != 0) return false;
      }
    }
    if ((numBits & 0x3f) == 0) return true;

    long mask = -1L << numBits;

    if (numWords < 1) return true;
    int lastIdx = numWords - 1;
    return (bits[lastIdx >> WORDS_SHIFT][lastIdx & BLOCK_MASK] & mask) == 0;
  }

  @Override
  public int length() {
    return numBits;
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + RamUsageEstimator.shallowSizeOf(bits) + Arrays.stream(bits).mapToLong(RamUsageEstimator::sizeOf).sum();
  }

  /** Expert. */
  public long[][] getBits() {
    return bits;
  }

  /**
   * Returns number of set bits. NOTE: this visits every long in the backing bits array, and the
   * result is not internally cached!
   */
  @Override
  public int cardinality() {
    // Depends on the ghost bits being clear!
    long tot = 0;
    if (numWords == 0) return 0;
    int subInit = (numWords - 1) & BLOCK_MASK;
    for (int j = (numWords - 1) >> WORDS_SHIFT; j >= 0; j--) {
      long[] a = bits[j];
      for (int i = subInit; i >= 0; --i) {
        tot += Long.bitCount(a[i]);
      }
      subInit = BLOCK_MASK;
    }
    return Math.toIntExact(tot);
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
      long[] a = bits[maxWord >> WORDS_SHIFT];
      for (int i = (maxWord & BLOCK_MASK), lim = ((maxWord + rangeLength - 1) & BLOCK_MASK) + 1; i < lim; ++i) {
        popCount += Long.bitCount(a[i]);
      }
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
    return (bits[i >> WORDS_SHIFT][i & BLOCK_MASK] & bitmask) != 0;
  }

  @Override
  public void set(int index) {
    assert index >= 0 && index < numBits : "index=" + index + ", numBits=" + numBits;
    int wordNum = index >> 6; // div 64
    long bitmask = 1L << index;
    bits[wordNum >> WORDS_SHIFT][wordNum & BLOCK_MASK] |= bitmask;
  }

  @Override
  public boolean getAndSet(int index) {
    assert index >= 0 && index < numBits : "index=" + index + ", numBits=" + numBits;
    int wordNum = index >> 6; // div 64
    long bitmask = 1L << index;
    int i = wordNum >> WORDS_SHIFT;
    int j = wordNum & BLOCK_MASK;
    boolean val = (bits[i][j] & bitmask) != 0;
    bits[i][j] |= bitmask;
    return val;
  }

  @Override
  public void clear(int index) {
    assert index >= 0 && index < numBits : "index=" + index + ", numBits=" + numBits;
    int wordNum = index >> 6;
    long bitmask = 1L << index;
    bits[wordNum >> WORDS_SHIFT][wordNum & BLOCK_MASK] &= ~bitmask;
  }

  public boolean getAndClear(int index) {
    assert index >= 0 && index < numBits : "index=" + index + ", numBits=" + numBits;
    int wordNum = index >> 6; // div 64
    long bitmask = 1L << index;
    int i = wordNum >> WORDS_SHIFT;
    int j = wordNum & BLOCK_MASK;
    boolean val = (bits[i][j] & bitmask) != 0;
    bits[i][j] &= ~bitmask;
    return val;
  }

  @Override
  public int nextSetBit(int index) {
    // Depends on the ghost bits being clear!
    assert index >= 0 && index < numBits : "index=" + index + ", numBits=" + numBits;
    int from = index >> 6;
    long word = bits[from >> WORDS_SHIFT][from & BLOCK_MASK] >> index; // skip all the bits to the right of index

    if (word != 0) {
      return index + Long.numberOfTrailingZeros(word);
    }

    int innerInit = ++from & BLOCK_MASK;
    for (int i = from >> WORDS_SHIFT, outerLim = ((numWords - 1) >> WORDS_SHIFT); i <= outerLim; i++) {
      long[] a = bits[i];
      for (int j = innerInit, lim = (i == outerLim ? ((numWords - 1) & BLOCK_MASK) : BLOCK_MASK); j <= lim; j++) {
        word = a[j];
        if (word != 0) {
          return (((i << WORDS_SHIFT) | j) << 6) + Long.numberOfTrailingZeros(word);
        }
      }
      innerInit = 0;
    }

    return DocIdSetIterator.NO_MORE_DOCS;
  }

  @Override
  public int prevSetBit(int index) {
    assert index >= 0 && index < numBits : "index=" + index + " numBits=" + numBits;
    int from = index >> 6;
    final int subIndex = index & 0x3f; // index within the word
    long word = (bits[from >> WORDS_SHIFT][from & BLOCK_MASK] << (63 - subIndex)); // skip all the bits to the left of index

    if (word != 0) {
      return (from << 6) + subIndex - Long.numberOfLeadingZeros(word); // See LUCENE-3197
    }

    int innerInit = --from & BLOCK_MASK;
    for (int i = from >> WORDS_SHIFT; i >= 0; i--) {
      long[] a = bits[i];
      for (int j = innerInit; j >= 0; j--) {
        word = a[j];
        if (word != 0) {
          return (((i << WORDS_SHIFT) | j) << 6) + 63 - Long.numberOfLeadingZeros(word);
        }
      }
      innerInit = BLOCK_MASK;
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
    or(0, other.bits, other.numWords);
  }

  private void or(final int otherOffsetWords, FixedBitSet other) {
    or(otherOffsetWords, other.bits, other.numWords);
  }

  private void or(final int otherOffsetWords, final long[][] otherArr, final int otherNumWords) {
    assert otherNumWords + otherOffsetWords <= numWords
        : "numWords=" + numWords + ", otherNumWords=" + otherNumWords;
    int maxWord = Math.min(numWords - otherOffsetWords, otherNumWords);
    final long[][] thisArr = this.bits;
    int initInner = (maxWord - 1) & BLOCK_MASK;

    int localPos = maxWord + otherOffsetWords;
    for (int i = (maxWord - 1) >> WORDS_SHIFT; i >= 0; i--) {
      long[] b = otherArr[i];
      for (int j = initInner; j >= 0; j--) {
        localPos--;
        thisArr[localPos >> WORDS_SHIFT][localPos & BLOCK_MASK] |= b[j];
      }
      initInner = BLOCK_MASK;
    }
  }

  /** this = this XOR other */
  public void xor(FixedBitSet other) {
    xor(other.bits, other.numWords);
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

  private void xor(long[][] otherBits, int otherNumWords) {
    assert otherNumWords <= numWords : "numWords=" + numWords + ", other.numWords=" + otherNumWords;
    final long[][] thisBits = this.bits;
    int maxWord = Math.min(numWords, otherNumWords);
    int initInner = (maxWord - 1) & BLOCK_MASK;
    for (int i = (maxWord - 1) >> WORDS_SHIFT; i >= 0; i--) {
      long[] a = thisBits[i];
      long[] b = otherBits[i];
      for (int j = initInner; j >= 0; j--) {
        a[j] ^= b[j];
      }
      initInner = BLOCK_MASK;
    }
  }

  /** returns true if the sets have any elements in common */
  public boolean intersects(FixedBitSet other) {
    // Depends on the ghost bits being clear!
    int numWords = Math.min(this.numWords, other.numWords);
    if (numWords < 1) return false;

    int initInner = (numWords - 1) & BLOCK_MASK;

    for (int i = (numWords - 1) >> WORDS_SHIFT; i >= 0; i--) {
      long[] a = bits[i];
      long[] b = other.bits[i];
      for (int j = initInner; j >= 0; j--) {
        if ((a[j] & b[j]) != 0) return true;
      }
      initInner = BLOCK_MASK;
    }

    return false;
  }

  /** this = this AND other */
  public void and(FixedBitSet other) {
    and(other.bits, other.numWords);
  }

  private void and(final long[][] otherArr, final int otherNumWords) {
    final long[][] thisArr = this.bits;
    int maxWord = Math.min(this.numWords, otherNumWords);
    int initInner = (maxWord - 1) & BLOCK_MASK;
    for (int i = (maxWord - 1) >> WORDS_SHIFT; i >= 0; i--) {
      long[] a = thisArr[i];
      long[] b = otherArr[i];
      for (int j = initInner; j >= 0; j--) {
        a[j] &= b[j];
      }
      initInner = BLOCK_MASK;
    }
    if (this.numWords > otherNumWords) {
      int innerInit = otherNumWords & BLOCK_MASK;
      for (int i = otherNumWords >> WORDS_SHIFT, lim = ((this.numWords - 1) >> WORDS_SHIFT); i <= lim; i++) {
        int toIdx = i == lim ? (((this.numWords - 1) & BLOCK_MASK) + 1) : MAX_BLOCK_SIZE;
        Arrays.fill(thisArr[i], innerInit, toIdx, 0L);
        innerInit = 0;
      }
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
    andNot(0, other.bits, other.numWords);
  }

  private void andNot(final int otherOffsetWords, FixedBitSet other) {
    andNot(otherOffsetWords, other.bits, other.numWords);
  }

  private void andNot(final int otherOffsetWords, final long[][] otherArr, final int otherNumWords) {
    int maxWord = Math.min(numWords - otherOffsetWords, otherNumWords);
    final long[][] thisArr = this.bits;
    int initInner = (maxWord - 1) & BLOCK_MASK;

    int localPos = maxWord + otherOffsetWords;
    for (int i = (maxWord - 1) >> WORDS_SHIFT; i >= 0; i--) {
      long[] b = otherArr[i];
      for (int j = initInner; j >= 0; j--) {
        localPos--;
        thisArr[localPos >> WORDS_SHIFT][localPos & BLOCK_MASK] &= ~b[j];
      }
      initInner = BLOCK_MASK;
    }
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
    if (numWords < 1) return true;

    int initInner = (numWords - 1) & BLOCK_MASK;

    for (int i = (numWords - 1) >> WORDS_SHIFT; i >= 0; i--) {
      long[] a = bits[i];
      for (int j = initInner; j >= 0; j--) {
        if (a[i] != 0) return false;
      }
      initInner = BLOCK_MASK;
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
      bits[startWord >> WORDS_SHIFT][startWord & BLOCK_MASK] ^= (startmask & endmask);
      return;
    }

    bits[startWord >> WORDS_SHIFT][startWord & BLOCK_MASK] ^= startmask;

    int firstFullWord = startWord + 1;
    int firstBlock = firstFullWord >> WORDS_SHIFT;
    int innerLim = firstFullWord & BLOCK_MASK;
    for (int i = firstBlock, lim = (endWord - 1) >> WORDS_SHIFT; i <= lim; i++) {
      long[] a = bits[i];
      for (int j = i == lim ? ((endWord - 1) & BLOCK_MASK) : BLOCK_MASK; j >= innerLim; j--) {
        a[j] = ~a[j];
      }
      innerLim = 0;
    }

    bits[endWord >> WORDS_SHIFT][endWord & BLOCK_MASK] ^= endmask;
  }

  /** Flip the bit at the provided index. */
  public void flip(int index) {
    assert index >= 0 && index < numBits : "index=" + index + " numBits=" + numBits;
    int wordNum = index >> 6; // div 64
    long bitmask = 1L << index; // mod 64 is implicit
    bits[wordNum >> WORDS_SHIFT][wordNum & BLOCK_MASK] ^= bitmask;
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
      bits[startWord >> WORDS_SHIFT][startWord & BLOCK_MASK] |= (startmask & endmask);
      return;
    }

    bits[startWord >> WORDS_SHIFT][startWord & BLOCK_MASK] |= startmask;
    int firstFullWord = startWord + 1;
    int firstBlock = firstFullWord >> WORDS_SHIFT;
    int startIdx = firstFullWord & BLOCK_MASK;
    for (int i = firstBlock, lim = (endWord - 1) >> WORDS_SHIFT; i <= lim; i++) {
      long[] a = bits[i];
      Arrays.fill(a, startIdx, i == lim ? (((endWord - 1) & BLOCK_MASK) + 1) : MAX_BLOCK_SIZE, -1L);
      startIdx = 0;
    }
    bits[endWord >> WORDS_SHIFT][endWord & BLOCK_MASK] |= endmask;
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
      bits[startWord >> WORDS_SHIFT][startWord & BLOCK_MASK] &= (startmask | endmask);
      return;
    }

    bits[startWord >> WORDS_SHIFT][startWord & BLOCK_MASK] &= startmask;
    int firstFullWord = startWord + 1;
    int firstBlock = firstFullWord >> WORDS_SHIFT;
    int startIdx = firstFullWord & BLOCK_MASK;
    for (int i = firstBlock, lim = (endWord - 1) >> WORDS_SHIFT; i <= lim; i++) {
      long[] a = bits[i];
      Arrays.fill(a, startIdx, i == lim ? (((endWord - 1) & BLOCK_MASK) + 1) : MAX_BLOCK_SIZE, 0L);
      startIdx = 0;
    }
    bits[endWord >> WORDS_SHIFT][endWord & BLOCK_MASK] &= endmask;
  }

  @Override
  public FixedBitSet clone() {
    int blockCount = ((numWords - 1) >> WORDS_SHIFT) + 1;
    long[][] bits = new long[blockCount][];
    int limit = ((numWords - 1) & BLOCK_MASK) + 1;
    for (int i = blockCount - 1; i >= 0 ; i--) {
      long[] a = new long[limit];
      System.arraycopy(this.bits[i], 0, a, 0, limit);
      bits[i] = a;
      limit = MAX_BLOCK_SIZE;
    }
    return new FixedBitSet(new BitsBuilder(bits), numBits);
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
    if (bits.length != other.bits.length) {
      return false;
    }

    for (int i = bits.length - 1; i >= 0; i--) {
      if (!Arrays.equals(bits[i], other.bits[i])) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    // Depends on the ghost bits being clear!
    long h = 0;
    int initInner = (numWords - 1) & BLOCK_MASK;

    for (int i = (numWords - 1) >> WORDS_SHIFT; i >= 0; i--) {
      long[] a = bits[i];
      for (int j = initInner; j >= 0; j--) {
        h ^= a[j];
        h = (h << 1) | (h >>> 63); // rotate left
      }
      initInner = BLOCK_MASK;
    }
    // fold leftmost bits into right and add a constant to prevent
    // empty sets from returning 0, which is too common.
    return (int) ((h >> 32) ^ h) + 0x98761234;
  }

  /** Make a copy of the given bits. */
  public static FixedBitSet copyOf(Bits bits) {
    if (bits instanceof FixedBits) {
      // restore the original FixedBitSet
      FixedBits fixedBits = (FixedBits) bits;
      bits = new FixedBitSet(new BitsBuilder(fixedBits.bits), fixedBits.length);
    }

    if (bits instanceof FixedBitSet) {
      return ((FixedBitSet) bits).clone();
    } else {
      int length = bits.length();
      FixedBitSet bitSet = new FixedBitSet(length);
      bitSet.set(0, length);
      for (int i = 0; i < length; ++i) {
        if (bits.get(i) == false) {
          bitSet.clear(i);
        }
      }
      return bitSet;
    }
  }

  /**
   * Convert this instance to read-only {@link Bits}. This is useful in the case that this {@link
   * FixedBitSet} is returned as a {@link Bits} instance, to make sure that consumers may not get
   * write access back by casting to a {@link FixedBitSet}. NOTE: Changes to this {@link
   * FixedBitSet} will be reflected on the returned {@link Bits}.
   */
  public Bits asReadOnlyBits() {
    return new FixedBits(bits, numBits);
  }
}
