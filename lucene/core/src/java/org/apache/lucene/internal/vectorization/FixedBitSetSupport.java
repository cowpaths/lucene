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
package org.apache.lucene.internal.vectorization;

import java.lang.foreign.MemorySegment;
import java.nio.LongBuffer;

/**
 * Provider of vectorized implementations of bit-set operations for {@link
 * org.apache.lucene.util.FixedBitSet}.
 *
 * @lucene.internal
 */
public abstract class FixedBitSetSupport {

  FixedBitSetSupport() {}

  /**
   * Returns the preferred vector byte size for this provider, or 0 if vectorization is not
   * available. Used to decide whether to wrap buffers with {@link MemorySegment}.
   */
  public abstract int vectorByteSize();

  /** Popcount of {@code numWords} longs in {@code bits} starting at {@code fromWord}. */
  public abstract long popCount(MemorySegment ms, LongBuffer bits, int fromWord, int numWords);

  /** Popcount of {@code (aBits[i] & bBits[i])} for {@code i} in {@code [0, numCommonWords)}. */
  public abstract long intersectionPopCount(
      MemorySegment aMs,
      LongBuffer aBits,
      MemorySegment bMs,
      LongBuffer bBits,
      int numCommonWords);

  /**
   * Popcount of the union of {@code aBits[0..aNumWords)} and {@code bBits[0..bNumWords)},
   * equivalent to {@code popcount(a | b)} but without materializing the union.
   */
  public abstract long unionPopCount(
      MemorySegment aMs,
      LongBuffer aBits,
      int aNumWords,
      MemorySegment bMs,
      LongBuffer bBits,
      int bNumWords);

  /**
   * Popcount of {@code (aBits[i] & ~bBits[i])} for common words, plus popcount of remaining
   * {@code aBits} words beyond the common prefix.
   */
  public abstract long andNotPopCount(
      MemorySegment aMs,
      LongBuffer aBits,
      int aNumWords,
      MemorySegment bMs,
      LongBuffer bBits,
      int bNumWords);

  /**
   * In-place OR: {@code thisBits[otherOffsetWords + i] |= otherBits[i]} for {@code i} in {@code
   * [0, len)}.
   */
  public abstract void or(
      MemorySegment thisMs,
      LongBuffer thisBits,
      int otherOffsetWords,
      MemorySegment otherMs,
      LongBuffer otherBits,
      int len);

  /** In-place XOR: {@code thisBits[i] ^= otherBits[i]} for {@code i} in {@code [0, len)}. */
  public abstract void xor(
      MemorySegment thisMs,
      LongBuffer thisBits,
      MemorySegment otherMs,
      LongBuffer otherBits,
      int len);

  /** In-place AND: {@code thisBits[i] &= otherBits[i]} for {@code i} in {@code [0, len)}. */
  public abstract void and(
      MemorySegment thisMs,
      LongBuffer thisBits,
      MemorySegment otherMs,
      LongBuffer otherBits,
      int len);

  /**
   * In-place AND-NOT: {@code thisBits[otherOffsetWords + i] &= ~otherBits[i]} for {@code i} in
   * {@code [0, len)}.
   */
  public abstract void andNot(
      MemorySegment thisMs,
      LongBuffer thisBits,
      int otherOffsetWords,
      MemorySegment otherMs,
      LongBuffer otherBits,
      int len);

  /** NOT in-place: {@code bits[i] = ~bits[i]} for {@code i} in {@code [fromWord, toWord)}. */
  public abstract void flipWords(
      MemorySegment ms, LongBuffer bits, int fromWord, int toWord);

  /**
   * Fill: {@code bits[i] = val} for {@code i} in {@code [startWord, endWord)}. {@code val} must
   * be {@code 0L} or {@code -1L}.
   */
  public abstract void fill(
      MemorySegment ms, LongBuffer bits, int startWord, int endWord, long val);
}
