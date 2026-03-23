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

import java.nio.Buffer;
import java.nio.LongBuffer;

/**
 * Provider of vectorized implementations of bit-set operations for {@link
 * org.apache.lucene.util.FixedBitSet}.
 *
 * <p>The {@code ms} parameters are {@code MemorySegment} instances (from {@code
 * java.lang.foreign}) held as {@code Object} to avoid a compile-time dependency on that API in
 * base (JDK 11) source. Panama implementations cast them internally; the scalar implementation
 * ignores them.
 *
 * @lucene.internal
 */
public abstract class FixedBitSetSupport {

  FixedBitSetSupport() {}

  /**
   * Returns the preferred vector byte size for this provider, or 0 if vectorization is not
   * available. Used to decide whether to wrap buffers with a {@code MemorySegment}.
   */
  public abstract int vectorByteSize();

  /**
   * Wraps a {@link Buffer} in a {@code MemorySegment}, or returns {@code null} if vectorization
   * is not available.
   */
  public abstract Object wrapBuffer(Buffer buf);

  /** Popcount of {@code numWords} longs in {@code bits} starting at {@code fromWord}. */
  public abstract long popCount(Object ms, LongBuffer bits, int fromWord, int numWords);

  /** Popcount of {@code (aBits[i] & bBits[i])} for {@code i} in {@code [0, numCommonWords)}. */
  public abstract long intersectionPopCount(
      Object aMs, LongBuffer aBits, Object bMs, LongBuffer bBits, int numCommonWords);

  /**
   * Popcount of the union of {@code aBits[0..aNumWords)} and {@code bBits[0..bNumWords)},
   * equivalent to {@code popcount(a | b)} but without materializing the union.
   */
  public abstract long unionPopCount(
      Object aMs, LongBuffer aBits, int aNumWords, Object bMs, LongBuffer bBits, int bNumWords);

  /**
   * Popcount of {@code (aBits[i] & ~bBits[i])} for common words, plus popcount of remaining
   * {@code aBits} words beyond the common prefix.
   */
  public abstract long andNotPopCount(
      Object aMs, LongBuffer aBits, int aNumWords, Object bMs, LongBuffer bBits, int bNumWords);

  /**
   * In-place OR: {@code thisBits[otherOffsetWords + i] |= otherBits[i]} for {@code i} in {@code
   * [0, len)}.
   */
  public abstract void or(
      Object thisMs,
      LongBuffer thisBits,
      int otherOffsetWords,
      Object otherMs,
      LongBuffer otherBits,
      int len);

  /** In-place XOR: {@code thisBits[i] ^= otherBits[i]} for {@code i} in {@code [0, len)}. */
  public abstract void xor(
      Object thisMs, LongBuffer thisBits, Object otherMs, LongBuffer otherBits, int len);

  /** In-place AND: {@code thisBits[i] &= otherBits[i]} for {@code i} in {@code [0, len)}. */
  public abstract void and(
      Object thisMs, LongBuffer thisBits, Object otherMs, LongBuffer otherBits, int len);

  /**
   * In-place AND-NOT: {@code thisBits[otherOffsetWords + i] &= ~otherBits[i]} for {@code i} in
   * {@code [0, len)}.
   */
  public abstract void andNot(
      Object thisMs,
      LongBuffer thisBits,
      int otherOffsetWords,
      Object otherMs,
      LongBuffer otherBits,
      int len);

  /** NOT in-place: {@code bits[i] = ~bits[i]} for {@code i} in {@code [fromWord, toWord)}. */
  public abstract void flipWords(Object ms, LongBuffer bits, int fromWord, int toWord);

  /**
   * Fill: {@code bits[i] = val} for {@code i} in {@code [startWord, endWord)}. {@code val} must
   * be {@code 0L} or {@code -1L}.
   */
  public abstract void fill(Object ms, LongBuffer bits, int startWord, int endWord, long val);

  /**
   * Calls native funciton madvise, if available. Returns {@code true} if the call was
   * successful/supported, otherwise {@code false}.
   */
  public abstract boolean madvise(Object ms, int advice) throws Throwable;
}
