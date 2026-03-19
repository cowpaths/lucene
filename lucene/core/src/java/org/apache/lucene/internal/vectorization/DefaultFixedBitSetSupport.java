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

/** Scalar (non-vectorized) implementation of {@link FixedBitSetSupport}. */
final class DefaultFixedBitSetSupport extends FixedBitSetSupport {

  static final DefaultFixedBitSetSupport INSTANCE = new DefaultFixedBitSetSupport();

  @Override
  public int vectorByteSize() {
    return 0;
  }

  @Override
  public long popCount(MemorySegment ms, LongBuffer bits, int fromWord, int numWords) {
    long tot = 0;
    for (int i = fromWord, lim = fromWord + numWords; i < lim; i++) {
      tot += Long.bitCount(bits.get(i));
    }
    return tot;
  }

  @Override
  public long intersectionPopCount(
      MemorySegment aMs,
      LongBuffer aBits,
      MemorySegment bMs,
      LongBuffer bBits,
      int numCommonWords) {
    long tot = 0;
    for (int i = 0; i < numCommonWords; i++) {
      tot += Long.bitCount(aBits.get(i) & bBits.get(i));
    }
    return tot;
  }

  @Override
  public long unionPopCount(
      MemorySegment aMs,
      LongBuffer aBits,
      int aNumWords,
      MemorySegment bMs,
      LongBuffer bBits,
      int bNumWords) {
    int numCommonWords = Math.min(aNumWords, bNumWords);
    long tot = 0;
    for (int i = 0; i < numCommonWords; i++) {
      tot += Long.bitCount(aBits.get(i) | bBits.get(i));
    }
    for (int i = numCommonWords; i < aNumWords; i++) {
      tot += Long.bitCount(aBits.get(i));
    }
    for (int i = numCommonWords; i < bNumWords; i++) {
      tot += Long.bitCount(bBits.get(i));
    }
    return tot;
  }

  @Override
  public long andNotPopCount(
      MemorySegment aMs,
      LongBuffer aBits,
      int aNumWords,
      MemorySegment bMs,
      LongBuffer bBits,
      int bNumWords) {
    int numCommonWords = Math.min(aNumWords, bNumWords);
    long tot = 0;
    for (int i = 0; i < numCommonWords; i++) {
      tot += Long.bitCount(aBits.get(i) & ~bBits.get(i));
    }
    for (int i = numCommonWords; i < aNumWords; i++) {
      tot += Long.bitCount(aBits.get(i));
    }
    return tot;
  }

  @Override
  public void or(
      MemorySegment thisMs,
      LongBuffer thisBits,
      int otherOffsetWords,
      MemorySegment otherMs,
      LongBuffer otherBits,
      int len) {
    for (int i = 0; i < len; i++) {
      int off = i + otherOffsetWords;
      thisBits.put(off, thisBits.get(off) | otherBits.get(i));
    }
  }

  @Override
  public void xor(
      MemorySegment thisMs,
      LongBuffer thisBits,
      MemorySegment otherMs,
      LongBuffer otherBits,
      int len) {
    for (int i = 0; i < len; i++) {
      thisBits.put(i, thisBits.get(i) ^ otherBits.get(i));
    }
  }

  @Override
  public void and(
      MemorySegment thisMs,
      LongBuffer thisBits,
      MemorySegment otherMs,
      LongBuffer otherBits,
      int len) {
    for (int i = 0; i < len; i++) {
      thisBits.put(i, thisBits.get(i) & otherBits.get(i));
    }
  }

  @Override
  public void andNot(
      MemorySegment thisMs,
      LongBuffer thisBits,
      int otherOffsetWords,
      MemorySegment otherMs,
      LongBuffer otherBits,
      int len) {
    for (int i = 0; i < len; i++) {
      int off = i + otherOffsetWords;
      thisBits.put(off, thisBits.get(off) & ~otherBits.get(i));
    }
  }

  @Override
  public void flipWords(MemorySegment ms, LongBuffer bits, int fromWord, int toWord) {
    for (int i = fromWord; i < toWord; i++) {
      bits.put(i, ~bits.get(i));
    }
  }

  @Override
  public void fill(MemorySegment ms, LongBuffer bits, int startWord, int endWord, long val) {
    for (int i = startWord; i < endWord; i++) {
      bits.put(i, val);
    }
  }
}
