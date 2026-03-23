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

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.Buffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/** Panama Vector API implementation of {@link FixedBitSetSupport}. */
@SuppressWarnings("preview")
final class PanamaFixedBitSetSupport extends FixedBitSetSupport {

  private static final VectorSpecies<Long> S = LongVector.SPECIES_PREFERRED;
  private static final int INC = S.length();
  private static final int VECTOR_BYTE_SIZE = S.vectorByteSize();
  private static final ByteOrder NATIVE_ORDER = ByteOrder.nativeOrder();

  private static final MethodHandle MADVISE_HANDLE = getMadviseHandle();

  private static MethodHandle getMadviseHandle() {
    Linker linker = Linker.nativeLinker();
    // Look up the symbol once during class loading
    SymbolLookup stdlib = linker.defaultLookup();

    MemorySegment madviseAddress =
        stdlib.find("madvise").orElseThrow(() -> new RuntimeException("madvise not found"));

    // Function signature: int madvise(void *addr, size_t length, int advice)
    FunctionDescriptor descriptor =
        FunctionDescriptor.of(
            ValueLayout.JAVA_INT, // Return type
            ValueLayout.ADDRESS, // addr
            ValueLayout.JAVA_LONG, // length (size_t)
            ValueLayout.JAVA_INT // advice
        );

    return linker.downcallHandle(madviseAddress, descriptor);
  }

  @Override
  public boolean madvise(Object ms, int advice) throws Throwable {
    MemorySegment segment = (MemorySegment) ms;
    // 2. Execute the call directly on the memory segment
    int result = (int) MADVISE_HANDLE.invokeExact(segment, segment.byteSize(), advice);
    return result == 0;
  }

  @Override
  public int vectorByteSize() {
    return VECTOR_BYTE_SIZE;
  }

  @Override
  public Object wrapBuffer(Buffer buf) {
    return MemorySegment.ofBuffer(buf);
  }

  @Override
  public long popCount(Object msObj, LongBuffer bits, int fromWord, int numWords) {
    MemorySegment ms = (MemorySegment) msObj;
    int i = 0;
    LongVector acc = LongVector.zero(S);
    for (int lim = S.loopBound(numWords); i < lim; i += INC) {
      LongVector v =
          LongVector.fromMemorySegment(S, ms, (long) (fromWord + i) << 3, NATIVE_ORDER);
      acc = acc.add(v.lanewise(VectorOperators.BIT_COUNT));
    }
    long tot = acc.reduceLanes(VectorOperators.ADD);
    for (; i < numWords; i++) {
      tot += Long.bitCount(bits.get(fromWord + i));
    }
    return tot;
  }

  @Override
  public long intersectionPopCount(
      Object aMsObj, LongBuffer aBits, Object bMsObj, LongBuffer bBits, int numCommonWords) {
    MemorySegment aMs = (MemorySegment) aMsObj;
    MemorySegment bMs = (MemorySegment) bMsObj;
    int i = 0;
    LongVector acc = LongVector.zero(S);
    for (int lim = S.loopBound(numCommonWords); i < lim; i += INC) {
      long off = (long) i << 3;
      LongVector vA = LongVector.fromMemorySegment(S, aMs, off, NATIVE_ORDER);
      LongVector vB = LongVector.fromMemorySegment(S, bMs, off, NATIVE_ORDER);
      acc = acc.add(vA.and(vB).lanewise(VectorOperators.BIT_COUNT));
    }
    long tot = acc.reduceLanes(VectorOperators.ADD);
    for (; i < numCommonWords; i++) {
      tot += Long.bitCount(aBits.get(i) & bBits.get(i));
    }
    return tot;
  }

  @Override
  public long unionPopCount(
      Object aMsObj,
      LongBuffer aBits,
      int aNumWords,
      Object bMsObj,
      LongBuffer bBits,
      int bNumWords) {
    MemorySegment aMs = (MemorySegment) aMsObj;
    MemorySegment bMs = (MemorySegment) bMsObj;
    final int numCommonWords = Math.min(aNumWords, bNumWords);
    int i = 0;
    LongVector acc = LongVector.zero(S);
    for (int lim = S.loopBound(numCommonWords); i < lim; i += INC) {
      long off = (long) i << 3;
      LongVector vA = LongVector.fromMemorySegment(S, aMs, off, NATIVE_ORDER);
      LongVector vB = LongVector.fromMemorySegment(S, bMs, off, NATIVE_ORDER);
      acc = acc.add(vA.or(vB).lanewise(VectorOperators.BIT_COUNT));
    }
    long tot = 0;
    int alignedLim;
    if (i == numCommonWords) {
      alignedLim = i;
    } else {
      alignedLim = i + INC;
      do {
        tot += Long.bitCount(aBits.get(i) | bBits.get(i));
      } while (++i < numCommonWords);
      for (int lim = Math.min(aNumWords, alignedLim); i < lim; ++i) {
        tot += Long.bitCount(aBits.get(i));
      }
      for (int j = numCommonWords, lim = Math.min(bNumWords, alignedLim); j < lim; ++j) {
        tot += Long.bitCount(bBits.get(j));
      }
    }
    for (int lim = S.loopBound(aNumWords); i < lim; i += INC) {
      LongVector v = LongVector.fromMemorySegment(S, aMs, (long) i << 3, NATIVE_ORDER);
      acc = acc.add(v.lanewise(VectorOperators.BIT_COUNT));
    }
    int j = alignedLim;
    for (int lim = S.loopBound(bNumWords); j < lim; j += INC) {
      LongVector v = LongVector.fromMemorySegment(S, bMs, (long) j << 3, NATIVE_ORDER);
      acc = acc.add(v.lanewise(VectorOperators.BIT_COUNT));
    }
    tot += acc.reduceLanes(VectorOperators.ADD);
    for (; i < aNumWords; ++i) {
      tot += Long.bitCount(aBits.get(i));
    }
    for (; j < bNumWords; ++j) {
      tot += Long.bitCount(bBits.get(j));
    }
    return tot;
  }

  @Override
  public long andNotPopCount(
      Object aMsObj,
      LongBuffer aBits,
      int aNumWords,
      Object bMsObj,
      LongBuffer bBits,
      int bNumWords) {
    MemorySegment aMs = (MemorySegment) aMsObj;
    MemorySegment bMs = (MemorySegment) bMsObj;
    final int numCommonWords = Math.min(aNumWords, bNumWords);
    int i = 0;
    LongVector acc = LongVector.zero(S);
    for (int lim = S.loopBound(numCommonWords); i < lim; i += INC) {
      long off = (long) i << 3;
      LongVector vA = LongVector.fromMemorySegment(S, aMs, off, NATIVE_ORDER);
      LongVector vB = LongVector.fromMemorySegment(S, bMs, off, NATIVE_ORDER);
      acc =
          acc.add(
              vA.lanewise(VectorOperators.AND_NOT, vB).lanewise(VectorOperators.BIT_COUNT));
    }
    long tot = 0;
    if (i < numCommonWords) {
      int alignedLim = i + INC;
      for (; i < numCommonWords; ++i) {
        tot += Long.bitCount(aBits.get(i) & ~bBits.get(i));
      }
      for (int lim = Math.min(aNumWords, alignedLim); i < lim; ++i) {
        tot += Long.bitCount(aBits.get(i));
      }
    }
    for (int lim = S.loopBound(aNumWords); i < lim; i += INC) {
      LongVector v = LongVector.fromMemorySegment(S, aMs, (long) i << 3, NATIVE_ORDER);
      acc = acc.add(v.lanewise(VectorOperators.BIT_COUNT));
    }
    tot += acc.reduceLanes(VectorOperators.ADD);
    for (; i < aNumWords; ++i) {
      tot += Long.bitCount(aBits.get(i));
    }
    return tot;
  }

  @Override
  public void or(
      Object thisMsObj,
      LongBuffer thisBits,
      int otherOffsetWords,
      Object otherMsObj,
      LongBuffer otherBits,
      int len) {
    MemorySegment thisMs = (MemorySegment) thisMsObj;
    MemorySegment otherMs = (MemorySegment) otherMsObj;
    int i = 0;
    for (int lim = S.loopBound(len); i < lim; i += INC) {
      long thisOff = (long) (i + otherOffsetWords) << 3;
      long otherOff = (long) i << 3;
      LongVector vThis = LongVector.fromMemorySegment(S, thisMs, thisOff, NATIVE_ORDER);
      LongVector vOther = LongVector.fromMemorySegment(S, otherMs, otherOff, NATIVE_ORDER);
      vThis.or(vOther).intoMemorySegment(thisMs, thisOff, NATIVE_ORDER);
    }
    for (; i < len; i++) {
      int off = i + otherOffsetWords;
      thisBits.put(off, thisBits.get(off) | otherBits.get(i));
    }
  }

  @Override
  public void xor(
      Object thisMsObj, LongBuffer thisBits, Object otherMsObj, LongBuffer otherBits, int len) {
    MemorySegment thisMs = (MemorySegment) thisMsObj;
    MemorySegment otherMs = (MemorySegment) otherMsObj;
    int i = 0;
    for (int lim = S.loopBound(len); i < lim; i += INC) {
      long off = (long) i << 3;
      LongVector vThis = LongVector.fromMemorySegment(S, thisMs, off, NATIVE_ORDER);
      LongVector vOther = LongVector.fromMemorySegment(S, otherMs, off, NATIVE_ORDER);
      vThis.lanewise(VectorOperators.XOR, vOther).intoMemorySegment(thisMs, off, NATIVE_ORDER);
    }
    for (; i < len; i++) {
      thisBits.put(i, thisBits.get(i) ^ otherBits.get(i));
    }
  }

  @Override
  public void and(
      Object thisMsObj, LongBuffer thisBits, Object otherMsObj, LongBuffer otherBits, int len) {
    MemorySegment thisMs = (MemorySegment) thisMsObj;
    MemorySegment otherMs = (MemorySegment) otherMsObj;
    int i = 0;
    for (int lim = S.loopBound(len); i < lim; i += INC) {
      long off = (long) i << 3;
      LongVector vThis = LongVector.fromMemorySegment(S, thisMs, off, NATIVE_ORDER);
      LongVector vOther = LongVector.fromMemorySegment(S, otherMs, off, NATIVE_ORDER);
      vThis.and(vOther).intoMemorySegment(thisMs, off, NATIVE_ORDER);
    }
    for (; i < len; i++) {
      thisBits.put(i, thisBits.get(i) & otherBits.get(i));
    }
  }

  @Override
  public void andNot(
      Object thisMsObj,
      LongBuffer thisBits,
      int otherOffsetWords,
      Object otherMsObj,
      LongBuffer otherBits,
      int len) {
    MemorySegment thisMs = (MemorySegment) thisMsObj;
    MemorySegment otherMs = (MemorySegment) otherMsObj;
    int i = 0;
    for (int lim = S.loopBound(len); i < lim; i += INC) {
      long thisOff = (long) (i + otherOffsetWords) << 3;
      long otherOff = (long) i << 3;
      LongVector vThis = LongVector.fromMemorySegment(S, thisMs, thisOff, NATIVE_ORDER);
      LongVector vOther = LongVector.fromMemorySegment(S, otherMs, otherOff, NATIVE_ORDER);
      vThis
          .lanewise(VectorOperators.AND_NOT, vOther)
          .intoMemorySegment(thisMs, thisOff, NATIVE_ORDER);
    }
    for (; i < len; i++) {
      int off = i + otherOffsetWords;
      thisBits.put(off, thisBits.get(off) & ~otherBits.get(i));
    }
  }

  @Override
  public void flipWords(Object msObj, LongBuffer bits, int fromWord, int toWord) {
    MemorySegment ms = (MemorySegment) msObj;
    int len = toWord - fromWord;
    int i = fromWord;
    for (int lim = fromWord + S.loopBound(len); i < lim; i += INC) {
      long off = (long) i << 3;
      LongVector v = LongVector.fromMemorySegment(S, ms, off, NATIVE_ORDER);
      v.not().intoMemorySegment(ms, off, NATIVE_ORDER);
    }
    for (; i < toWord; i++) {
      bits.put(i, ~bits.get(i));
    }
  }

  @Override
  public void fill(Object msObj, LongBuffer bits, int startWord, int endWord, long val) {
    MemorySegment ms = (MemorySegment) msObj;
    assert val == 0 || val == -1L; // otherwise ByteOrder matters
    int len = endWord - startWord;
    int i = startWord;
    if (len >= INC) {
      LongVector fillVector = LongVector.broadcast(S, val);
      for (int lim = startWord + S.loopBound(len); i < lim; i += INC) {
        fillVector.intoMemorySegment(ms, (long) i << 3, NATIVE_ORDER);
      }
    }
    for (; i < endWord; i++) {
      bits.put(i, val);
    }
  }
}
