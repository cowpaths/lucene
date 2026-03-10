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

import java.lang.foreign.MemorySegment;
import java.nio.LongBuffer;

/** Immutable twin of FixedBitSet. */
@SuppressWarnings("preview")
final class FixedBits implements Bits {

  final LongBuffer bits;
  final MemorySegment m;
  final int length;

  FixedBits(LongBuffer bits, MemorySegment m, int length) {
    this.bits = bits;
    this.m = m;
    this.length = length;
  }

  @Override
  public boolean get(int index) {
    assert index >= 0 && index < length : "index=" + index + ", numBits=" + length;
    int i = index >> 6; // div 64
    // signed shift will keep a negative index and force an
    // array-index-out-of-bounds-exception, removing the need for an explicit check.
    long bitmask = 1L << index;
    return (bits.get(i) & bitmask) != 0;
  }

  @Override
  public int length() {
    return length;
  }
}
