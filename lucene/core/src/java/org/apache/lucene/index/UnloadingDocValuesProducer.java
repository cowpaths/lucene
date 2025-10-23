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

import static org.apache.lucene.index.Unloader.FPIOFunction;

import java.io.IOException;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.util.IOFunction;
import org.apache.lucene.util.automaton.CompiledAutomaton;

/**
 * A {@link DocValuesProducer} that conditionally unloads (and subsequently reloads on-demand)
 * backing resources (via {@link Unloader}).
 */
public class UnloadingDocValuesProducer extends DocValuesProducer {

  private final Unloader<DocValuesProducer> u;

  /**
   * Creates a new instance
   *
   * @param reopen opens/reopens the backing resource
   * @param keepAliveNanos time threshold (since last access) at which the backing resource is
   *     eligible to be unloaded
   * @throws IOException e.g., on error opening backing resource
   */
  public UnloadingDocValuesProducer(
      Unloader.UnloadHelper reporter,
      IOFunction<Unloader<DocValuesProducer>, DocValuesProducer> reopen,
      long keepAliveNanos)
      throws IOException {
    u = new Unloader<>(reporter, reopen, keepAliveNanos, Object::toString);
  }

  @Override
  public void close() throws IOException {
    u.close();
  }

  private final FPIOFunction<DocValuesProducer, String, Void> checkIntegrity =
      (dvp, ignored) -> {
        dvp.checkIntegrity();
        return null;
      };

  @Override
  public void checkIntegrity() throws IOException {
    u.execute(checkIntegrity, null);
  }

  private final FPIOFunction<DocValuesProducer, FieldInfo, NumericDocValues> getNumeric =
      DocValuesProducer::getNumeric;

  @Override
  public NumericDocValues getNumeric(FieldInfo field) throws IOException {
    return u.execute(getNumeric, field);
  }

  private final FPIOFunction<DocValuesProducer, FieldInfo, BinaryDocValues> getBinary =
      DocValuesProducer::getBinary;

  @Override
  public BinaryDocValues getBinary(FieldInfo field) throws IOException {
    return u.execute(getBinary, field);
  }

  private final FPIOFunction<DocValuesProducer, FieldInfo, SortedDocValues> getSorted =
      DocValuesProducer::getSorted;

  @Override
  public SortedDocValues getSorted(FieldInfo field) throws IOException {
    return u.execute(
        getSorted,
        field,
        true,
        (rawSorted, registerRef) -> {
          // wrap so that we can track refs for returned `TermsEnum` instances
          return new FilterSortedDocValues(rawSorted) {
            @Override
            public TermsEnum intersect(CompiledAutomaton automaton) throws IOException {
              return Unloader.wrap(
                  registerRef.trackedInstance(() -> super.intersect(automaton)), registerRef);
            }

            @Override
            public TermsEnum termsEnum() throws IOException {
              return Unloader.wrap(registerRef.trackedInstance(super::termsEnum), registerRef);
            }
          };
        });
  }

  private final FPIOFunction<DocValuesProducer, FieldInfo, SortedNumericDocValues>
      getSortedNumeric = DocValuesProducer::getSortedNumeric;

  @Override
  public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
    return u.execute(getSortedNumeric, field);
  }

  private final FPIOFunction<DocValuesProducer, FieldInfo, SortedSetDocValues> getSortedSet =
      DocValuesProducer::getSortedSet;

  @Override
  public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
    return u.execute(
        getSortedSet,
        field,
        true,
        (rawSorted, registerRef) -> {
          // wrap so that we can track refs for returned `TermsEnum` instances
          return new FilterSortedSetDocValues(rawSorted) {
            @Override
            public TermsEnum intersect(CompiledAutomaton automaton) throws IOException {
              return Unloader.wrap(
                  registerRef.trackedInstance(() -> super.intersect(automaton)), registerRef);
            }

            @Override
            public TermsEnum termsEnum() throws IOException {
              return Unloader.wrap(registerRef.trackedInstance(super::termsEnum), registerRef);
            }
          };
        });
  }
}
