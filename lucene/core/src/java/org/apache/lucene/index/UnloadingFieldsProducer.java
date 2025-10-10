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
import java.io.UncheckedIOException;
import java.util.Iterator;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOFunction;
import org.apache.lucene.util.automaton.CompiledAutomaton;

/**
 * A {@link DocValuesProducer} that conditionally unloads (and subsequently reloads on-demand)
 * backing resources (via {@link Unloader}).
 */
public class UnloadingFieldsProducer extends FieldsProducer {

  private final int size;

  private final Unloader<FieldsProducer> u;

  /**
   * Creates a new instance
   *
   * @param reopen opens/reopens the backing resource
   * @param keepAliveNanos time threshold (since last access) at which the backing resource is
   *     eligible to be unloaded
   * @throws IOException e.g., on error opening backing resource
   */
  public UnloadingFieldsProducer(
      Unloader.UnloadHelper unloadHelper,
      IOFunction<Unloader<FieldsProducer>, FieldsProducer> reopen,
      long keepAliveNanos)
      throws IOException {
    final int[] size = new int[1];
    u =
        new Unloader<>(
            unloadHelper,
            reopen,
            keepAliveNanos,
            (fp) -> {
              size[0] = fp.size();
              return fp.toString();
            });
    this.size = size[0];
  }

  @Override
  public void close() throws IOException {
    u.close();
  }

  private final FPIOFunction<FieldsProducer, String, Void> checkIntegrity =
      (fp, ignored) -> {
        fp.checkIntegrity();
        return null;
      };

  @Override
  public void checkIntegrity() throws IOException {
    u.execute(checkIntegrity, null);
  }

  private final FPIOFunction<FieldsProducer, String, Iterator<String>> iterator =
      (fp, ignored) -> fp.iterator();

  @Override
  public Iterator<String> iterator() {
    try {
      return u.execute(iterator, null);
    } catch (IOException e) {
      // this should never happen
      throw new UncheckedIOException(e);
    }
  }

  private final FPIOFunction<FieldsProducer, String, Terms> terms = Fields::terms;

  @Override
  public Terms terms(String field) throws IOException {
    return u.execute(
        terms,
        field,
        (rawTerms, registerRef) -> {
          // NOTE: we have to wrap here because a reference to the raw value may be
          // retained internal to the backing `FieldsProducer`. This can generate a
          // profusion of redundant references that never get collected. This is a
          // memory leak, and also prevents resources from being unloaded, even when
          // they should be eligible for unloading.
          //
          // This particular rationale for wrapping only applies to `Terms` -- other
          // resources are already created as one-offs.
          return new FilterLeafReader.FilterTerms(rawTerms) {
            @Override
            public TermsEnum iterator() throws IOException {
              return Unloader.wrap(super.iterator(), registerRef);
            }

            @Override
            public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm)
                throws IOException {
              return Unloader.wrap(super.intersect(compiled, startTerm), registerRef);
            }
          };
        });
  }

  @Override
  public int size() {
    return size;
  }
}
