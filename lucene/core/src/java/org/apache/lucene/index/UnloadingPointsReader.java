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
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.util.IOFunction;

/**
 * A {@link DocValuesProducer} that conditionally unloads (and subsequently reloads on-demand)
 * backing resources (via {@link Unloader}).
 */
public class UnloadingPointsReader extends PointsReader {

  private final Unloader<PointsReader> u;

  /**
   * Creates a new instance
   *
   * @param reopen opens/reopens the backing resource
   * @param keepAliveNanos time threshold (since last access) at which the backing resource is
   *     eligible to be unloaded
   * @throws IOException e.g., on error opening backing resource
   */
  public UnloadingPointsReader(
      Unloader.UnloadHelper unloadHelper,
      IOFunction<Unloader<PointsReader>, PointsReader> reopen,
      long keepAliveNanos)
      throws IOException {
    u = new Unloader<>(unloadHelper, reopen, keepAliveNanos, Object::toString);
  }

  @Override
  public void close() throws IOException {
    u.close();
  }

  private final FPIOFunction<PointsReader, String, Void> checkIntegrity =
      (pr, ignored) -> {
        pr.checkIntegrity();
        return null;
      };

  @Override
  public void checkIntegrity() throws IOException {
    u.execute(checkIntegrity, null);
  }

  private final FPIOFunction<PointsReader, String, PointValues> getValues = PointsReader::getValues;

  @Override
  public PointValues getValues(String field) throws IOException {
    return u.execute(
        getValues,
        field,
        (rawPointValues, registerRef) -> {
          // NOTE: we have to wrap here in order to track derived `PointTree` instances
          return new PointValues() {
            @Override
            public PointTree getPointTree() throws IOException {
              return Unloader.wrap(rawPointValues.getPointTree(), registerRef);
            }

            @Override
            public byte[] getMinPackedValue() throws IOException {
              return rawPointValues.getMinPackedValue();
            }

            @Override
            public byte[] getMaxPackedValue() throws IOException {
              return rawPointValues.getMaxPackedValue();
            }

            @Override
            public int getNumDimensions() throws IOException {
              return rawPointValues.getNumDimensions();
            }

            @Override
            public int getNumIndexDimensions() throws IOException {
              return rawPointValues.getNumIndexDimensions();
            }

            @Override
            public int getBytesPerDimension() throws IOException {
              return rawPointValues.getBytesPerDimension();
            }

            @Override
            public long size() {
              return rawPointValues.size();
            }

            @Override
            public int getDocCount() {
              return rawPointValues.getDocCount();
            }
          };
        });
  }
}
