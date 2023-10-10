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
package org.apache.lucene.queryparser.surround.query;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Operations;

/** Query that matches wildcards */
public class SrndTruncQuery extends SimpleTerm {
  public SrndTruncQuery(String truncated, char unlimited, char mask) {
    super(false); /* not quoted */
    this.truncated = truncated;
    this.unlimited = unlimited;
    this.mask = mask;
    WildcardQuery wc = new WildcardQuery(new Term("dummy", truncated), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    compiled = new CompiledAutomaton(wc.getAutomaton(), null, true, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, wc.isAutomatonBinary());
    int i = 0;
    while ((i < truncated.length()) && matchingChar(truncated.charAt(i))) {
      i++;
    }
    prefixRef = new BytesRef(truncated.substring(0, i));
  }

  private final String truncated;
  private final char unlimited;
  private final char mask;

  private final BytesRef prefixRef;
  private final CompiledAutomaton compiled;

  public String getTruncated() {
    return truncated;
  }

  @Override
  public String toStringUnquoted() {
    return getTruncated();
  }

  protected boolean matchingChar(char c) {
    return (c != unlimited) && (c != mask);
  }

  @Override
  public void visitMatchingTerms(IndexReader reader, String fieldName, MatchingTermVisitor mtv)
      throws IOException {
    Terms terms = MultiTerms.getTerms(reader, fieldName);
    if (terms != null) {
      TermsEnum termsEnum = terms.intersect(compiled, prefixRef);

      BytesRef br;
      while ((br = termsEnum.next()) != null) {
        mtv.visitMatchingTerm(new Term(fieldName, BytesRef.deepCopyOf(br)));
      }
    }
  }
}
