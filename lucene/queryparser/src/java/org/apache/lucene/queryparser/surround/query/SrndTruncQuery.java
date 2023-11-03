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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Operations;

/** Query that matches wildcards */
public class SrndTruncQuery extends SimpleTerm {
  public SrndTruncQuery(String truncated) {
    super(false); /* not quoted */
    this.truncated = truncated;
  }

  private final String truncated;
  private CompiledAutomaton compiled;

  public String getTruncated() {
    return truncated;
  }

  @Override
  public String toStringUnquoted() {
    return getTruncated();
  }

  @Override
  public void visitMatchingTerms(
      LeafReader reader, String fieldName, Analyzer analyzer, MatchingTermVisitor mtv)
      throws IOException {
    if (compiled == null) {
      BytesRef wildcardRef = toAnalyzedTerm(truncated, fieldName, analyzer);
      // TODO: there could be some URL-encoded terms that decode to wildcard characters.
      //  we either have to figure out how to escape these (if that's possible, which I
      //  think it should be?), or altogether abandon the idea of analyzed indexed terms
      //  (and the attendant benefits for ngram/maxSubstring.
      WildcardQuery wc =
          new WildcardQuery(
              new Term(fieldName, wildcardRef), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
      compiled =
          new CompiledAutomaton(
              wc.getAutomaton(),
              null,
              true,
              Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
              wc.isAutomatonBinary());
    }
    Terms terms = reader.terms(fieldName);

    if (terms != null) {
      TermsEnum termsEnum = compiled.getTermsEnum(terms);

      BytesRef br;
      while ((br = termsEnum.next()) != null) {
        mtv.visitMatchingTerm(new Term(fieldName, BytesRef.deepCopyOf(br)));
      }
    }
  }
}
