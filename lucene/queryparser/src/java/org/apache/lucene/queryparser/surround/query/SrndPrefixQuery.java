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
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Operations;

/** Query that matches String prefixes */
public class SrndPrefixQuery extends SimpleTerm {
  private CompiledAutomaton compiled;

  public SrndPrefixQuery(String prefix, boolean quoted, char truncator) {
    super(quoted);
    this.prefix = prefix;
    this.truncator = truncator;
  }

  private final String prefix;

  public String getPrefix() {
    return prefix;
  }

  private final char truncator;

  public char getSuffixOperator() {
    return truncator;
  }

  @Override
  public String toStringUnquoted() {
    return getPrefix();
  }

  @Override
  protected void suffixToString(StringBuilder r) {
    r.append(getSuffixOperator());
  }

  @Override
  public void visitMatchingTerms(
      LeafReader reader, String fieldName, Analyzer analyzer, MatchingTermVisitor mtv)
      throws IOException {
    /* inspired by PrefixQuery.rewrite(): */
    if (compiled == null) {
      BytesRef prefixRef = toAnalyzedTerm(prefix, fieldName, analyzer);
      // TODO: we may have to escape/unescape around `toAnalyzedTerm()`?
      PrefixQuery q = new PrefixQuery(new Term(fieldName, prefixRef));
      compiled =
          new CompiledAutomaton(
              q.getAutomaton(),
              null,
              true,
              Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
              q.isAutomatonBinary());
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
