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
package org.apache.lucene.facet;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.facet.taxonomy.IntAssociationFacetField;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.util.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestDrillDownQuery extends FacetTestCase {

  private static IndexReader reader;
  private static DirectoryTaxonomyReader taxo;
  private static Directory dir;
  private static Directory taxoDir;
  private static FacetsConfig config;

  @AfterClass
  public static void afterClassDrillDownQueryTest() throws Exception {
    IOUtils.close(reader, taxo, dir, taxoDir);
    reader = null;
    taxo = null;
    dir = null;
    taxoDir = null;
    config = null;
  }

  @BeforeClass
  public static void beforeClassDrillDownQueryTest() throws Exception {
    dir = newDirectory();
    Random r = random();
    RandomIndexWriter writer =
        new RandomIndexWriter(
            r, dir, newIndexWriterConfig(new MockAnalyzer(r, MockTokenizer.KEYWORD, false)));

    taxoDir = newDirectory();
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    config = new FacetsConfig();

    // Randomize the per-dim config:
    config.setHierarchical("a", random().nextBoolean());
    config.setMultiValued("a", random().nextBoolean());
    if (random().nextBoolean()) {
      config.setIndexFieldName("a", "$a");
    }
    config.setRequireDimCount("a", true);

    config.setHierarchical("b", random().nextBoolean());
    config.setMultiValued("b", random().nextBoolean());
    if (random().nextBoolean()) {
      config.setIndexFieldName("b", "$b");
    }
    config.setRequireDimCount("b", true);

    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      if (i % 2 == 0) { // 50
        doc.add(new TextField("content", "foo", Field.Store.NO));
      }
      if (i % 3 == 0) { // 33
        doc.add(new TextField("content", "bar", Field.Store.NO));
      }
      if (i % 4 == 0) { // 25
        if (r.nextBoolean()) {
          doc.add(new FacetField("a", "1"));
        } else {
          doc.add(new FacetField("a", "2"));
        }
      }
      if (i % 5 == 0) { // 20
        doc.add(new FacetField("b", "1"));
      }
      writer.addDocument(config.build(taxoWriter, doc));
    }

    taxoWriter.close();
    reader = writer.getReader();
    writer.close();

    taxo = new DirectoryTaxonomyReader(taxoDir);
  }

  public void testAndOrs() throws Exception {
    IndexSearcher searcher = newSearcher(reader);

    // test (a/1 OR a/2) AND b/1
    DrillDownQuery q = new DrillDownQuery(config);
    q.add("a", "1");
    q.add("a", "2");
    q.add("b", "1");
    TopDocs docs = searcher.search(q, 100);
    assertEquals(5, docs.totalHits.value);
  }

  public void testQuery() throws IOException {
    IndexSearcher searcher = newSearcher(reader);

    // Making sure the query yields 25 documents with the facet "a"
    DrillDownQuery q = new DrillDownQuery(config);
    q.add("a");
    QueryUtils.check(q);
    TopDocs docs = searcher.search(q, 100);
    assertEquals(25, docs.totalHits.value);

    // Making sure the query yields 5 documents with the facet "b" and the
    // previous (facet "a") query as a base query
    DrillDownQuery q2 = new DrillDownQuery(config, q);
    q2.add("b");
    docs = searcher.search(q2, 100);
    assertEquals(5, docs.totalHits.value);

    // Making sure that a query of both facet "a" and facet "b" yields 5 results
    DrillDownQuery q3 = new DrillDownQuery(config);
    q3.add("a");
    q3.add("b");
    docs = searcher.search(q3, 100);

    assertEquals(5, docs.totalHits.value);
    // Check that content:foo (which yields 50% results) and facet/b (which yields 20%)
    // would gather together 10 results (10%..)
    Query fooQuery = new TermQuery(new Term("content", "foo"));
    DrillDownQuery q4 = new DrillDownQuery(config, fooQuery);
    q4.add("b");
    docs = searcher.search(q4, 100);
    assertEquals(10, docs.totalHits.value);
  }

  public void testQueryImplicitDefaultParams() throws IOException {
    IndexSearcher searcher = newSearcher(reader);

    // Create the base query to start with
    DrillDownQuery q = new DrillDownQuery(config);
    q.add("a");

    // Making sure the query yields 5 documents with the facet "b" and the
    // previous (facet "a") query as a base query
    DrillDownQuery q2 = new DrillDownQuery(config, q);
    q2.add("b");
    TopDocs docs = searcher.search(q2, 100);
    assertEquals(5, docs.totalHits.value);

    // Check that content:foo (which yields 50% results) and facet/b (which yields 20%)
    // would gather together 10 results (10%..)
    Query fooQuery = new TermQuery(new Term("content", "foo"));
    DrillDownQuery q4 = new DrillDownQuery(config, fooQuery);
    q4.add("b");
    docs = searcher.search(q4, 100);
    assertEquals(10, docs.totalHits.value);
  }

  public void testZeroLimit() throws IOException {
    IndexSearcher searcher = newSearcher(reader);
    DrillDownQuery q = new DrillDownQuery(config);
    q.add("b", "1");
    int limit = 0;
    FacetsCollector facetCollector = new FacetsCollector();
    FacetsCollector.search(searcher, q, limit, facetCollector);
    Facets facets =
        getTaxonomyFacetCounts(
            taxo, config, facetCollector, config.getDimConfig("b").indexFieldName);
    assertNotNull(facets.getTopChildren(10, "b"));
  }

  public void testScoring() throws IOException {
    // verify that drill-down queries do not modify scores
    IndexSearcher searcher = newSearcher(reader);

    float[] scores = new float[reader.maxDoc()];

    Query q = new TermQuery(new Term("content", "foo"));
    TopDocs docs = searcher.search(q, reader.maxDoc()); // fetch all available docs to this query
    for (ScoreDoc sd : docs.scoreDocs) {
      scores[sd.doc] = sd.score;
    }

    // create a drill-down query with category "a", scores should not change
    DrillDownQuery q2 = new DrillDownQuery(config, q);
    q2.add("a");
    docs = searcher.search(q2, reader.maxDoc()); // fetch all available docs to this query
    for (ScoreDoc sd : docs.scoreDocs) {
      assertEquals("score of doc=" + sd.doc + " modified", scores[sd.doc], sd.score, 0f);
    }
  }

  public void testScoringNoBaseQuery() throws IOException {
    // verify that drill-down queries (with no base query) returns 0.0 score
    IndexSearcher searcher = newSearcher(reader);

    DrillDownQuery q = new DrillDownQuery(config);
    q.add("a");
    TopDocs docs = searcher.search(q, reader.maxDoc()); // fetch all available docs to this query
    for (ScoreDoc sd : docs.scoreDocs) {
      assertEquals(0f, sd.score, 0f);
    }
  }

  public void testTermNonDefault() {
    String aField = config.getDimConfig("a").indexFieldName;
    Term termA = DrillDownQuery.term(aField, "a");
    assertEquals(new Term(aField, "a"), termA);

    String bField = config.getDimConfig("b").indexFieldName;
    Term termB = DrillDownQuery.term(bField, "b");
    assertEquals(new Term(bField, "b"), termB);
  }

  public void testClone() throws Exception {
    DrillDownQuery q = new DrillDownQuery(config, new MatchAllDocsQuery());
    q.add("a");

    DrillDownQuery clone = q.clone();
    clone.add("b");

    assertFalse(
        "query wasn't cloned: source=" + q + " clone=" + clone,
        q.toString().equals(clone.toString()));
  }

  public void testNoDrillDown() throws Exception {
    Query base = new MatchAllDocsQuery();
    DrillDownQuery q = new DrillDownQuery(config, base);
    IndexSearcher searcher = newSearcher(reader);
    Query rewrite = q.rewrite(searcher).rewrite(searcher);
    assertEquals(base, rewrite);
  }

  public void testSkipDrillDownTermsIndexing() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer =
        new RandomIndexWriter(
            random(),
            dir,
            newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.KEYWORD, false)));
    Directory taxoDir = newDirectory();
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    FacetsConfig config = new FacetsConfig();
    config.setDrillDownTermsIndexing("a", FacetsConfig.DrillDownTermsIndexing.FULL_PATH_ONLY);
    config.setDrillDownTermsIndexing("b", FacetsConfig.DrillDownTermsIndexing.FULL_PATH_ONLY);

    Document doc = new Document();
    doc.add(new FacetField("a", "1"));
    doc.add(new FacetField("b", "2"));
    writer.addDocument(config.build(taxoWriter, doc));
    taxoWriter.close();

    IndexReader reader = writer.getReader();
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    IndexSearcher searcher = newSearcher(reader);

    DrillDownQuery q = new DrillDownQuery(config);
    q.add("a");
    // no hits because we disabled dimension drill down completely
    assertEquals(0, searcher.count(q));

    q = new DrillDownQuery(config);
    q.add("a", "1");
    assertEquals(1, searcher.count(q));

    q = new DrillDownQuery(config);
    q.add("b");
    // no hits because we disabled dimension drill down completely
    assertEquals(0, searcher.count(q));

    q = new DrillDownQuery(config);
    q.add("b", "2");
    assertEquals(1, searcher.count(q));

    IOUtils.close(taxoReader, reader, writer, dir, taxoDir);
  }

  public void testDrillDownTermsDifferentOptions() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer =
        new RandomIndexWriter(
            random(),
            dir,
            newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.KEYWORD, false)));
    Directory taxoDir = newDirectory();
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    FacetsConfig config = new FacetsConfig();
    config.setHierarchical("a", true);
    config.setHierarchical("b", true);
    config.setHierarchical("c", true);
    config.setHierarchical("d", true);
    config.setHierarchical("e", true);
    config.setDrillDownTermsIndexing("a", FacetsConfig.DrillDownTermsIndexing.NONE);
    config.setDrillDownTermsIndexing("b", FacetsConfig.DrillDownTermsIndexing.FULL_PATH_ONLY);
    config.setDrillDownTermsIndexing("c", FacetsConfig.DrillDownTermsIndexing.ALL_PATHS_NO_DIM);
    config.setDrillDownTermsIndexing(
        "d", FacetsConfig.DrillDownTermsIndexing.DIMENSION_AND_FULL_PATH);
    config.setDrillDownTermsIndexing("e", FacetsConfig.DrillDownTermsIndexing.ALL);
    config.setDrillDownTermsIndexing("f", FacetsConfig.DrillDownTermsIndexing.NONE);
    config.setIndexFieldName("f", "facet-for-f");

    Document doc = new Document();
    doc.add(new FacetField("a", "a1", "a2", "a3"));
    doc.add(new FacetField("b", "b1", "b2", "b3"));
    doc.add(new FacetField("c", "c1", "c2", "c3"));
    doc.add(new FacetField("d", "d1", "d2", "d3"));
    doc.add(new FacetField("e", "e1", "e2", "e3"));
    doc.add(new IntAssociationFacetField(5, "f", "f1"));
    writer.addDocument(config.build(taxoWriter, doc));
    taxoWriter.close();

    IndexReader reader = writer.getReader();
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    IndexSearcher searcher = newSearcher(reader);

    // Verifies for FacetsConfig.DrillDownTermsIndexing.NONE option
    DrillDownQuery q = new DrillDownQuery(config);
    q.add("a");
    assertEquals(0, searcher.count(q));

    q = new DrillDownQuery(config);
    q.add("a", "a1");
    assertEquals(0, searcher.count(q));

    q = new DrillDownQuery(config);
    q.add("a", "a1", "a2");
    assertEquals(0, searcher.count(q));

    q = new DrillDownQuery(config);
    q.add("a", "a1", "a2", "a3");
    assertEquals(0, searcher.count(q));

    // Verifies for FacetsConfig.DrillDownTermsIndexing.FULL_PATH_ONLY option
    q = new DrillDownQuery(config);
    q.add("b");
    assertEquals(0, searcher.count(q));

    q = new DrillDownQuery(config);
    q.add("b", "b1");
    assertEquals(0, searcher.count(q));

    q = new DrillDownQuery(config);
    q.add("b", "b1", "b2");
    assertEquals(0, searcher.count(q));

    q = new DrillDownQuery(config);
    q.add("b", "b1", "b2", "b3");
    assertEquals(1, searcher.count(q));

    // Verifies for FacetsConfig.DrillDownTermsIndexing.ALL_PATHS_NO_DIM option
    q = new DrillDownQuery(config);
    q.add("c");
    assertEquals(0, searcher.count(q));

    q = new DrillDownQuery(config);
    q.add("c", "c1");
    assertEquals(1, searcher.count(q));

    q = new DrillDownQuery(config);
    q.add("c", "c1", "c2");
    assertEquals(1, searcher.count(q));

    q = new DrillDownQuery(config);
    q.add("c", "c1", "c2", "c3");
    assertEquals(1, searcher.count(q));

    // Verifies for FacetsConfig.DrillDownTermsIndexing.DIMENSION_AND_FULL_PATH option
    q = new DrillDownQuery(config);
    q.add("d");
    assertEquals(1, searcher.count(q));

    q = new DrillDownQuery(config);
    q.add("d", "d1");
    assertEquals(0, searcher.count(q));

    q = new DrillDownQuery(config);
    q.add("d", "d1", "d2");
    assertEquals(0, searcher.count(q));

    q = new DrillDownQuery(config);
    q.add("d", "d1", "d2", "d3");
    assertEquals(1, searcher.count(q));

    // Verifies for FacetsConfig.DrillDownTermsIndexing.ALL option
    q = new DrillDownQuery(config);
    q.add("e");
    assertEquals(1, searcher.count(q));

    q = new DrillDownQuery(config);
    q.add("e", "e1");
    assertEquals(1, searcher.count(q));

    q = new DrillDownQuery(config);
    q.add("e", "e1", "e2");
    assertEquals(1, searcher.count(q));

    q = new DrillDownQuery(config);
    q.add("e", "e1", "e2", "e3");
    assertEquals(1, searcher.count(q));

    // Verifies for FacetsConfig.DrillDownTermsIndexing.DIMENSION_AND_FULL_PATH option with
    // IntAssociationFacetField
    q = new DrillDownQuery(config);
    q.add("f");
    assertEquals(0, searcher.count(q));

    q = new DrillDownQuery(config);
    q.add("f", "f1");
    assertEquals(0, searcher.count(q));

    IOUtils.close(taxoReader, reader, writer, dir, taxoDir);
  }

  public void testDrillDownTermsDefaultWithHierarchicalSetting() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer =
        new RandomIndexWriter(
            random(),
            dir,
            newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.KEYWORD, false)));
    Directory taxoDir = newDirectory();
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    FacetsConfig config = new FacetsConfig();
    config.setHierarchical("a", true);

    Document doc = new Document();
    doc.add(new FacetField("a", "1", "2", "3"));
    doc.add(new FacetField("b", "4"));
    writer.addDocument(config.build(taxoWriter, doc));
    taxoWriter.close();

    IndexReader reader = writer.getReader();
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    IndexSearcher searcher = newSearcher(reader);

    DrillDownQuery q = new DrillDownQuery(config);
    q.add("a");
    assertEquals(1, searcher.count(q));

    q = new DrillDownQuery(config);
    q.add("a", "1");
    assertEquals(1, searcher.count(q));

    q = new DrillDownQuery(config);
    q.add("a", "1", "2");
    assertEquals(1, searcher.count(q));

    q = new DrillDownQuery(config);
    q.add("a", "1", "2", "3");
    assertEquals(1, searcher.count(q));

    q = new DrillDownQuery(config);
    q.add("b");
    assertEquals(1, searcher.count(q));

    q = new DrillDownQuery(config);
    q.add("b", "4");
    assertEquals(1, searcher.count(q));

    IOUtils.close(taxoReader, reader, writer, dir, taxoDir);
  }

  public void testGetDrillDownQueries() throws Exception {
    DrillDownQuery q = new DrillDownQuery(config);
    q.add("a", "1");
    q.add("b", "1");

    Query[] drillDownQueries = q.getDrillDownQueries();
    Query[] drillDownQueriesCopy = q.getDrillDownQueries();

    assert Arrays.equals(drillDownQueries, drillDownQueriesCopy);

    q.add("c", "1");
    q.add("a", "2");
    q.add("a", "3");
    Query[] drillDownQueriesModified = q.getDrillDownQueries();
    Query[] drillDownQueriesModifiedCopy = q.getDrillDownQueries();

    // the cached builtDimQueries object is now stale
    assert Arrays.equals(drillDownQueriesModified, drillDownQueriesCopy) == false;
    assert Arrays.equals(drillDownQueriesModified, drillDownQueriesModifiedCopy);
  }
}
