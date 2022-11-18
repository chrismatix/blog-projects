package cz.chrismati.blogprojects.lucene.store;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;

import java.util.List;

public interface ReadOnlyLuceneStore {
    List<IndexReader> reader();
    List<Document> search(Query query);
}
