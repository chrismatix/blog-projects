package cz.chrismati.blogprojects.lucene.store;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;

public interface ReadOnlyLuceneStore {
    IndexReader reader();

    Analyzer analyzer();
}
