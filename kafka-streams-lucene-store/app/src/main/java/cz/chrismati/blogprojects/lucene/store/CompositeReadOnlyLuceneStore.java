package cz.chrismati.blogprojects.lucene.store;

import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;

import java.io.IOException;

public class CompositeReadOnlyLuceneStore implements ReadOnlyLuceneStore {
    private final StateStoreProvider storeProvider;
    private final QueryableStoreType<ReadOnlyLuceneStore> storeType;
    private final String storeName;

    public CompositeReadOnlyLuceneStore(final StateStoreProvider storeProvider,
                                        final QueryableStoreType<ReadOnlyLuceneStore> storeType,
                                        final String storeName) {
        this.storeProvider = storeProvider;
        this.storeType = storeType;
        this.storeName = storeName;
    }


    @Override
    public IndexReader reader() {
        final IndexReader[] readers = storeProvider.stores(storeName, storeType)
                .stream().map(ReadOnlyLuceneStore::reader).toArray(IndexReader[]::new);

        try {
            return new MultiReader(readers);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Analyzer analyzer() {
        return storeProvider.stores(storeName, storeType).get(0).analyzer();
    }
}
