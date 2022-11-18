package cz.chrismati.blogprojects.lucene.store;

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.internals.StateStoreProvider;

public class LuceneStoreType implements QueryableStoreType<ReadOnlyLuceneStore> {

    public boolean accepts(final StateStore stateStore) {
        return stateStore instanceof LuceneStore;
    }

    public ReadOnlyLuceneStore create(
            final StateStoreProvider storeProvider, final String storeName) {
        return storeProvider.stores(storeName, this).get(0);
    }
}
