package cz.chrismati.blogprojects.lucene.store;

import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;

import java.util.List;

import static java.util.stream.Collectors.toList;

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
    public List<IndexReader> reader() {
        return storeProvider.stores(storeName, storeType).stream().flatMap(store -> store.reader().stream()).collect(toList());
    }

    @Override
    public List<Document> search(Query query) {
        final List<ReadOnlyLuceneStore> stores = storeProvider.stores(storeName, storeType);
/*
        final IndexSearcher searcher = new IndexSearcher(reader);
        SortField lastMessageSort = new SortedNumericSortField("last_message_at", SortField.Type.LONG, true);
        Sort sort = new Sort(lastMessageSort);
        final TopFieldCollector collector = TopFieldCollector.create(sort, 2000, Integer.MAX_VALUE);

        searcher.search(query, collector);
        final TopDocs hits = collector.topDocs(cursor, pageSize);
*/

        return null;
    }
}
