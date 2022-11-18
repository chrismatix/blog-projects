package cz.chrismati.blogprojects.lucene.store;

import cz.chrismati.blogprojects.lucene.User;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

public class LuceneStore implements StateStore, ReadOnlyLuceneStore {

    final String name;
    String path;
    IndexWriter writer;
    boolean loggingEnabled;
    ProcessorContext initialProcessorContext;
    StoreChangeLogger<String, User> changeLogger;

    public LuceneStore(String name) {
        this(name, true);
    }

    public LuceneStore(String name, boolean loggingEnabled) {
        this.name = name;
        this.loggingEnabled = loggingEnabled;
    }

    private IndexWriter getWriter() {
        return writer;
    }

    public List<IndexReader> reader() {
        try {
            return List.of(DirectoryReader.open(writer, true, true));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Document> search(Query query) {
        return null;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(ProcessorContext context, StateStore stateStore) {
        final String topic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), name());

        try {
            this.path = "/tmp/lucene"; // context.stateDir() + "/" + name();
            FSDirectory dir = FSDirectory.open(Paths.get(path));

            IndexWriterConfig config = new IndexWriterConfig();

            writer = new IndexWriter(dir, config);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        changeLogger = new StoreChangeLogger<>(
                name(),
                context,
                new StateSerdes<>(topic, Serdes.String(), (Serde<User>) context.valueSerde()));

        initialProcessorContext = context;

        context.register(stateStore, (keyBytes, value) -> {
            // Restoration callback
            try {
                final String key = ((Serde<String>) context.keySerde()).deserializer().deserialize(topic, keyBytes);
                final User user = (User) context.valueSerde().deserializer().deserialize(topic, value);
                put(key, user);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }


    void log(final String key,
             final User value) {
        changeLogger.logChange(key, value);
    }

    @Override
    public void flush() {
        try {
            getWriter().flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        try {
            getWriter().close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean persistent() {
        return true;
    }

    @Override
    public boolean isOpen() {
        return getWriter().isOpen();
    }

    public void put(String key, User user) throws IOException {
        writer.updateDocument(new Term("id", key), DocumentMapper.from(key, user));
        log(key, user);
    }

    public void delete(String id) throws IOException {
        writer.deleteDocuments(new Term("id", id));
        log(id, null);
    }

}
