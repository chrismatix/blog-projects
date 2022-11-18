package cz.chrismati.blogprojects.lucene.store;

import cz.chrismati.blogprojects.lucene.User;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.io.IOException;

public class LuceneProcessor implements Processor<String, User> {

    private ProcessorContext context;
    private LuceneStore store;
    private final String storeName;

    public LuceneProcessor(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.store = context.getStateStore(this.storeName);
    }

    @Override
    public void process(String key, User value) {
        try {
            if (value == null) {
                store.delete(key);
            } else {
                store.put(key, value);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        context.forward(key, value);
    }

    @Override
    public void close() {
    }

    public static ProcessorSupplier<String, User> getSupplier(String luceneStoreName) {
        return () -> new LuceneProcessor(luceneStoreName);
    }
}
