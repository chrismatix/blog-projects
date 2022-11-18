package cz.chrismati.blogprojects.lucene.store;

import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Map;

public class LuceneStoreBuilder implements StoreBuilder<LuceneStore> {
    final String name;

    public LuceneStoreBuilder(String name) {
        this.name = name;
    }

    @Override
    public StoreBuilder<LuceneStore> withCachingEnabled() {
        return this;
    }

    @Override
    public StoreBuilder<LuceneStore> withCachingDisabled() {
        return this;
    }

    @Override
    public StoreBuilder<LuceneStore> withLoggingEnabled(Map<String, String> config) {
        return this;
    }

    @Override
    public StoreBuilder<LuceneStore> withLoggingDisabled() {
        return this;
    }

    @Override
    public LuceneStore build() {
        return new LuceneStore(name);
    }

    @Override
    public Map<String, String> logConfig() {
        return Map.of();
    }

    @Override
    public boolean loggingEnabled() {
        return true;
    }

    @Override
    public String name() {
        return name;
    }
}
