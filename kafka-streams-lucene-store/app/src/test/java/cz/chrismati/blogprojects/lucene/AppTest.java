package cz.chrismati.blogprojects.lucene;

import cz.chrismati.blogprojects.lucene.serde.JsonSerde;
import cz.chrismati.blogprojects.lucene.store.LuceneProcessor;
import cz.chrismati.blogprojects.lucene.store.LuceneStoreBuilder;
import cz.chrismati.blogprojects.lucene.store.LuceneStoreType;
import cz.chrismati.blogprojects.lucene.store.ReadOnlyLuceneStore;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.List.of;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class AppTest {

    @Container
    RedpandaContainer kafka =
            new RedpandaContainer("docker.redpanda.com/vectorized/redpanda:v22.2.1");

    final String inputTopic = "user-updates";

    private final List<User> users =
            of(
                    new User("grace", "grace.hopper@example.com"),
                    new User("ada", "ada.lovelace@example.com"),
                    new User("barbara", "barbara.liskov@example.org"),
                    new User("margaret", "margaret.hamilton@example.org"));


    @Test
    void canQueryLucene() throws Exception {
        createTopic();

        final StreamsBuilder builder = new StreamsBuilder();

        builder.addStateStore(new LuceneStoreBuilder("lucene"));

        KStream<String, User> source = builder.stream(inputTopic);

        source.process(LuceneProcessor.getSupplier("lucene"), "lucene");

        final KafkaStreams streams = getStreams(builder.build());

        streams.cleanUp();

        streams.start();

        final KafkaProducer<String, User> producer = getProducer();
        for (User user : users) {
            // We use the username as a key
            producer.send(new ProducerRecord<>(inputTopic, user.name(), user)).get();
        }

        TimeUnit.SECONDS.sleep(10);

        final ReadOnlyLuceneStore store = streams.store(StoreQueryParameters.fromNameAndType("lucene", new LuceneStoreType()));
        final IndexReader reader = store.reader();
        assertThat(reader.numDocs()).isEqualTo(4);
        final IndexSearcher searcher = new IndexSearcher(reader);

        // Returns all the data from the objects
        ScoreDoc[] scoreDocs = searcher.search(new MatchAllDocsQuery(), 10).scoreDocs;
        assertThat(scoreDocs).hasSize(4);
        for (ScoreDoc scoreDoc : scoreDocs) {
            final Document doc = searcher.doc(scoreDoc.doc);
            assertThat(
                    users.stream()
                            .anyMatch(
                                    user ->
                                            user.email()
                                                    .equals(
                                                            doc.getField("email")
                                                                    .stringValue())))
                    .isTrue();
        }

        // Can query on the email field
        final Query query = new QueryParser("email", store.analyzer()).parse("barbara* OR ada*");
        scoreDocs = searcher.search(query, 10).scoreDocs;

        assertThat(
                Arrays.stream(scoreDocs)
                        .anyMatch(
                                scoreDoc -> {
                                    try {
                                        return searcher.doc(scoreDoc.doc)
                                                .getField("email")
                                                .stringValue()
                                                .equals("barbara.liskov@example.org");
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                }))
                .isTrue();

        assertThat(
                Arrays.stream(scoreDocs)
                        .anyMatch(
                                scoreDoc -> {
                                    try {
                                        return searcher.doc(scoreDoc.doc)
                                                .getField("email")
                                                .stringValue()
                                                .equals("ada.lovelace@example.com");
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                }))
                .isTrue();
    }

    private void createTopic() {
        // Create topic
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            adminClient.createTopics(List.of(new NewTopic(inputTopic, 3, (short) 1))).all().get(30, TimeUnit.SECONDS);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    KafkaProducer<String, User> getProducer() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerde.class);
        return new KafkaProducer<>(props);
    }

    KafkaStreams getStreams(Topology topology) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);

        return new KafkaStreams(topology, props);
    }
}
