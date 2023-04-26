import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AccessKtableStateStoreUsingPrefix {

    public static final String STORE_NAME = "bigstore";
    public static final String GLOBAL_KTABLE_TOPIC = "ktable";
    public static final String STREAMS_TOPIC = "other";

    public static void main(String[] args) {
        try {
            String bootstrapServers = "localhost:9092";
            createTopics(bootstrapServers, GLOBAL_KTABLE_TOPIC, STREAMS_TOPIC);
            produceDemoMessages(bootstrapServers);

            StreamsBuilder builder = new StreamsBuilder();

            GlobalKTable<String, String> ktable = builder.globalTable(GLOBAL_KTABLE_TOPIC,
                    Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(STORE_NAME)
                            .withKeySerde(Serdes.String())
                            .withValueSerde(Serdes.String()));

            KStream<String, String> stream = builder.stream(STREAMS_TOPIC);
            stream.join(ktable, (key, value) -> key, (readOnlyKey, value1, value2) -> value1 + "#" + value2)
                    .foreach((key, value) -> System.out.println("Hello, " + key + " " + value + "!"));

            KafkaStreams streams = startKafkaStreams(bootstrapServers, builder);
            startHttpServer(streams);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void produceDemoMessages(String bootstrapServers) {
        try {
            Properties properties = new Properties();
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            try (KafkaProducer<Object, Object> producer = new KafkaProducer<>(properties)) {
                send(producer, GLOBAL_KTABLE_TOPIC, "a1", "hello");
                send(producer, GLOBAL_KTABLE_TOPIC, "a2", "there");
                send(producer, GLOBAL_KTABLE_TOPIC, "b1", "world");
                send(producer, GLOBAL_KTABLE_TOPIC, "b", "magic");
                send(producer, STREAMS_TOPIC, "a1", "arbitrary1");
                send(producer, STREAMS_TOPIC, "a2", "arbitrary2");
                send(producer, STREAMS_TOPIC, "a2", "arbitrary2");
                send(producer, STREAMS_TOPIC, "b1", "world");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void send(KafkaProducer<Object, Object> producer, String topic, String key, String value) throws InterruptedException, ExecutionException, TimeoutException {
        producer.send(new ProducerRecord<>(topic, key, value)).get(60, TimeUnit.SECONDS);
    }

    private static KafkaStreams startKafkaStreams(String bootstrapServers, StreamsBuilder builder) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5000);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        return streams;
    }

    private static void startHttpServer(KafkaStreams streams) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(8000), 0);
        // the path /test/a returns the values with prefix "a" concatenated
        server.createContext("/test", new MyHandler(streams));
        server.setExecutor(null); // creates a default executor
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> server.stop(0)));
    }

    // just here for demonstration purposes
    private static void createTopics(String bootstrapServers, String... topicNames) {
        Properties adminConfig = new Properties();
        adminConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (Admin admin = Admin.create(adminConfig)) {
            for (String topicName : topicNames) {
                createTable(admin, topicName);
            }
        }
    }

    private static void createTable(Admin admin, String name) {
        NewTopic topic = new NewTopic(name, Optional.empty(), Optional.empty());
        try {
            admin.createTopics(List.of(topic)).all().get(20, TimeUnit.SECONDS);
        } catch (Exception e) {
            if (e.getCause() instanceof TopicExistsException) {
                System.out.println("topic " + name + " exists already, continuing");
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    static class MyHandler implements HttpHandler {
        private final KafkaStreams streams;

        public MyHandler(KafkaStreams streams) {
            this.streams = streams;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                String path = exchange.getRequestURI().getPath();
                String prefix = path.substring(path.lastIndexOf("/") + 1);
                String response = concatenatedValuesFor(prefix);
                exchange.sendResponseHeaders(200, response.length());
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes());
                os.close();
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        }

        private String concatenatedValuesFor(String prefix) {
            ReadOnlyKeyValueStore<Object, Object> store = streams.store(StoreQueryParameters.fromNameAndType(STORE_NAME, QueryableStoreTypes.keyValueStore()));
            KeyValueIterator<Object, Object> iterator = store.prefixScan(prefix, new StringSerializer());
            StringBuilder stringBuilder = new StringBuilder();
            iterator.forEachRemaining(objectObjectKeyValue -> stringBuilder.append(objectObjectKeyValue.value.toString()));
            return stringBuilder.toString();
        }
    }
}
