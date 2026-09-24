package co.inter.piggies;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
class KafkaIntegrationTest {
    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:4.3.1");

    @Test
    void shouldPublishAndConsumeMessage() throws Exception {
        String topic = "preparation-" + UUID.randomUUID();
        String key = UUID.randomUUID().toString();
        String payload = "Mensagem da preparacao Inter";
        String bootstrapServers = KAFKA.getBootstrapServers();

        try (var admin = AdminClient.create(Map.of("bootstrap.servers", bootstrapServers))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                .all().get(30, TimeUnit.SECONDS);
            try {
                try (var producer = new KafkaProducer<String, String>(Map.of(
                         "bootstrap.servers", bootstrapServers,
                         "acks", "all",
                         "max.block.ms", 10000,
                         "request.timeout.ms", 10000,
                         "delivery.timeout.ms", 15000), new StringSerializer(), new StringSerializer());
                     var consumer = new KafkaConsumer<String, String>(Map.of(
                         "bootstrap.servers", bootstrapServers,
                         "group.id", "test-" + UUID.randomUUID(),
                         "auto.offset.reset", "earliest",
                         "enable.auto.commit", false), new StringDeserializer(), new StringDeserializer())) {
                    consumer.subscribe(List.of(topic));
                    var metadata = producer.send(new ProducerRecord<>(topic, key, payload))
                        .get(20, TimeUnit.SECONDS);
                    assertThat(metadata.topic()).isEqualTo(topic);

                    List<ConsumerRecord<String, String>> received = new ArrayList<>();
                    // KafkaConsumer nao e thread-safe: o polling fica na thread do teste.
                    await().alias("receber a mensagem publicada no Kafka")
                        .pollInSameThread().atMost(Duration.ofSeconds(30))
                        .untilAsserted(() -> {
                            consumer.poll(Duration.ofMillis(500)).forEach(received::add);
                            assertThat(received).anySatisfy(record -> {
                                assertThat(record.topic()).isEqualTo(topic);
                                assertThat(record.key()).isEqualTo(key);
                                assertThat(record.value()).isEqualTo(payload);
                            });
                        });
                }
            } finally {
                admin.deleteTopics(List.of(topic)).all().get(30, TimeUnit.SECONDS);
            }
        }
    }
}
