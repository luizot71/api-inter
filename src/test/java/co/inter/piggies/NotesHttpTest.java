package co.inter.piggies;

import co.inter.piggies.notes.*;
import io.micronaut.http.*;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.http.exceptions.HttpStatusException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

@MicronautTest(transactional = false)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NotesHttpTest implements TestPropertyProvider {
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:4.3.1");
    static final String TOPIC = "notes-" + UUID.randomUUID();
    @Override public Map<String, String> getProperties() {
        POSTGRES.start();
        KAFKA.start();
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
        } catch (Exception e) { throw new IllegalStateException(e); }
        return Map.of("datasources.default.url", POSTGRES.getJdbcUrl(),
            "datasources.default.username", POSTGRES.getUsername(),
            "datasources.default.password", POSTGRES.getPassword(),
            "kafka.bootstrap.servers", KAFKA.getBootstrapServers(),
            "app.notes-topic", TOPIC, "app.notes-group", "group-" + UUID.randomUUID(),
            "app.consumer-enabled", "true");
    }
    @Inject @Client("/") HttpClient client;
    @Inject SessionFactory factory;
    @Inject NoteStore store;
    @Inject NoteService service;
    @Inject NoteConsumer consumer;

    private HttpResponse<NoteResponse> post(CreateNoteRequest request) {
        return client.toBlocking().exchange(HttpRequest.POST("/notes", request), NoteResponse.class);
    }

    @Test void shouldCreateReadAndConsumeThroughMicronaut() {
        var request = new CreateNoteRequest(UUID.randomUUID(), "Estudar Micronaut");
        var response = post(request);
        assertThat(response.code()).isEqualTo(201);
        assertThat(response.getHeaders().get("Location")).isEqualTo("/notes/" + request.id());
        var loaded = client.toBlocking().retrieve("/notes/" + request.id(), NoteResponse.class);
        assertThat(loaded).isEqualTo(new NoteResponse(request.id(), request.content()));
        awaitReceipt(request);
    }

    @Test void identicalReplayReturnsExistingNoteAndDoesNotDuplicateRows() {
        var request = new CreateNoteRequest(UUID.randomUUID(), "Idempotencia");
        assertThat(post(request).code()).isEqualTo(201);
        assertThat(post(request).code()).isEqualTo(200);
        assertThat(count("preparation_note", request.id())).isEqualTo(1);
        awaitReceipt(request);
        // Exercita deterministicamente o mesmo handler duas vezes, alem do fluxo real acima.
        consumer.receive(request.id().toString(), request.content());
        consumer.receive(request.id().toString(), request.content());
        assertThat(count("note_receipt", request.id())).isEqualTo(1);
    }

    @Test void sameIdWithDifferentContentReturnsConflict() {
        var id = UUID.randomUUID();
        post(new CreateNoteRequest(id, "Original"));
        assertStatus(() -> post(new CreateNoteRequest(id, "Alterado")), HttpStatus.CONFLICT);
        assertThat(store.find(id).content()).isEqualTo("Original");
    }

    @ParameterizedTest @ValueSource(strings = {"", " ", "   "})
    void blankContentReturnsBadRequest(String content) {
        assertStatus(() -> post(new CreateNoteRequest(UUID.randomUUID(), content)), HttpStatus.BAD_REQUEST);
    }
    @Test void longContentReturnsBadRequest() {
        assertStatus(() -> post(new CreateNoteRequest(UUID.randomUUID(), "x".repeat(256))), HttpStatus.BAD_REQUEST);
    }
    @Test void missingIdReturnsBadRequest() {
        assertStatus(() -> post(new CreateNoteRequest(null, "Valido")), HttpStatus.BAD_REQUEST);
    }
    @Test void unknownIdReturnsNotFound() {
        assertStatus(() -> client.toBlocking().retrieve("/notes/" + UUID.randomUUID()), HttpStatus.NOT_FOUND);
    }
    @Test void concurrentRequestsCreateOnlyOneNote() throws Exception {
        var request = new CreateNoteRequest(UUID.randomUUID(), "Concorrente");
        var gate = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(2)) {
            Callable<Integer> action = () -> { gate.await(); return post(request).code(); };
            var first = executor.submit(action);
            var second = executor.submit(action);
            gate.countDown();
            assertThat(List.of(first.get(30, TimeUnit.SECONDS), second.get(30, TimeUnit.SECONDS)))
                .containsExactlyInAnyOrder(201, 200);
        }
        assertThat(count("preparation_note", request.id())).isEqualTo(1);
    }

    @Test void publicationFailureKeepsCommitAndIdenticalRetryRecovers() {
        var request = new CreateNoteRequest(UUID.randomUUID(), "Falha apos commit");
        NoteProducer failed = (id, content) -> CompletableFuture.failedFuture(new IllegalStateException("broker indisponivel"));
        // Injecao de falha deterministica na fronteira; banco continua real.
        var failingService = new NoteService(store, failed);
        assertThatThrownBy(() -> failingService.create(request)).isInstanceOfSatisfying(HttpStatusException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(503));
        assertThat(store.find(request.id()).content()).isEqualTo(request.content());
        assertThat(count("note_receipt", request.id())).isZero();
        assertThat(post(request).code()).isEqualTo(200);
        awaitReceipt(request);
    }

    private void awaitReceipt(CreateNoteRequest request) {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            try (var session = factory.openSession()) {
                var receipt = session.find(NoteReceipt.class, request.id());
                assertThat(receipt).isNotNull();
                assertThat(receipt.getContent()).isEqualTo(request.content());
            }
        });
    }
    private long count(String table, UUID id) {
        // table e fornecido apenas pelos literais internos deste teste.
        try (var session = factory.openSession()) {
            return session.createNativeQuery("SELECT count(*) FROM " + table + " WHERE id = :id", Long.class)
                .setParameter("id", id).getSingleResult();
        }
    }
    private void assertStatus(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, HttpStatus status) {
        assertThatThrownBy(action).isInstanceOfSatisfying(HttpClientResponseException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(status.getCode()));
    }
}
