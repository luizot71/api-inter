package co.inter.piggies;

import io.micronaut.runtime.EmbeddedApplication;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import jakarta.inject.Inject;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@MicronautTest(transactional = false)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ApiInterTest implements TestPropertyProvider {
    // Iniciado antes de do Micronaut criar o DataSource
    private static final PostgreSQLContainer POSTGRES =
        new PostgreSQLContainer("postgres:16-alpine");

    @Override
    public Map<String, String> getProperties() {
        POSTGRES.start();
        return Map.of(
            "datasources.default.url", POSTGRES.getJdbcUrl(),
            "datasources.default.username", POSTGRES.getUsername(),
            "datasources.default.password", POSTGRES.getPassword());
    }

    @Inject EmbeddedApplication<?> application;
    @Inject SessionFactory sessionFactory;

    @Test
    void testItWorks() {
        assertThat(application.isRunning()).isTrue();
    }

    @Test
    void shouldPersistAndReadNoteInANewSession() {
        UUID id = UUID.randomUUID();
        String content = "Persistencia real no PostgreSQL";

        // Commit antes de fechar a primeira sessao: flush sozinho nao basta
        try (var session = sessionFactory.openSession()) {
            var transaction = session.beginTransaction();
            try {
                session.persist(new PreparationNote(id, content));
                transaction.commit();
            } catch (RuntimeException failure) {
                if (transaction.isActive()) transaction.rollback();
                throw failure;
            }
        }

        // Nova sessao, sem o cache de primeiro nivel da sessao anterior
        try (var session = sessionFactory.openSession()) {
            var transaction = session.beginTransaction();
            try {
                PreparationNote loaded = session.find(PreparationNote.class, id);
                assertThat(loaded).isNotNull();
                assertThat(loaded.getId()).isEqualTo(id);
                assertThat(loaded.getContent()).isEqualTo(content);
                session.remove(loaded);
                transaction.commit();
            } catch (RuntimeException | AssertionError failure) {
                if (transaction.isActive()) transaction.rollback();
                throw failure;
            }
        }
    }
}
