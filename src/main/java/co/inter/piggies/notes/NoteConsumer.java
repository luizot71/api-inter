package co.inter.piggies.notes;
import io.micronaut.configuration.kafka.annotation.*;
import io.micronaut.context.annotation.Requires;
import org.hibernate.SessionFactory;
import java.util.UUID;

@Requires(property = "app.consumer-enabled", value = "true")
@KafkaListener(groupId = "${app.notes-group}", offsetReset = OffsetReset.EARLIEST,
    offsetStrategy = OffsetStrategy.SYNC_PER_RECORD,
    errorStrategy = @ErrorStrategy(value = ErrorStrategyValue.RETRY_ON_ERROR,
        retryCount = 3, retryDelay = "1s", stopOnExhaustedRetry = true))
public class NoteConsumer {
    private final SessionFactory factory;
    public NoteConsumer(SessionFactory factory) { this.factory = factory; }
    @Topic("${app.notes-topic}")
    public void receive(@KafkaKey String key, String content) {
        try (var session = factory.openSession()) {
            var tx = session.beginTransaction();
            try {
                // O efeito e a deduplicacao sao a mesma gravacao atomica no banco.
                session.createNativeMutationQuery(
                    "INSERT INTO note_receipt (id, content) VALUES (:id, :content) ON CONFLICT (id) DO NOTHING")
                    .setParameter("id", UUID.fromString(key)).setParameter("content", content).executeUpdate();
                tx.commit();
            } catch (RuntimeException e) {
                if (tx.isActive()) tx.rollback();
                throw e;
            }
        } // O listener so retorna depois do commit; entao o offset pode ser confirmado.
    }
}
