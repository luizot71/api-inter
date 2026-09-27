package co.inter.piggies.notes;
import io.micronaut.configuration.kafka.annotation.*;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.RecordMetadata;

@KafkaClient
public interface NoteProducer {
    @Topic("${app.notes-topic}")
    CompletableFuture<RecordMetadata> send(@KafkaKey String id, String content);
}
