package co.inter.piggies.notes;
import jakarta.inject.Singleton;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;
import java.util.concurrent.TimeUnit;

@Singleton
public class NoteService {
    private final NoteStore store;
    private final NoteProducer producer;
    public NoteService(NoteStore store, NoteProducer producer) {
        this.store = store; this.producer = producer;
    }
    public NoteStore.SaveResult create(CreateNoteRequest request) {
        var result = store.save(request); // Commit do PostgreSQL termina antes do envio.
        try {
            // Repeticoes identicas republicam: o consumidor precisa ser idempotente.
            producer.send(request.id().toString(), request.content()).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw unavailable();
        } catch (Exception e) {
            throw unavailable();
        }
        return result;
    }
    private HttpStatusException unavailable() {
        return new HttpStatusException(HttpStatus.SERVICE_UNAVAILABLE,
            "Nota salva; confirmacao Kafka indisponivel. Repita o POST com o mesmo ID e conteudo.");
    }
}
