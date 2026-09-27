package co.inter.piggies.notes;

import co.inter.piggies.PreparationNote;
import jakarta.inject.Singleton;
import org.hibernate.SessionFactory;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;
import java.util.UUID;

@Singleton
public class NoteStore {
    private final SessionFactory factory;
    public NoteStore(SessionFactory factory) { this.factory = factory; }
    public record SaveResult(NoteResponse note, boolean created) { }

    public SaveResult save(CreateNoteRequest request) {
        try (var session = factory.openSession()) {
            var tx = session.beginTransaction();
            try {
                // A PK protege inclusive requisicoes concorrentes. Nao e check-then-insert.
                int inserted = session.createNativeMutationQuery(
                    "INSERT INTO preparation_note (id, content) VALUES (:id, :content) ON CONFLICT (id) DO NOTHING")
                    .setParameter("id", request.id()).setParameter("content", request.content()).executeUpdate();
                var entity = session.find(PreparationNote.class, request.id());
                if (!entity.getContent().equals(request.content())) {
                    throw new HttpStatusException(HttpStatus.CONFLICT, "ID ja utilizado com outro conteudo");
                }
                tx.commit();
                return new SaveResult(new NoteResponse(entity.getId(), entity.getContent()), inserted == 1);
            } catch (RuntimeException e) {
                if (tx.isActive()) tx.rollback();
                throw e;
            }
        }
    }

    public NoteResponse find(UUID id) {
        try (var session = factory.openSession()) {
            var note = session.find(PreparationNote.class, id);
            if (note == null) throw new HttpStatusException(HttpStatus.NOT_FOUND, "Nota nao encontrada");
            return new NoteResponse(note.getId(), note.getContent());
        }
    }
}
