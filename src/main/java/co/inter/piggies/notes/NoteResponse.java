package co.inter.piggies.notes;
import io.micronaut.serde.annotation.Serdeable;
import java.util.UUID;
@Serdeable
public record NoteResponse(UUID id, String content) { }
