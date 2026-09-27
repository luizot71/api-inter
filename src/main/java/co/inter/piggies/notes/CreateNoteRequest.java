package co.inter.piggies.notes;

import io.micronaut.serde.annotation.Serdeable;
import jakarta.validation.constraints.*;
import java.util.UUID;

@Serdeable
public record CreateNoteRequest(@NotNull UUID id,
    @NotBlank @Size(max = 255) String content) { }
