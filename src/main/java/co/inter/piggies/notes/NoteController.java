package co.inter.piggies.notes;
import io.micronaut.http.*;
import io.micronaut.http.annotation.*;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.UUID;

@Controller("/notes")
@ExecuteOn(TaskExecutors.BLOCKING)
public class NoteController {
    private final NoteService service;
    private final NoteStore store;
    public NoteController(NoteService service, NoteStore store) { this.service = service; this.store = store; }
    @Post
    public HttpResponse<NoteResponse> create(@Body @Valid CreateNoteRequest request) {
        var result = service.create(request);
        return result.created() ? HttpResponse.created(result.note(), URI.create("/notes/" + result.note().id()))
            : HttpResponse.ok(result.note());
    }
    @Get("/{id}")
    public NoteResponse get(UUID id) { return store.find(id); }
}
