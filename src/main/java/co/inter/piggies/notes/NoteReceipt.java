package co.inter.piggies.notes;
import jakarta.persistence.*;
import java.util.UUID;
@Entity
@Table(name = "note_receipt")
public class NoteReceipt {
    @Id private UUID id;
    @Column(nullable = false, length = 255) private String content;
    protected NoteReceipt() { }
    public UUID getId() { return id; }
    public String getContent() { return content; }
}
