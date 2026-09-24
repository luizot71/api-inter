package co.inter.piggies;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.util.UUID;

@Entity
@Table(name = "preparation_note")
public class PreparationNote {

    @Id
    private UUID id;

    @Column(nullable = false)
    private String content;

    protected PreparationNote() {
        // Construtor utilizado pelo JPA
    }

    public PreparationNote(UUID id, String content) {
        this.id = id;
        this.content = content;
    }

    public UUID getId() {
        return id;
    }

    public String getContent() {
        return content;
    }
}