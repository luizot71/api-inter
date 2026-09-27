package co.inter.piggies;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Getter
@Entity
@NoArgsConstructor
@Table(name = "preparation_note")
public class PreparationNote {

    @Id
    private UUID id;

    @Column(nullable = false)
    private String content;

    public PreparationNote(UUID id, String content) {
        this.id = id;
        this.content = content;
    }

}