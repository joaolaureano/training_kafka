package dev.joaolaureano.trainingkafka.audit.adapters.persistence.jsonl;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import dev.joaolaureano.trainingkafka.audit.domain.model.ApplicationName;
import dev.joaolaureano.trainingkafka.audit.domain.model.AuditEvent;
import dev.joaolaureano.trainingkafka.audit.domain.model.AuditFilter;
import dev.joaolaureano.trainingkafka.audit.domain.model.AuditLevel;
import dev.joaolaureano.trainingkafka.audit.domain.port.AuditRepository;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Implementação de {@link AuditRepository} que grava um registro por linha em um
 * arquivo {@code .jsonl} (JSON Lines): cada linha é um objeto JSON completo, sem
 * quebras de linha internas.
 *
 * <p>Usamos {@link ObjectMapper} (Jackson) em vez de montar o JSON manualmente:
 * escaping correto de strings arbitrárias (mensagens de log podem conter aspas,
 * barras invertidas, quebras de linha, unicode) é um problema já resolvido pela
 * biblioteca, e reinventá-lo só para evitar uma dependência que já está no
 * classpath (via spring-boot-starter-web) não compensa o risco de um bug sutil de
 * escaping corromper o arquivo.
 */
public class JsonlFileAuditRepository implements AuditRepository {

    private final Path file;
    private final ObjectMapper mapper;
    private final Object lock = new Object();

    public JsonlFileAuditRepository(Path file) {
        this.file = file;
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        try {
            Path parent = file.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("falha ao criar diretórios para o arquivo JSONL " + file, e);
        }
    }

    /**
     * Grava a linha JSON e devolve.
     *
     * <p>{@code synchronized}: várias threads (ex.: múltiplos listeners Kafka)
     * podem chamar {@code save} ao mesmo tempo, e escritas concorrentes num mesmo
     * arquivo sem coordenação podem intercalar bytes de linhas diferentes,
     * corrompendo o JSONL. O lock garante que cada linha é escrita inteira, de
     * uma vez, antes da próxima começar.
     */
    @Override
    public void save(AuditEvent entry) {
        synchronized (lock) {
            try {
                String line = mapper.writeValueAsString(toJson(entry));
                Files.writeString(
                        file,
                        line + System.lineSeparator(),
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND
                );
            } catch (IOException e) {
                throw new UncheckedIOException("falha ao gravar registro de log em " + file, e);
            }
        }
    }

    /**
     * Lê o arquivo inteiro, desserializa linha a linha, filtra e ordena.
     *
     * <p>Se o arquivo ainda não existe, devolve lista vazia (ainda não houve
     * nenhum {@code save}). Linhas corrompidas (JSON inválido, campo faltando)
     * são puladas — um registro ruim não deve derrubar a consulta inteira.
     */
    @Override
    public List<AuditEvent> query(AuditFilter filter, int limit) {
        synchronized (lock) {
            if (!Files.exists(file)) {
                return List.of();
            }
            List<AuditEvent> matched = new ArrayList<>();
            try {
                List<String> lines = Files.readAllLines(file);
                for (String line : lines) {
                    if (line.isBlank()) {
                        continue;
                    }
                    AuditEvent entry = tryParse(line);
                    if (entry != null && entry.matches(filter)) {
                        matched.add(entry);
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException("falha ao ler registros de log em " + file, e);
            }
            matched.sort(Comparator.comparing(AuditEvent::occurredAt).reversed());
            return matched.size() > limit ? matched.subList(0, limit) : matched;
        }
    }

    private AuditEvent tryParse(String line) {
        try {
            StoredLine stored = mapper.readValue(line, StoredLine.class);
            return new AuditEvent(
                    AuditLevel.parse(stored.level()),
                    stored.occurredAt(),
                    new ApplicationName(stored.app()),
                    stored.action(),
                    stored.context()
            );
        } catch (Exception e) {
            // Linha corrompida: pula, não derruba a consulta inteira.
            return null;
        }
    }

    private StoredLine toJson(AuditEvent entry) {
        return new StoredLine(
                entry.level().name(),
                entry.occurredAt(),
                entry.app().value(),
                entry.action(),
                entry.context()
        );
    }

    /** Forma serializada de um {@link AuditEvent} em disco. */
    private record StoredLine(
            String level,
            Instant occurredAt,
            String app,
            String action,
            Map<String, String> context
    ) {
    }
}
