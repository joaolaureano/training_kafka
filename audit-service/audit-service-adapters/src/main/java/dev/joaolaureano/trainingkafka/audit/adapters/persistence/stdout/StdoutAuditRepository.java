package dev.joaolaureano.trainingkafka.audit.adapters.persistence.stdout;

import dev.joaolaureano.trainingkafka.audit.domain.model.AuditEvent;
import dev.joaolaureano.trainingkafka.audit.domain.model.AuditFilter;
import dev.joaolaureano.trainingkafka.audit.domain.port.AuditRepository;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Implementação de {@link AuditRepository} que apenas imprime cada registro em
 * {@code System.out}, de forma legível para um humano acompanhando o terminal.
 *
 * Não guarda nada em lugar nenhum: é um adapter "write-only".
 */
public class StdoutAuditRepository implements AuditRepository {

    @Override
    public void save(AuditEvent entry) {
        String context = formatContext(entry.context());
        System.out.printf(
                "%s  %-5s  [%s] %s%s%n",
                entry.occurredAt(),
                entry.level(),
                entry.app(),
                entry.action(),
                context.isEmpty() ? "" : " " + context
        );
    }

    /** Contexto vazio não deve imprimir chaves vazias ({@code {}}). */
    private String formatContext(Map<String, String> context) {
        if (context.isEmpty()) {
            return "";
        }
        return context.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(", ", "{", "}"));
    }

    /**
     * Sempre devolve lista vazia.
     *
     * <p><strong>Isto não significa "nenhum log casou com o filtro"</strong> — significa
     * que este adapter não armazena absolutamente nada, então não há onde consultar.
     * É uma limitação declarada deste adapter específico, não um bug.
     */
    @Override
    public List<AuditEvent> query(AuditFilter filter, int limit) {
        // Ver javadoc: lista vazia aqui é "não há onde procurar", não "nada casou".
        return List.of();
    }
}
