package dev.joaolaureano.trainingkafka.inventory.application.port;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Um registro estruturado de algo que a aplicação fez.
 *
 * Repare no que NÃO está aqui: o nome da aplicação. Saber "quem sou eu" é uma
 * questão de deployment, não de caso de uso — quem carimba isso é o adapter.
 */
public record ActivityLog(
        AuditLevel level,
        String action,
        Map<String, String> context,
        Instant occurredAt
) {

    public ActivityLog {
        context = Map.copyOf(context == null ? Map.of() : context);
    }

    public static ActivityLog info(String action, Map<String, String> context, Instant at) {
        return new ActivityLog(AuditLevel.INFO, action, context, at);
    }

    public static ActivityLog warn(String action, Map<String, String> context, Instant at) {
        return new ActivityLog(AuditLevel.WARN, action, context, at);
    }

    /** Açúcar para montar o contexto sem repetir {@code new LinkedHashMap<>()} em todo lugar. */
    public static Map<String, String> context(String... keyValuePairs) {
        if (keyValuePairs.length % 2 != 0) {
            throw new IllegalArgumentException("context espera pares chave/valor");
        }
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            map.put(keyValuePairs[i], keyValuePairs[i + 1]);
        }
        return map;
    }
}
