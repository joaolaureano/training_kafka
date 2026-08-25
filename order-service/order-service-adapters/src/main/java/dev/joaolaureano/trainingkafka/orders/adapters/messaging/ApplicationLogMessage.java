package dev.joaolaureano.trainingkafka.orders.adapters.messaging;

import java.util.Map;

/**
 * Contrato JSON do tópico "application-logs", consumido pelo App C.
 *
 * O campo {@code app} é carimbado aqui, pelo adapter, e não pela camada de
 * aplicação: qual serviço está rodando é informação de deployment.
 */
public record ApplicationLogMessage(
        String level,
        String timestamp,
        String app,
        String message,
        Map<String, String> context
) {
}
