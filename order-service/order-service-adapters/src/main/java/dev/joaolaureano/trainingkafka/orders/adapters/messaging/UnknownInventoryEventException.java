package dev.joaolaureano.trainingkafka.orders.adapters.messaging;

/**
 * Chegou um tipo de evento de estoque que este contexto não sabe traduzir.
 *
 * É {@link IllegalArgumentException} de propósito: o error handler trata essa
 * família como erro permanente e manda direto para a DLQ, sem gastar retentativa
 * numa mensagem que nunca vai ser entendida.
 */
public class UnknownInventoryEventException extends IllegalArgumentException {

    public UnknownInventoryEventException(String eventType) {
        super("evento de estoque desconhecido: " + eventType);
    }
}
