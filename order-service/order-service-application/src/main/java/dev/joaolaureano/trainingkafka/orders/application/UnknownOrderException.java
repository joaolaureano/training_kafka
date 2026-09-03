package dev.joaolaureano.trainingkafka.orders.application;

/**
 * Chegou um resultado de pagamento para um pedido que este serviço não conhece.
 *
 * Com o outbox, o pedido é commitado ANTES de OrderPlaced sair — então não existe
 * corrida legítima que produza isto. Engolir em silêncio (o {@code ifPresent} de
 * antes) perderia a Saga sem deixar rastro; levantar deixa a mensagem ir para a
 * DLQ, onde dá para inspecionar o que aconteceu de fato.
 */
public class UnknownOrderException extends RuntimeException {

    public UnknownOrderException(String orderId) {
        super("no local order for payment result: " + orderId);
    }
}
