package dev.joaolaureano.trainingkafka.inventory.adapters.messaging;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Adapter de entrada: cada pedido novo vira uma tentativa de reserva.
 *
 * Sem try/catch: a falha sobe para o error handler do container, que retenta ou
 * publica na DLQ conforme {@code kafka.dlq.*}.
 */
@Component
public final class OrderPlacedListener {

    private final OrderPlacedPort reserveStock;

    public OrderPlacedListener(OrderPlacedPort reserveStock) {
        this.reserveStock = reserveStock;
    }

    @KafkaListener(topics = Topics.ORDERS, groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderPlaced(OrderPlacedMessage message) {
        reserveStock.handle(message);
    }
}
