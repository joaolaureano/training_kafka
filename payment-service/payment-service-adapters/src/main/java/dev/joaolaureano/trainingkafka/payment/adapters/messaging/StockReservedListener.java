package dev.joaolaureano.trainingkafka.payment.adapters.messaging;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Adapter de entrada: a cobrança começa quando o estoque confirma, não quando o
 * pedido é feito.
 *
 * Sem try/catch: a falha sobe para o error handler do container, que retenta ou
 * publica na DLQ conforme {@code kafka.dlq.*}.
 */
@Component
public final class StockReservedListener {

    private final StockReservedPort processPayment;

    public StockReservedListener(StockReservedPort processPayment) {
        this.processPayment = processPayment;
    }

    @KafkaListener(topics = Topics.INVENTORY_EVENTS, groupId = "${spring.kafka.consumer.group-id}")
    public void onInventoryEvent(InventoryEventMessage message) {
        processPayment.handle(message);
    }
}
