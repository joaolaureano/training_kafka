package dev.joaolaureano.trainingkafka.inventory.adapters.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.joaolaureano.trainingkafka.inventory.adapters.persistence.OutboxRecord;
import dev.joaolaureano.trainingkafka.inventory.adapters.persistence.OutboxTranslator;
import dev.joaolaureano.trainingkafka.inventory.domain.event.DomainEvent;
import dev.joaolaureano.trainingkafka.inventory.domain.event.StockRejected;
import dev.joaolaureano.trainingkafka.inventory.domain.event.StockReleased;
import dev.joaolaureano.trainingkafka.inventory.domain.event.StockReserved;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Toda a tradução para o fio está aqui: tipo do evento, chave de partição e JSON.
 *
 * A chave é o orderId. Todos os desfechos de estoque de um mesmo pedido — reserva
 * e devolução — caem na mesma partição e chegam em ordem a quem consome; é dessa
 * ordem que dependem tanto a guarda de transição do Order quanto a decisão do
 * payment de cobrar.
 */
public final class InventoryEventOutboxTranslator implements OutboxTranslator {

    private final ObjectMapper objectMapper;

    public InventoryEventOutboxTranslator(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public OutboxRecord translate(DomainEvent event) {
        InventoryEventMessage message = switch (event) {
            case StockReserved reserved -> new InventoryEventMessage(
                    InventoryEventMessage.STOCK_RESERVED,
                    eventId(InventoryEventMessage.STOCK_RESERVED, reserved.orderId()),
                    reserved.correlationId(), reserved.orderId(), reserved.customerId(),
                    reserved.sku().value(), reserved.quantity().value(), reserved.amount(),
                    null, reserved.occurredAt().toString());
            case StockRejected rejected -> new InventoryEventMessage(
                    InventoryEventMessage.STOCK_REJECTED,
                    eventId(InventoryEventMessage.STOCK_REJECTED, rejected.orderId()),
                    rejected.correlationId(), rejected.orderId(), rejected.customerId(),
                    rejected.sku().value(), rejected.quantity().value(), rejected.amount(),
                    rejected.reason().name(), rejected.occurredAt().toString());
            case StockReleased released -> new InventoryEventMessage(
                    InventoryEventMessage.STOCK_RELEASED,
                    eventId(InventoryEventMessage.STOCK_RELEASED, released.orderId()),
                    released.correlationId(), released.orderId(), released.customerId(),
                    released.sku().value(), released.quantity().value(), released.amount(),
                    released.reason(), released.occurredAt().toString());
            default -> throw new IllegalArgumentException(
                    "Evento de estoque sem tradução: " + event.getClass().getSimpleName());
        };
        return new OutboxRecord(Topics.INVENTORY_EVENTS, message.orderId(), message.eventType(),
                serialize(message));
    }

    private String serialize(InventoryEventMessage message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("Could not serialize " + message.eventType(), failure);
        }
    }

    /**
     * Derivado do pedido e do tipo, não aleatório: uma reentrega carrega o MESMO
     * eventId, e quem quiser deduplicar consegue.
     */
    private static String eventId(String type, String orderId) {
        return UUID.nameUUIDFromBytes((type + ':' + orderId).getBytes(StandardCharsets.UTF_8))
                .toString();
    }
}
