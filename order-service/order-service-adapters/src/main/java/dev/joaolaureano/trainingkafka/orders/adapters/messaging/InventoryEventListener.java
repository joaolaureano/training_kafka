package dev.joaolaureano.trainingkafka.orders.adapters.messaging;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Adapter de entrada do primeiro elo da Saga.
 *
 * Container próprio porque o contrato é outro — ver
 * {@code InventoryEventConsumerConfig}.
 */
@Component
public final class InventoryEventListener {

    private final InventoryEventPort inventoryEvents;

    public InventoryEventListener(InventoryEventPort inventoryEvents) {
        this.inventoryEvents = inventoryEvents;
    }

    @KafkaListener(topics = Topics.INVENTORY_EVENTS, groupId = "${order.inventory.group-id}",
            containerFactory = "inventoryEventListenerContainerFactory")
    public void onInventoryEvent(InventoryEventMessage message) {
        inventoryEvents.handle(message);
    }
}
