package dev.joaolaureano.trainingkafka.payment.adapters.messaging;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public final class OrderPlacedListener {

    private final OrderPlacedPort processPayment;

    public OrderPlacedListener(OrderPlacedPort processPayment) {
        this.processPayment = processPayment;
    }

    @KafkaListener(topics = Topics.ORDERS, groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderPlaced(OrderPlacedMessage message) {
        processPayment.handle(message);
    }
}
