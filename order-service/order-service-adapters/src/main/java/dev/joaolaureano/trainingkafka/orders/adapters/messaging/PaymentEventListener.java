package dev.joaolaureano.trainingkafka.orders.adapters.messaging;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public final class PaymentEventListener {

    private final PaymentEventPort paymentEvents;

    public PaymentEventListener(PaymentEventPort paymentEvents) {
        this.paymentEvents = paymentEvents;
    }

    @KafkaListener(topics = Topics.PAYMENT_EVENTS, groupId = "${order.payment.group-id}")
    public void onPaymentEvent(PaymentEventMessage message) {
        paymentEvents.handle(message);
    }
}
