package dev.joaolaureano.trainingkafka.inventory.adapters.messaging;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Adapter de entrada da compensação: o desfecho do pagamento pode devolver
 * unidades ao estoque.
 *
 * Container próprio porque o contrato é outro — ver
 * {@code PaymentEventConsumerConfig}.
 */
@Component
public final class PaymentEventListener {

    private final PaymentEventPort paymentEvents;

    public PaymentEventListener(PaymentEventPort paymentEvents) {
        this.paymentEvents = paymentEvents;
    }

    @KafkaListener(topics = Topics.PAYMENT_EVENTS, groupId = "${inventory.payment.group-id}",
            containerFactory = "paymentEventListenerContainerFactory")
    public void onPaymentEvent(PaymentEventMessage message) {
        paymentEvents.handle(message);
    }
}
