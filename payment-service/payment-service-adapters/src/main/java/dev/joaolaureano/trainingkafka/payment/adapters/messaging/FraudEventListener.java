package dev.joaolaureano.trainingkafka.payment.adapters.messaging;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public final class FraudEventListener {

    private final FraudEventPort fraudEvents;

    public FraudEventListener(FraudEventPort fraudEvents) {
        this.fraudEvents = fraudEvents;
    }

    @KafkaListener(topics = Topics.FRAUD_EVENTS, groupId = "${payment.fraud.group-id}",
            containerFactory = "fraudEventListenerContainerFactory")
    public void onFraudDetected(FraudDetectedMessage message) {
        fraudEvents.handle(message);
    }
}
