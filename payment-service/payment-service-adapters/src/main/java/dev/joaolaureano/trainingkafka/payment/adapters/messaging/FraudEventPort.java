package dev.joaolaureano.trainingkafka.payment.adapters.messaging;

public interface FraudEventPort {
    void handle(FraudDetectedMessage message);
}
