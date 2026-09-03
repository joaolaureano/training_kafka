package dev.joaolaureano.trainingkafka.fraud.application;

import dev.joaolaureano.trainingkafka.fraud.domain.event.FraudDetected;
import dev.joaolaureano.trainingkafka.fraud.domain.model.CustomerFraudPattern;
import dev.joaolaureano.trainingkafka.fraud.domain.model.FraudOrder;

import java.util.Optional;

public class FraudDetectionService {

    public Optional<FraudDetected> detect(CustomerFraudPattern pattern, FraudOrder order) {
        return pattern.register(order);
    }
}
