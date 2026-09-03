package dev.joaolaureano.trainingkafka.fraud.domain.event;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

public record FraudDetected(
        String customerId,
        int ordersInWindow,
        Duration window,
        List<String> sampleOrderIds,
        Instant occurredAt) {

    public FraudDetected {
        if (customerId == null || customerId.isBlank()) {
            throw new IllegalArgumentException("customerId is required");
        }
        sampleOrderIds = List.copyOf(sampleOrderIds);
    }
}
