package dev.joaolaureano.trainingkafka.fraud.adapters.streams;

import dev.joaolaureano.trainingkafka.fraud.domain.model.FraudOrder;

import java.time.Instant;
import java.util.List;
import java.util.Set;

public record CustomerFraudState(
        List<FraudOrder> recentOrders,
        Set<String> knownOrderIds,
        Instant latestEventTime) {

    public CustomerFraudState {
        recentOrders = List.copyOf(recentOrders);
        knownOrderIds = Set.copyOf(knownOrderIds);
    }

    public static CustomerFraudState empty() {
        return new CustomerFraudState(List.of(), Set.of(), null);
    }
}
