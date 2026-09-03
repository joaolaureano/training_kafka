package dev.joaolaureano.trainingkafka.fraud.domain.model;

import dev.joaolaureano.trainingkafka.fraud.domain.event.FraudDetected;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public final class CustomerFraudPattern {

    private final String customerId;
    private final FraudPolicy policy;
    private final List<FraudOrder> recentOrders;
    private final Set<String> knownOrderIds;

    private CustomerFraudPattern(String customerId, FraudPolicy policy,
                                 List<FraudOrder> recentOrders, Set<String> knownOrderIds) {
        this.customerId = customerId;
        this.policy = policy;
        this.recentOrders = new ArrayList<>(recentOrders);
        this.knownOrderIds = new LinkedHashSet<>(knownOrderIds);
    }

    public static CustomerFraudPattern startFor(String customerId, FraudPolicy policy) {
        if (customerId == null || customerId.isBlank()) {
            throw new IllegalArgumentException("customerId is required");
        }
        return new CustomerFraudPattern(customerId, policy, List.of(), Set.of());
    }

    public static CustomerFraudPattern reconstitute(String customerId, FraudPolicy policy,
                                                    List<FraudOrder> recentOrders,
                                                    Set<String> knownOrderIds) {
        if (recentOrders == null || knownOrderIds == null) {
            throw new IllegalArgumentException("state is required");
        }
        return new CustomerFraudPattern(customerId, policy, recentOrders, knownOrderIds);
    }

    public Optional<FraudDetected> register(FraudOrder order) {
        if (knownOrderIds.contains(order.orderId())) {
            return Optional.empty();
        }

        pruneBefore(order.occurredAt());
        boolean wasFraudulent = isFraudulent();
        recentOrders.add(order);
        knownOrderIds.add(order.orderId());

        if (!wasFraudulent && isFraudulent()) {
            // A janela inteira: é a lista de pedidos que precisam ser compensados.
            return Optional.of(new FraudDetected(customerId, policy.window(),
                    recentOrders.stream()
                            .sorted(Comparator.comparing(FraudOrder::occurredAt))
                            .toList(),
                    order.occurredAt()));
        }
        return Optional.empty();
    }

    private void pruneBefore(Instant windowEnd) {
        Instant cutoff = windowEnd.minus(policy.window());
        List<String> expiredOrderIds = recentOrders.stream()
                .filter(order -> order.occurredAt().isBefore(cutoff))
                .map(FraudOrder::orderId)
                .toList();
        recentOrders.removeIf(order -> order.occurredAt().isBefore(cutoff));
        knownOrderIds.removeAll(expiredOrderIds);
    }

    public boolean isFraudulent() {
        return recentOrders.size() >= policy.maxOrders();
    }

    public String customerId() {
        return customerId;
    }

    public List<FraudOrder> recentOrders() {
        return List.copyOf(recentOrders);
    }

    public Set<String> knownOrderIds() {
        return Set.copyOf(knownOrderIds);
    }
}
