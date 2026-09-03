package dev.joaolaureano.trainingkafka.fraud.domain.model;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class CustomerFraudPatternTest {

    private static final Instant T0 = Instant.parse("2026-08-25T12:00:00Z");
    private static final FraudPolicy POLICY = new FraudPolicy(5, Duration.ofSeconds(10), Duration.ofSeconds(2));

    @Test
    void emitsOnlyWhenTheThresholdIsReached() {
        CustomerFraudPattern pattern = CustomerFraudPattern.startFor("cust-1", POLICY);

        for (int index = 0; index < 4; index++) {
            assertThat(pattern.register(order("order-" + index, index))).isEmpty();
        }
        assertThat(pattern.register(order("order-4", 4))).isPresent();
        assertThat(pattern.register(order("order-5", 5))).isEmpty();
    }

    @Test
    void duplicateOrderDoesNotChangeThePattern() {
        CustomerFraudPattern pattern = CustomerFraudPattern.startFor("cust-1", POLICY);

        pattern.register(order("order-1", 0));
        pattern.register(order("order-1", 1));

        assertThat(pattern.recentOrders()).hasSize(1);
        assertThat(pattern.knownOrderIds()).containsExactly("order-1");
    }

    @Test
    void ordersOutsideTheWindowArePruned() {
        CustomerFraudPattern pattern = CustomerFraudPattern.startFor("cust-1", POLICY);

        pattern.register(order("old", 0));
        pattern.register(order("current", 11));

        assertThat(pattern.recentOrders()).extracting(FraudOrder::orderId).containsExactly("current");
        assertThat(pattern.knownOrderIds()).containsExactly("current");
    }

    private FraudOrder order(String orderId, long second) {
        return new FraudOrder(orderId, T0.plusSeconds(second), BigDecimal.TEN);
    }
}
