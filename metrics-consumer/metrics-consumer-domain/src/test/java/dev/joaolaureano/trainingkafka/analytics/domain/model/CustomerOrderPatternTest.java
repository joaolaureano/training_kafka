package dev.joaolaureano.trainingkafka.analytics.domain.model;

import dev.joaolaureano.trainingkafka.analytics.domain.event.SuspiciousPatternDetected;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class CustomerOrderPatternTest {

    private static final Instant T0 = Instant.parse("2026-08-25T12:00:00Z");
    private static final SuspicionPolicy POLICY = SuspicionPolicy.of(5, Duration.ofSeconds(10));
    private static final CustomerId CUSTOMER = new CustomerId("cust-suspeito");

    private static CustomerOrderPattern freshPattern() {
        return CustomerOrderPattern.startFor(CUSTOMER, POLICY);
    }

    private static void placeOrderAt(CustomerOrderPattern pattern, long secondsFromT0) {
        pattern.registerOrder(new OrderId(UUID.randomUUID()),
                T0.plusSeconds(secondsFromT0), Money.of("50.00"));
    }

    @Nested
    @DisplayName("a decisão de suspeita")
    class SuspicionDecision {

        @Test
        @DisplayName("abaixo do limiar, o padrão é normal")
        void belowThresholdIsNormal() {
            CustomerOrderPattern pattern = freshPattern();
            for (int i = 0; i < 4; i++) {
                placeOrderAt(pattern, i);
            }

            assertThat(pattern.isSuspicious()).isFalse();
            assertThat(pattern.pendingEvents()).isEmpty();
        }

        @Test
        @DisplayName("ao atingir o limiar dentro da janela, o próprio agregado emite o alerta")
        void firesOnThreshold() {
            CustomerOrderPattern pattern = freshPattern();
            for (int i = 0; i < 5; i++) {
                placeOrderAt(pattern, i);
            }

            assertThat(pattern.isSuspicious()).isTrue();
            assertThat(pattern.pendingEvents()).singleElement()
                    .isInstanceOfSatisfying(SuspiciousPatternDetected.class, alert -> {
                        assertThat(alert.customerId()).isEqualTo(CUSTOMER);
                        assertThat(alert.ordersInWindow()).isEqualTo(5);
                        assertThat(alert.window()).isEqualTo(Duration.ofSeconds(10));
                        assertThat(alert.sample()).hasSize(5);
                    });
        }

        @Test
        @DisplayName("pedidos espaçados além da janela nunca acionam o alerta")
        void spreadOutOrdersNeverFire() {
            CustomerOrderPattern pattern = freshPattern();
            for (int i = 0; i < 20; i++) {
                placeOrderAt(pattern, i * 30L);
            }

            assertThat(pattern.isSuspicious()).isFalse();
            assertThat(pattern.pendingEvents()).isEmpty();
        }
    }

    @Nested
    @DisplayName("o comportamento sob rajada — o que o k6 vai provocar")
    class BurstBehaviour {

        @Test
        @DisplayName("500 pedidos em rajada geram UM alerta, não 496")
        void burstFiresOnlyOnce() {
            CustomerOrderPattern pattern = freshPattern();
            for (int i = 0; i < 500; i++) {
                pattern.registerOrder(new OrderId(UUID.randomUUID()),
                        T0.plusMillis(i * 10L), Money.of("50.00"));
            }

            assertThat(pattern.pendingEvents()).hasSize(1);
        }

        @Test
        @DisplayName("depois que a janela drena, um novo surto volta a alertar")
        void alertsAgainAfterWindowDrains() {
            CustomerOrderPattern pattern = freshPattern();
            for (int i = 0; i < 5; i++) {
                placeOrderAt(pattern, i);
            }
            assertThat(pattern.pullDomainEvents()).hasSize(1);

            // Silêncio longo o bastante para a janela esvaziar, depois novo surto.
            for (int i = 0; i < 5; i++) {
                placeOrderAt(pattern, 600 + i);
            }

            assertThat(pattern.pendingEvents()).hasSize(1);
        }
    }

    @Nested
    @DisplayName("a poda da janela")
    class WindowPruning {

        @Test
        @DisplayName("mantém o agregado pequeno, descartando o que saiu da janela")
        void prunesOldPlacements() {
            CustomerOrderPattern pattern = freshPattern();
            for (int i = 0; i < 200; i++) {
                placeOrderAt(pattern, i * 5L);
            }

            // Janela de 10s com pedidos a cada 5s: no máximo 3 cabem.
            assertThat(pattern.ordersInWindow()).isLessThanOrEqualTo(3);
        }
    }

    @Nested
    @DisplayName("reconstituição a partir do estado persistido")
    class Reconstitution {

        @Test
        @DisplayName("deriva a suspeita da janela, em vez de confiar num booleano guardado")
        void derivesSuspicionFromWindow() {
            var placements = new java.util.ArrayList<OrderPlacement>();
            for (int i = 0; i < 5; i++) {
                placements.add(new OrderPlacement(new OrderId(UUID.randomUUID()),
                        T0.plusSeconds(i), Money.of("50.00")));
            }

            CustomerOrderPattern restored =
                    CustomerOrderPattern.reconstitute(CUSTOMER, POLICY, placements);

            assertThat(restored.isSuspicious()).isTrue();
            // Reconstituir não é um fato novo: não dispara alerta.
            assertThat(restored.pendingEvents()).isEmpty();
        }

        @Test
        @DisplayName("mudar o limiar na configuração vale imediatamente, sem migrar dado")
        void policyIsAParameterNotStoredState() {
            var placements = new java.util.ArrayList<OrderPlacement>();
            for (int i = 0; i < 5; i++) {
                placements.add(new OrderPlacement(new OrderId(UUID.randomUUID()),
                        T0.plusSeconds(i), Money.of("50.00")));
            }

            var strict = CustomerOrderPattern.reconstitute(CUSTOMER, SuspicionPolicy.of(3, Duration.ofSeconds(10)), placements);
            var lenient = CustomerOrderPattern.reconstitute(CUSTOMER, SuspicionPolicy.of(50, Duration.ofSeconds(10)), placements);

            assertThat(strict.isSuspicious()).isTrue();
            assertThat(lenient.isSuspicious()).isFalse();
        }
    }
}
