package dev.joaolaureano.trainingkafka.payment.application;

import dev.joaolaureano.trainingkafka.payment.domain.event.DomainEvent;
import dev.joaolaureano.trainingkafka.payment.domain.event.PaymentCancelled;
import dev.joaolaureano.trainingkafka.payment.domain.model.Payment;
import dev.joaolaureano.trainingkafka.payment.domain.model.PaymentStatus;
import dev.joaolaureano.trainingkafka.payment.domain.port.PaymentGateway;
import dev.joaolaureano.trainingkafka.payment.domain.port.PaymentRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** A compensação da Saga quando a fraude só aparece depois do pagamento. */
class CompensateFraudulentOrdersTest {

    private static final Instant NOW = Instant.parse("2026-09-03T12:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    private final FakeRepository payments = new FakeRepository();
    private final CompensateFraudulentOrders compensate =
            new CompensateFraudulentOrders(payments, CLOCK);

    @Test
    @DisplayName("estorna a janela inteira, um evento por pedido")
    void refundsEveryOrderInTheWindow() {
        approvedPayment("order-1");
        approvedPayment("order-2");

        compensate.handle("cust-1", window("order-1", "order-2"), "fraud", "corr-f");

        assertThat(payments.events).hasSize(2)
                .allSatisfy(event -> assertThat(event).isInstanceOfSatisfying(
                        PaymentCancelled.class, cancelled -> assertThat(cancelled.refunded()).isTrue()));
        assertThat(payments.stored.values())
                .allSatisfy(payment -> assertThat(payment.status()).isEqualTo(PaymentStatus.CANCELLED));
    }

    @Test
    @DisplayName("pagamento ainda não visto é criado já cancelado")
    void unseenPaymentIsCreatedAlreadyCancelled() {
        // Fraud e Payment consomem `orders` em grupos separados: nada garante quem
        // chega primeiro.
        compensate.handle("cust-1", window("order-1"), "fraud", "corr-f");

        Payment created = payments.stored.get("order-1");
        assertThat(created.status()).isEqualTo(PaymentStatus.CANCELLED);
        assertThat(created.refunded()).isFalse();
        assertThat(payments.events).singleElement().isInstanceOf(PaymentCancelled.class);
    }

    @Test
    @DisplayName("o OrderPlaced que chegar depois não cobra o pedido cancelado")
    void theLateOrderPlacedIsNotCharged() {
        compensate.handle("cust-1", window("order-1"), "fraud", "corr-f");
        payments.events.clear();

        RecordingGateway gateway = new RecordingGateway();
        new ProcessOrderPayment(payments, gateway, CLOCK)
                .handle("order-1", "cust-1", BigDecimal.TEN, "corr-1");

        assertThat(gateway.charges).isZero();
        assertThat(payments.stored.get("order-1").status()).isEqualTo(PaymentStatus.CANCELLED);
        // Nenhum evento novo: o PaymentCancelled já foi enfileirado na compensação.
        assertThat(payments.events).isEmpty();
    }

    @Test
    @DisplayName("FraudDetected reentregue não estorna duas vezes")
    void redeliveredFraudIsIdempotent() {
        approvedPayment("order-1");

        compensate.handle("cust-1", window("order-1"), "fraud", "corr-f");
        compensate.handle("cust-1", window("order-1"), "fraud", "corr-f");

        assertThat(payments.events).hasSize(1);
    }

    @Test
    @DisplayName("pagamento que já falhou não tem o que compensar")
    void failedPaymentIsLeftAlone() {
        Payment failed = Payment.request("order-1", "cust-1", BigDecimal.TEN);
        failed.fail(NOW, "declined", "corr-1");
        failed.pullDomainEvents();
        payments.save(failed);

        compensate.handle("cust-1", window("order-1"), "fraud", "corr-f");

        assertThat(payments.stored.get("order-1").status()).isEqualTo(PaymentStatus.FAILED);
        assertThat(payments.events).isEmpty();
    }

    private void approvedPayment(String orderId) {
        Payment payment = Payment.request(orderId, "cust-1", BigDecimal.TEN);
        payment.approve(NOW, "corr-1");
        payment.pullDomainEvents();
        payments.save(payment);
    }

    private static List<CompensateFraudulentOrders.FraudulentOrder> window(String... orderIds) {
        return java.util.Arrays.stream(orderIds)
                .map(id -> new CompensateFraudulentOrders.FraudulentOrder(id, BigDecimal.TEN))
                .toList();
    }

    /**
     * O caso de uso não publica mais: grava o pagamento e seus eventos na mesma
     * chamada. O fake registra os dois lados para que o teste possa afirmar que o
     * evento foi enfileirado JUNTO do desfecho, e não depois dele.
     */
    private static final class FakeRepository implements PaymentRepository {
        private final Map<String, Payment> stored = new HashMap<>();
        private final List<DomainEvent> events = new ArrayList<>();

        @Override
        public Optional<Payment> findByOrderId(String orderId) {
            return Optional.ofNullable(stored.get(orderId));
        }

        @Override
        public void save(Payment payment, List<DomainEvent> newEvents) {
            stored.put(payment.orderId(), payment);
            events.addAll(newEvents);
        }

        @Override
        public void save(Payment payment) {
            save(payment, List.of());
        }
    }

    private static final class RecordingGateway implements PaymentGateway {
        private int charges;

        @Override
        public GatewayResult charge(Payment payment) {
            charges++;
            return GatewayResult.accepted();
        }
    }

}
