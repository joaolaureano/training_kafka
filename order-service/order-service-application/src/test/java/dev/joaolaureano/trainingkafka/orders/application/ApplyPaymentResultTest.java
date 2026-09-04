package dev.joaolaureano.trainingkafka.orders.application;

import dev.joaolaureano.trainingkafka.orders.domain.event.DomainEvent;
import dev.joaolaureano.trainingkafka.orders.domain.model.InvalidOrderTransitionException;
import dev.joaolaureano.trainingkafka.orders.domain.model.Order;
import dev.joaolaureano.trainingkafka.orders.domain.model.OrderId;
import dev.joaolaureano.trainingkafka.orders.domain.model.OrderStatus;
import dev.joaolaureano.trainingkafka.orders.domain.port.OrderRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** O lado Order da Saga: compensação, idempotência e o que fazer com o inesperado. */
class ApplyPaymentResultTest {

    private static final Instant NOW = Instant.parse("2026-09-03T12:00:00Z");

    private FakeRepository orders;
    private ApplyPaymentResult applyResult;
    private String orderId;

    @BeforeEach
    void setUp() {
        orders = new FakeRepository();
        applyResult = new ApplyPaymentResult(orders);
        Order order = Order.place("cust-1", "Teclado", 1, new BigDecimal("10.00"), NOW);
        // Desde que o estoque entrou na Saga, o pagamento só decide sobre um pedido
        // cujas unidades já foram separadas — este é o estado real em que ele chega.
        order.confirmStock();
        order.pullDomainEvents();
        orders.save(order, List.of());
        orderId = order.id().toString();
    }

    @Test
    @DisplayName("PaymentApproved leva o pedido a PAID")
    void approvesOrder() {
        applyResult.approved(orderId);

        assertThat(orders.byId(orderId).status()).isEqualTo(OrderStatus.PAID);
    }

    @Test
    @DisplayName("PaymentFailed compensa: o pedido é cancelado")
    void compensatesOnFailure() {
        applyResult.failed(orderId);

        assertThat(orders.byId(orderId).status()).isEqualTo(OrderStatus.CANCELLED);
    }

    @Test
    @DisplayName("resultado reentregue não muda o desfecho")
    void duplicateResultIsHarmless() {
        applyResult.approved(orderId);
        applyResult.approved(orderId);

        assertThat(orders.byId(orderId).status()).isEqualTo(OrderStatus.PAID);
    }

    @Test
    @DisplayName("resultado contraditório é recusado em vez de sobrescrever")
    void contradictoryResultIsRejected() {
        applyResult.approved(orderId);

        assertThatThrownBy(() -> applyResult.failed(orderId))
                .isInstanceOf(InvalidOrderTransitionException.class);
        assertThat(orders.byId(orderId).status()).isEqualTo(OrderStatus.PAID);
    }

    @Test
    @DisplayName("PaymentCancelled compensa um pedido já pago")
    void fraudCompensatesAPaidOrder() {
        applyResult.approved(orderId);

        applyResult.cancelledForFraud(orderId);

        assertThat(orders.byId(orderId).status()).isEqualTo(OrderStatus.CANCELLED);
    }

    @Test
    @DisplayName("PaymentCancelled reentregue não muda o desfecho")
    void duplicateFraudCancellationIsHarmless() {
        applyResult.approved(orderId);

        applyResult.cancelledForFraud(orderId);
        applyResult.cancelledForFraud(orderId);

        assertThat(orders.byId(orderId).status()).isEqualTo(OrderStatus.CANCELLED);
    }

    @Test
    @DisplayName("pagamento de pedido desconhecido não é engolido em silêncio")
    void unknownOrderIsReported() {
        String unknown = UUID.randomUUID().toString();

        assertThatThrownBy(() -> applyResult.approved(unknown))
                .isInstanceOf(UnknownOrderException.class)
                .hasMessageContaining(unknown);
    }

    private static final class FakeRepository implements OrderRepository {
        private final Map<OrderId, Order> stored = new HashMap<>();

        Order byId(String orderId) {
            return stored.get(OrderId.parse(orderId));
        }

        @Override
        public Optional<Order> findById(OrderId orderId) {
            return Optional.ofNullable(stored.get(orderId));
        }

        @Override
        public void save(Order order, List<DomainEvent> events) {
            stored.put(order.id(), order);
        }

        @Override
        public void save(Order order) {
            save(order, List.of());
        }

        @Override
        public boolean applyTransition(dev.joaolaureano.trainingkafka.orders.domain.model.OrderId orderId,
                                       java.util.function.Consumer<Order> transition) {
            return findById(orderId).map(order -> {
                transition.accept(order);
                save(order);
                return true;
            }).orElse(false);
        }
    }
}
