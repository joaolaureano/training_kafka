package dev.joaolaureano.trainingkafka.orders.application;

import dev.joaolaureano.trainingkafka.orders.domain.event.DomainEvent;
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

class FindOrderServiceTest {

    private static final Instant NOW = Instant.parse("2026-09-03T12:00:00Z");

    private FakeRepository orders;
    private FindOrderService findOrder;
    private Order order;

    @BeforeEach
    void setUp() {
        orders = new FakeRepository();
        findOrder = new FindOrderService(orders);
        order = Order.place("cust-1", "Teclado", 2, new BigDecimal("199.90"), NOW);
        order.pullDomainEvents();
        orders.save(order);
    }

    @Test
    @DisplayName("devolve o estado corrente do pedido")
    void returnsTheCurrentState() {
        assertThat(findOrder.byId(order.id().toString())).get().satisfies(view -> {
            assertThat(view.status()).isEqualTo("PENDING_STOCK");
            assertThat(view.customerId()).isEqualTo("cust-1");
            assertThat(view.product()).isEqualTo("Teclado");
            assertThat(view.quantity()).isEqualTo(2);
            assertThat(view.amount()).isEqualByComparingTo("199.90");
        });
    }

    @Test
    @DisplayName("acompanha o desfecho da Saga")
    void followsTheSagaOutcome() {
        order.confirmStock();
        orders.save(order);
        assertThat(findOrder.byId(order.id().toString()).orElseThrow().status())
                .isEqualTo("PENDING_PAYMENT");

        order.approvePayment();
        orders.save(order);
        assertThat(findOrder.byId(order.id().toString()).orElseThrow().status()).isEqualTo("PAID");

        order.cancelForFraud();
        orders.save(order);
        assertThat(findOrder.byId(order.id().toString()).orElseThrow().status()).isEqualTo("CANCELLED");
    }

    @Test
    @DisplayName("pedido inexistente é vazio, não erro")
    void unknownOrderIsEmpty() {
        assertThat(findOrder.byId(UUID.randomUUID().toString())).isEmpty();
    }

    @Test
    @DisplayName("id malformado responde como desconhecido, não como falha de servidor")
    void malformedIdIsTreatedAsUnknown() {
        // Perguntar por "abc" é perguntar por um pedido que não pode existir. A
        // resposta é a mesma de um id válido e desconhecido — 404, não 500.
        assertThat(findOrder.byId("nao-e-um-uuid")).isEmpty();
        assertThat(findOrder.byId(null)).isEmpty();
    }

    private static final class FakeRepository implements OrderRepository {
        private final Map<OrderId, Order> stored = new HashMap<>();

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
