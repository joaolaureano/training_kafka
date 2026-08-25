package dev.joaolaureano.trainingkafka.analytics.application;

import dev.joaolaureano.trainingkafka.analytics.domain.event.DomainEvent;
import dev.joaolaureano.trainingkafka.analytics.domain.event.OrderPlaced;
import dev.joaolaureano.trainingkafka.analytics.domain.event.SuspiciousPatternDetected;
import dev.joaolaureano.trainingkafka.analytics.domain.model.*;
import dev.joaolaureano.trainingkafka.analytics.domain.port.CustomerPatternRepository;
import dev.joaolaureano.trainingkafka.analytics.domain.port.DomainEventPublisher;
import dev.joaolaureano.trainingkafka.analytics.domain.port.OrderLedgerRepository;
import dev.joaolaureano.trainingkafka.analytics.domain.port.ProductSalesRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

class OrderPlacedHandlerTest {

    private static final Instant T0 = Instant.parse("2026-08-25T12:00:00Z");
    private static final SuspicionPolicy POLICY = SuspicionPolicy.of(5, Duration.ofSeconds(10));

    private FakeLedger ledger;
    private FakeProductSales productSales;
    private FakeCustomerPatterns customerPatterns;
    private RecordingPublisher publisher;
    private OrderPlacedHandler handler;

    @BeforeEach
    void setUp() {
        ledger = new FakeLedger();
        productSales = new FakeProductSales();
        customerPatterns = new FakeCustomerPatterns();
        publisher = new RecordingPublisher();
        handler = new OrderPlacedHandler(ledger, productSales, customerPatterns, publisher, POLICY);
    }

    private OrderPlaced orderFrom(String customer, String product, int qty, String amount, long second) {
        return new OrderPlaced(new OrderId(UUID.randomUUID()), new CustomerId(customer),
                new ProductId(product), new Quantity(qty), Money.of(amount), T0.plusSeconds(second));
    }

    @Test
    @DisplayName("registra o pedido no ledger, acumula vendas e avalia o cliente")
    void touchesAllThreeAggregates() {
        handler.handle(orderFrom("cust-1", "Teclado", 2, "100.00", 0));

        assertThat(ledger.records).hasSize(1);
        assertThat(productSales.byProduct.get(new ProductId("Teclado")).unitsSold().value()).isEqualTo(2);
        assertThat(customerPatterns.byCustomer.get(new CustomerId("cust-1")).ordersInWindow()).isEqualTo(1);
    }

    @Test
    @DisplayName("acumula vendas do mesmo produto vindas de clientes diferentes")
    void accumulatesAcrossCustomers() {
        handler.handle(orderFrom("cust-1", "Teclado", 2, "100.00", 0));
        handler.handle(orderFrom("cust-2", "Teclado", 3, "150.00", 1));

        ProductSalesRecord teclado = productSales.byProduct.get(new ProductId("Teclado"));
        assertThat(teclado.unitsSold().value()).isEqualTo(5);
        assertThat(teclado.revenue().toString()).isEqualTo("250.00");
        assertThat(teclado.orderCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("despacha o alerta que o AGREGADO decidiu emitir — sem reavaliar a regra")
    void dispatchesAggregateDecision() {
        for (int i = 0; i < 5; i++) {
            handler.handle(orderFrom("cust-burst", "Mouse", 1, "50.00", i));
        }

        assertThat(publisher.published)
                .filteredOn(SuspiciousPatternDetected.class::isInstance)
                .singleElement()
                .isInstanceOfSatisfying(SuspiciousPatternDetected.class, alert -> {
                    assertThat(alert.customerId().value()).isEqualTo("cust-burst");
                    assertThat(alert.ordersInWindow()).isEqualTo(5);
                });
    }

    @Test
    @DisplayName("tráfego normal não gera alerta algum")
    void normalTrafficIsSilent() {
        for (int i = 0; i < 30; i++) {
            handler.handle(orderFrom("cust-" + i, "Produto-" + i, 1, "10.00", i));
        }

        assertThat(publisher.published).isEmpty();
    }

    @Test
    @DisplayName("o mesmo evento drena os eventos do agregado uma única vez")
    void drainsEventsOnce() {
        for (int i = 0; i < 8; i++) {
            handler.handle(orderFrom("cust-burst", "Mouse", 1, "50.00", i));
        }

        assertThat(publisher.published).hasSize(1);
    }

    @Test
    @DisplayName("o handler não contém regra de negócio")
    void handlerHasNoBusinessRules() {
        // Documenta a intenção arquitetural: o orquestrador tem apenas o método
        // público handle e auxiliares privados de coordenação. Qualquer método
        // público extra aqui seria sinal de lógica se acumulando no lugar errado.
        assertThat(OrderPlacedHandler.class.getDeclaredMethods())
                .filteredOn(method -> java.lang.reflect.Modifier.isPublic(method.getModifiers()))
                .extracting(java.lang.reflect.Method::getName)
                .containsExactly("handle");
    }

    // --- Fakes: implementações de brinquedo dos Ports, para o teste rodar sem I/O ---

    private static final class FakeLedger implements OrderLedgerRepository {
        private final List<OrderRecord> records = new ArrayList<>();

        @Override
        public void append(OrderRecord record) {
            records.add(record);
        }

        @Override
        public RevenueWindow revenueOver(TimeRange range) {
            Money total = records.stream()
                    .filter(r -> range.contains(r.placedAt()))
                    .map(OrderRecord::amount)
                    .reduce(Money.ZERO, Money::plus);
            long count = records.stream().filter(r -> range.contains(r.placedAt())).count();
            return new RevenueWindow(range, total, count);
        }

        @Override
        public RevenueWindow revenueOver(TimeRange range, ProductId productId) {
            return revenueOver(range);
        }
    }

    private static final class FakeProductSales implements ProductSalesRepository {
        private final Map<ProductId, ProductSalesRecord> byProduct = new LinkedHashMap<>();

        @Override
        public ProductSalesRecord findOrCreate(ProductId productId) {
            return byProduct.computeIfAbsent(productId, ProductSalesRecord::startFor);
        }

        @Override
        public void save(ProductSalesRecord record) {
            byProduct.put(record.productId(), record);
        }

        @Override
        public List<ProductSalesRecord> topSelling(int limit) {
            return byProduct.values().stream()
                    .sorted(Comparator.comparing(ProductSalesRecord::unitsSold).reversed())
                    .limit(limit)
                    .toList();
        }
    }

    private static final class FakeCustomerPatterns implements CustomerPatternRepository {
        private final Map<CustomerId, CustomerOrderPattern> byCustomer = new LinkedHashMap<>();

        @Override
        public CustomerOrderPattern findOrCreate(CustomerId customerId, SuspicionPolicy policy) {
            return byCustomer.computeIfAbsent(customerId, id -> CustomerOrderPattern.startFor(id, policy));
        }

        @Override
        public void save(CustomerOrderPattern pattern) {
            byCustomer.put(pattern.customerId(), pattern);
        }
    }

    private static final class RecordingPublisher implements DomainEventPublisher {
        private final List<DomainEvent> published = new ArrayList<>();

        @Override
        public void publish(DomainEvent event) {
            published.add(event);
        }
    }
}
