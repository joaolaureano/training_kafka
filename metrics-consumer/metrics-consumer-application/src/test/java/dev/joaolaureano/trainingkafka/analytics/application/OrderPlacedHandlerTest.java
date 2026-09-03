package dev.joaolaureano.trainingkafka.analytics.application;

import dev.joaolaureano.trainingkafka.analytics.domain.event.OrderPlaced;
import dev.joaolaureano.trainingkafka.analytics.domain.model.Money;
import dev.joaolaureano.trainingkafka.analytics.domain.model.OrderId;
import dev.joaolaureano.trainingkafka.analytics.domain.model.OrderRecord;
import dev.joaolaureano.trainingkafka.analytics.domain.model.ProductId;
import dev.joaolaureano.trainingkafka.analytics.domain.model.ProductSalesRecord;
import dev.joaolaureano.trainingkafka.analytics.domain.model.Quantity;
import dev.joaolaureano.trainingkafka.analytics.domain.model.RevenueWindow;
import dev.joaolaureano.trainingkafka.analytics.domain.model.TimeRange;
import dev.joaolaureano.trainingkafka.analytics.domain.port.OrderLedgerRepository;
import dev.joaolaureano.trainingkafka.analytics.domain.port.ProductSalesRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class OrderPlacedHandlerTest {

    private static final Instant T0 = Instant.parse("2026-08-25T12:00:00Z");

    private FakeLedger ledger;
    private FakeProductSales productSales;
    private OrderPlacedHandler handler;

    @BeforeEach
    void setUp() {
        ledger = new FakeLedger();
        productSales = new FakeProductSales();
        handler = new OrderPlacedHandler(ledger, productSales);
    }

    @Test
    void recordsOrderAndAccumulatesProductSales() {
        handler.handle(orderFrom("cust-1", "Keyboard", 2, "100.00", 0));
        handler.handle(orderFrom("cust-2", "Keyboard", 3, "150.00", 1));

        assertThat(ledger.records).hasSize(2);
        ProductSalesRecord keyboard = productSales.byProduct.get(new ProductId("Keyboard"));
        assertThat(keyboard.unitsSold().value()).isEqualTo(5);
        assertThat(keyboard.revenue().toString()).isEqualTo("250.00");
        assertThat(keyboard.orderCount()).isEqualTo(2);
    }

    @Test
    void doesNotPublishFraudEvents() {
        for (int i = 0; i < 10; i++) {
            handler.handle(orderFrom("cust-burst", "Mouse", 1, "50.00", i));
        }

        assertThat(productSales.byProduct).containsKey(new ProductId("Mouse"));
    }

    private OrderPlaced orderFrom(String customer, String product, int quantity,
                                  String amount, long second) {
        return new OrderPlaced(new OrderId(UUID.randomUUID()),
                new dev.joaolaureano.trainingkafka.analytics.domain.model.CustomerId(customer),
                new ProductId(product), new Quantity(quantity), Money.of(amount), T0.plusSeconds(second));
    }

    private static final class FakeLedger implements OrderLedgerRepository {
        private final List<OrderRecord> records = new ArrayList<>();

        @Override
        public void append(OrderRecord record) {
            records.add(record);
        }

        @Override
        public RevenueWindow revenueOver(TimeRange range) {
            Money total = records.stream().filter(record -> range.contains(record.placedAt()))
                    .map(OrderRecord::amount).reduce(Money.ZERO, Money::plus);
            long count = records.stream().filter(record -> range.contains(record.placedAt())).count();
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
                    .limit(limit).toList();
        }
    }
}
