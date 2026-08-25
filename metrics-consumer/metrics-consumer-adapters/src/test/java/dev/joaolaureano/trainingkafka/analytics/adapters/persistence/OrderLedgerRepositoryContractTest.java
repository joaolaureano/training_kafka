package dev.joaolaureano.trainingkafka.analytics.adapters.persistence;

import dev.joaolaureano.trainingkafka.analytics.adapters.persistence.duckdb.DuckDbOrderLedgerRepository;
import dev.joaolaureano.trainingkafka.analytics.adapters.persistence.inmemory.InMemoryOrderLedgerRepository;
import dev.joaolaureano.trainingkafka.analytics.adapters.persistence.sqlite.SqliteOrderLedgerRepository;
import dev.joaolaureano.trainingkafka.analytics.domain.model.*;
import dev.joaolaureano.trainingkafka.analytics.domain.port.OrderLedgerRepository;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class OrderLedgerRepositoryContractTest {

    private static final Instant T0 = Instant.parse("2026-08-25T12:00:00Z");

    static Stream<Arguments> implementations() {
        return Stream.of(
                Arguments.of("inmemory", (Supplier<OrderLedgerRepository>)
                        InMemoryOrderLedgerRepository::new),
                Arguments.of("sqlite", (Supplier<OrderLedgerRepository>)
                        () -> new SqliteOrderLedgerRepository(TestRepositories.freshSqlite())),
                Arguments.of("duckdb", (Supplier<OrderLedgerRepository>)
                        () -> new DuckDbOrderLedgerRepository(TestRepositories.freshDuckDb())));
    }

    private static OrderRecord orderAt(String product, String amount, long secondsFromT0) {
        return new OrderRecord(new OrderId(UUID.randomUUID()), new CustomerId("cust-1"),
                new ProductId(product), new Quantity(1), Money.of(amount), T0.plusSeconds(secondsFromT0));
    }

    @ParameterizedTest(name = "[{0}] período sem pedidos devolve zerado, não null")
    @MethodSource("implementations")
    void emptyPeriodIsZeroed(String name, Supplier<OrderLedgerRepository> factory) {
        OrderLedgerRepository repository = factory.get();

        RevenueWindow window = repository.revenueOver(new TimeRange(T0, T0.plusSeconds(3600)));

        assertThat(window).isNotNull();
        assertThat(window.total()).isEqualTo(Money.ZERO);
        assertThat(window.orderCount()).isZero();
        assertThat(window.averageTicket()).isEqualTo(Money.ZERO);
    }

    @ParameterizedTest(name = "[{0}] soma o faturamento do período")
    @MethodSource("implementations")
    void sumsRevenue(String name, Supplier<OrderLedgerRepository> factory) {
        OrderLedgerRepository repository = factory.get();

        repository.append(orderAt("Teclado", "100.00", 10));
        repository.append(orderAt("Mouse", "50.50", 20));

        RevenueWindow window = repository.revenueOver(new TimeRange(T0, T0.plusSeconds(3600)));

        assertThat(window.total().toString()).isEqualTo("150.50");
        assertThat(window.orderCount()).isEqualTo(2);
        assertThat(window.averageTicket().toString()).isEqualTo("75.25");
    }

    @ParameterizedTest(name = "[{0}] pedidos fora do período não entram na conta")
    @MethodSource("implementations")
    void excludesOutsideRange(String name, Supplier<OrderLedgerRepository> factory) {
        OrderLedgerRepository repository = factory.get();

        repository.append(orderAt("Teclado", "100.00", 10));
        repository.append(orderAt("Teclado", "999.00", 5000));

        RevenueWindow window = repository.revenueOver(new TimeRange(T0, T0.plusSeconds(60)));

        assertThat(window.total().toString()).isEqualTo("100.00");
        assertThat(window.orderCount()).isEqualTo(1);
    }

    @ParameterizedTest(name = "[{0}] o intervalo é fechado nos dois extremos")
    @MethodSource("implementations")
    void rangeIsInclusive(String name, Supplier<OrderLedgerRepository> factory) {
        OrderLedgerRepository repository = factory.get();

        repository.append(orderAt("Teclado", "10.00", 0));
        repository.append(orderAt("Teclado", "20.00", 60));

        RevenueWindow window = repository.revenueOver(new TimeRange(T0, T0.plusSeconds(60)));

        assertThat(window.orderCount()).isEqualTo(2);
        assertThat(window.total().toString()).isEqualTo("30.00");
    }

    @ParameterizedTest(name = "[{0}] filtra por produto quando pedido")
    @MethodSource("implementations")
    void filtersByProduct(String name, Supplier<OrderLedgerRepository> factory) {
        OrderLedgerRepository repository = factory.get();

        repository.append(orderAt("Teclado", "100.00", 10));
        repository.append(orderAt("Mouse", "50.00", 20));

        RevenueWindow window = repository.revenueOver(
                new TimeRange(T0, T0.plusSeconds(3600)), new ProductId("Teclado"));

        assertThat(window.total().toString()).isEqualTo("100.00");
        assertThat(window.orderCount()).isEqualTo(1);
    }

    @ParameterizedTest(name = "[{0}] soma de muitos centavos permanece exata")
    @MethodSource("implementations")
    void manyCentsStayExact(String name, Supplier<OrderLedgerRepository> factory) {
        OrderLedgerRepository repository = factory.get();

        for (int i = 0; i < 1000; i++) {
            repository.append(orderAt("Teclado", "0.01", i));
        }

        RevenueWindow window = repository.revenueOver(new TimeRange(T0, T0.plusSeconds(3600)));

        assertThat(window.total().toString()).isEqualTo("10.00");
        assertThat(window.orderCount()).isEqualTo(1000);
    }
}
