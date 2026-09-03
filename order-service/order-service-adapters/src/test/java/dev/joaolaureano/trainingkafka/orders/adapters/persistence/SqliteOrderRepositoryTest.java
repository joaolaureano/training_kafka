package dev.joaolaureano.trainingkafka.orders.adapters.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.joaolaureano.trainingkafka.orders.adapters.messaging.OrderEventOutboxTranslator;
import dev.joaolaureano.trainingkafka.orders.domain.model.Order;
import dev.joaolaureano.trainingkafka.orders.domain.model.OrderStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SQLite de verdade, em memória: o que se está testando é justamente o SQL — a
 * atomicidade entre o pedido e o seu outbox não existiria num fake.
 */
class SqliteOrderRepositoryTest {

    private static final Instant NOW = Instant.parse("2026-09-03T12:00:00Z");

    private Connection connection;
    private SqliteOrderRepository repository;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:sqlite::memory:");
        connection.setAutoCommit(false);
        repository = new SqliteOrderRepository(connection,
                new OrderEventOutboxTranslator(new ObjectMapper()));
    }

    @AfterEach
    void tearDown() throws SQLException {
        connection.close();
    }

    @Test
    @DisplayName("grava o pedido e a linha de outbox no mesmo commit")
    void persistsOrderAndOutboxTogether() {
        Order order = anOrder();

        repository.save(order, order.pullDomainEvents());

        assertThat(repository.findById(order.id())).get()
                .satisfies(loaded -> assertThat(loaded.status()).isEqualTo(OrderStatus.PENDING_PAYMENT));
        assertThat(repository.pending(10)).singleElement().satisfies(record -> {
            assertThat(record.topic()).isEqualTo("orders");
            assertThat(record.eventType()).isEqualTo("OrderPlaced");
            assertThat(record.partitionKey()).isEqualTo("cust-1");
            assertThat(record.payload()).contains(order.id().toString());
        });
    }

    @Test
    @DisplayName("evento sem tradução aborta a transação inteira: nem pedido, nem outbox")
    void untranslatableEventRollsEverythingBack() {
        SqliteOrderRepository failing = new SqliteOrderRepository(connection, event -> {
            throw new IllegalArgumentException("sem tradução");
        });
        Order order = anOrder();

        assertThatThrownBy(() -> failing.save(order, order.pullDomainEvents()))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(failing.findById(order.id())).isEmpty();
        assertThat(failing.pending(10)).isEmpty();
    }

    @Test
    @DisplayName("a mudança de status não reenfileira o pedido no outbox")
    void statusUpdateDoesNotDuplicateOutbox() {
        Order order = anOrder();
        repository.save(order, order.pullDomainEvents());

        Order loaded = repository.findById(order.id()).orElseThrow();
        loaded.approvePayment();
        repository.save(loaded);

        assertThat(repository.findById(order.id()).orElseThrow().status()).isEqualTo(OrderStatus.PAID);
        assertThat(repository.pending(10)).hasSize(1);
    }

    @Test
    @DisplayName("linha confirmada some do pendente e não volta")
    void publishedRowIsNotRedelivered() {
        Order order = anOrder();
        repository.save(order, order.pullDomainEvents());
        List<OutboxRecord> pending = repository.pending(10);

        repository.markPublished(pending.getFirst().sequence());

        assertThat(repository.pending(10)).isEmpty();
    }

    private static Order anOrder() {
        return Order.place("cust-1", "Teclado", 2, new BigDecimal("199.90"), NOW);
    }
}
