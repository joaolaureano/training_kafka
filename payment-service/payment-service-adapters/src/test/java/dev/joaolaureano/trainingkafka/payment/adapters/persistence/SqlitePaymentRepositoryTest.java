package dev.joaolaureano.trainingkafka.payment.adapters.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.joaolaureano.trainingkafka.payment.adapters.messaging.PaymentEventOutboxTranslator;
import dev.joaolaureano.trainingkafka.payment.domain.model.Payment;
import dev.joaolaureano.trainingkafka.payment.domain.model.PaymentStatus;
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
 * SQLite de verdade, em memória: o que se testa aqui é o SQL — a atomicidade
 * entre o desfecho e o outbox não existiria num fake.
 */
class SqlitePaymentRepositoryTest {

    private static final Instant NOW = Instant.parse("2026-09-03T12:00:00Z");

    private Connection connection;
    private SqlitePaymentRepository repository;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:sqlite::memory:");
        connection.setAutoCommit(false);
        repository = new SqlitePaymentRepository(connection,
                new PaymentEventOutboxTranslator(new ObjectMapper()));
    }

    @AfterEach
    void tearDown() throws SQLException {
        connection.close();
    }

    @Test
    @DisplayName("o desfecho e o evento entram no mesmo commit")
    void persistsOutcomeAndOutboxTogether() {
        Payment payment = Payment.request("order-1", "cust-1", new BigDecimal("42.00"));
        payment.fail(NOW, "limite excedido", "corr-1");

        repository.save(payment, payment.pullDomainEvents());

        Payment loaded = repository.findByOrderId("order-1").orElseThrow();
        assertThat(loaded.status()).isEqualTo(PaymentStatus.FAILED);
        assertThat(loaded.resolvedAt()).isEqualTo(NOW);
        assertThat(loaded.reason()).isEqualTo("limite excedido");
        assertThat(loaded.correlationId()).isEqualTo("corr-1");

        assertThat(repository.pending(10)).singleElement().satisfies(record -> {
            assertThat(record.topic()).isEqualTo("payment-events");
            assertThat(record.eventType()).isEqualTo("PaymentFailed");
            // A chave é o orderId: os resultados de um mesmo pedido chegam em ordem.
            assertThat(record.partitionKey()).isEqualTo("order-1");
            assertThat(record.payload()).contains("limite excedido");
        });
    }

    @Test
    @DisplayName("o estorno registra que houve estorno, e não só o cancelamento")
    void refundIsRecordedAsSuch() {
        Payment payment = Payment.request("order-1", "cust-1", new BigDecimal("42.00"));
        payment.approve(NOW, "corr-1");
        repository.save(payment, payment.pullDomainEvents());

        payment.cancelForFraud(NOW, "fraud", "corr-f");
        repository.save(payment, payment.pullDomainEvents());

        assertThat(repository.findByOrderId("order-1").orElseThrow().refunded()).isTrue();
        assertThat(repository.pending(10)).hasSize(2)
                .extracting(OutboxRecord::eventType)
                .containsExactly("PaymentApproved", "PaymentCancelled");
    }

    @Test
    @DisplayName("evento sem tradução aborta tudo: nem pagamento, nem outbox")
    void untranslatableEventRollsEverythingBack() {
        SqlitePaymentRepository failing = new SqlitePaymentRepository(connection, event -> {
            throw new IllegalArgumentException("sem tradução");
        });
        Payment payment = Payment.request("order-1", "cust-1", new BigDecimal("42.00"));
        payment.approve(NOW, "corr-1");

        assertThatThrownBy(() -> failing.save(payment, payment.pullDomainEvents()))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(failing.findByOrderId("order-1")).isEmpty();
        assertThat(failing.pending(10)).isEmpty();
    }

    @Test
    @DisplayName("linha confirmada some do pendente e não volta")
    void publishedRowIsNotRedelivered() {
        Payment payment = Payment.request("order-1", "cust-1", new BigDecimal("42.00"));
        payment.approve(NOW, "corr-1");
        repository.save(payment, payment.pullDomainEvents());
        List<OutboxRecord> pending = repository.pending(10);

        repository.markPublished(pending.getFirst().sequence());

        assertThat(repository.pending(10)).isEmpty();
    }

    @Test
    @DisplayName("um orderId tem no máximo um pagamento")
    void onePaymentPerOrder() {
        Payment payment = Payment.request("order-1", "cust-1", new BigDecimal("42.00"));
        repository.save(payment);
        payment.approve(NOW, "corr-1");
        repository.save(payment, payment.pullDomainEvents());

        assertThat(repository.findByOrderId("order-1").orElseThrow().status())
                .isEqualTo(PaymentStatus.APPROVED);
    }
}
