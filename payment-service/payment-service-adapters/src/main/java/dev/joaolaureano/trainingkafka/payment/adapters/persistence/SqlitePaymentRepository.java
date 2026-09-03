package dev.joaolaureano.trainingkafka.payment.adapters.persistence;

import dev.joaolaureano.trainingkafka.payment.domain.event.DomainEvent;
import dev.joaolaureano.trainingkafka.payment.domain.model.Payment;
import dev.joaolaureano.trainingkafka.payment.domain.model.PaymentId;
import dev.joaolaureano.trainingkafka.payment.domain.model.PaymentStatus;
import dev.joaolaureano.trainingkafka.payment.domain.port.PaymentRepository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * O estado do pagamento e o outbox, no mesmo arquivo SQLite e na mesma transação.
 *
 * Gravar o desfecho e registrar o evento é UM commit. Ou os dois acontecem, ou
 * nenhum — que é o que impede um pagamento estornado de existir sem que o pedido
 * jamais fique sabendo.
 *
 * Os métodos públicos são sincronizados: a {@code Connection} JDBC é única e não é
 * thread-safe, e o relay do outbox roda em paralelo com o listener.
 */
public final class SqlitePaymentRepository implements PaymentRepository, OutboxStore {

    private final Connection connection;
    private final OutboxTranslator translator;

    public SqlitePaymentRepository(Connection connection, OutboxTranslator translator) {
        this.connection = connection;
        this.translator = translator;
        initialize();
    }

    @Override
    public synchronized Optional<Payment> findByOrderId(String orderId) {
        try (PreparedStatement statement = connection.prepareStatement("""
                select payment_id, order_id, customer_id, amount, status, resolved_at, reason,
                       correlation_id, refunded
                from payments where order_id = ?
                """)) {
            statement.setString(1, orderId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return Optional.empty();
                }
                return Optional.of(Payment.reconstitute(
                        new PaymentId(UUID.fromString(result.getString("payment_id"))),
                        result.getString("order_id"), result.getString("customer_id"),
                        result.getBigDecimal("amount"),
                        PaymentStatus.valueOf(result.getString("status")),
                        instantOrNull(result.getString("resolved_at")), result.getString("reason"),
                        result.getString("correlation_id"), result.getBoolean("refunded")));
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("Could not load payment for order " + orderId, failure);
        }
    }

    @Override
    public synchronized void save(Payment payment, List<DomainEvent> events) {
        try {
            writePayment(payment);
            for (DomainEvent event : events) {
                writeOutbox(translator.translate(event));
            }
            connection.commit();
        } catch (SQLException failure) {
            rollbackQuietly();
            throw new IllegalStateException("Could not save payment " + payment.id(), failure);
        } catch (RuntimeException failure) {
            rollbackQuietly();
            throw failure;
        }
    }

    @Override
    public void save(Payment payment) {
        save(payment, List.of());
    }

    @Override
    public synchronized List<OutboxRecord> pending(int limit) {
        try (PreparedStatement statement = connection.prepareStatement("""
                select seq, topic, partition_key, event_type, payload
                from payment_outbox where published_at is null order by seq limit ?
                """)) {
            statement.setInt(1, limit);
            try (ResultSet result = statement.executeQuery()) {
                List<OutboxRecord> pending = new ArrayList<>();
                while (result.next()) {
                    pending.add(new OutboxRecord(result.getLong("seq"), result.getString("topic"),
                            result.getString("partition_key"), result.getString("event_type"),
                            result.getString("payload")));
                }
                return pending;
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("Could not read the outbox", failure);
        }
    }

    @Override
    public synchronized void markPublished(long sequence) {
        try (PreparedStatement statement = connection.prepareStatement(
                "update payment_outbox set published_at = ? where seq = ?")) {
            statement.setString(1, Instant.now().toString());
            statement.setLong(2, sequence);
            statement.executeUpdate();
            connection.commit();
        } catch (SQLException failure) {
            rollbackQuietly();
            throw new IllegalStateException("Could not mark outbox row " + sequence, failure);
        }
    }

    private void writePayment(Payment payment) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                insert into payments(payment_id, order_id, customer_id, amount, status,
                                     resolved_at, reason, correlation_id, refunded)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                on conflict(order_id) do update set
                  status = excluded.status,
                  resolved_at = excluded.resolved_at,
                  reason = excluded.reason,
                  correlation_id = excluded.correlation_id,
                  refunded = excluded.refunded
                """)) {
            statement.setString(1, payment.id().toString());
            statement.setString(2, payment.orderId());
            statement.setString(3, payment.customerId());
            statement.setBigDecimal(4, payment.amount());
            statement.setString(5, payment.status().name());
            statement.setString(6, payment.resolvedAt() == null ? null : payment.resolvedAt().toString());
            statement.setString(7, payment.reason());
            statement.setString(8, payment.correlationId());
            statement.setBoolean(9, payment.refunded());
            statement.executeUpdate();
        }
    }

    private void writeOutbox(OutboxRecord record) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                insert into payment_outbox(topic, partition_key, event_type, payload, created_at)
                values (?, ?, ?, ?, ?)
                """)) {
            statement.setString(1, record.topic());
            statement.setString(2, record.partitionKey());
            statement.setString(3, record.eventType());
            statement.setString(4, record.payload());
            statement.setString(5, Instant.now().toString());
            statement.executeUpdate();
        }
    }

    private void initialize() {
        try (var statement = connection.createStatement()) {
            statement.executeUpdate("""
                    create table if not exists payments(
                      payment_id text primary key,
                      order_id text not null unique,
                      customer_id text not null,
                      amount decimal not null,
                      status text not null,
                      resolved_at text,
                      reason text,
                      correlation_id text,
                      refunded integer not null default 0
                    )
                    """);
            statement.executeUpdate("""
                    create table if not exists payment_outbox(
                      seq integer primary key autoincrement,
                      topic text not null,
                      partition_key text not null,
                      event_type text not null,
                      payload text not null,
                      created_at text not null,
                      published_at text
                    )
                    """);
            statement.executeUpdate(
                    "create index if not exists idx_payment_outbox_pending on payment_outbox(published_at, seq)");
            connection.commit();
        } catch (SQLException failure) {
            throw new IllegalStateException("Could not initialize the payment database", failure);
        }
    }

    private static Instant instantOrNull(String value) {
        return value == null ? null : Instant.parse(value);
    }

    private void rollbackQuietly() {
        try {
            connection.rollback();
        } catch (SQLException ignored) {
            // Preserva a falha original.
        }
    }
}
