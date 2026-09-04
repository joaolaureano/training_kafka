package dev.joaolaureano.trainingkafka.orders.adapters.persistence;

import dev.joaolaureano.trainingkafka.orders.domain.event.DomainEvent;
import dev.joaolaureano.trainingkafka.orders.domain.model.CustomerId;
import dev.joaolaureano.trainingkafka.orders.domain.model.Money;
import dev.joaolaureano.trainingkafka.orders.domain.model.Order;
import dev.joaolaureano.trainingkafka.orders.domain.model.OrderId;
import dev.joaolaureano.trainingkafka.orders.domain.model.OrderStatus;
import dev.joaolaureano.trainingkafka.orders.domain.model.ProductId;
import dev.joaolaureano.trainingkafka.orders.domain.model.Quantity;
import dev.joaolaureano.trainingkafka.orders.domain.port.OrderRepository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * O estado do pedido e o outbox, no mesmo arquivo SQLite e na mesma transação.
 *
 * É essa co-localização que dá sentido ao outbox: gravar o pedido e registrar o
 * OrderPlaced é UM commit. Ou os dois acontecem, ou nenhum. Um banco separado
 * para o outbox reintroduziria exatamente o problema de dois recursos que o
 * padrão existe para eliminar.
 *
 * A conexão vem com {@code autoCommit=false} e é única no processo — o commit é
 * explícito e delimita a unidade de trabalho. Como uma {@code Connection} JDBC não
 * é thread-safe e o relay do outbox roda em paralelo com as threads HTTP, os
 * métodos públicos são sincronizados: sem isso, um commit do relay poderia
 * confirmar a transação pela metade de outra thread.
 */
public final class SqliteOrderRepository implements OrderRepository, OutboxStore {

    private final Connection connection;
    private final OutboxTranslator translator;

    public SqliteOrderRepository(Connection connection, OutboxTranslator translator) {
        this.connection = connection;
        this.translator = translator;
        initialize();
    }

    @Override
    public synchronized Optional<Order> findById(OrderId orderId) {
        try (PreparedStatement statement = connection.prepareStatement("""
                select order_id, customer_id, product_id, quantity, amount, placed_at, status
                from orders where order_id = ?
                """)) {
            statement.setString(1, orderId.toString());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return Optional.empty();
                }
                return Optional.of(Order.reconstitute(
                        OrderId.parse(result.getString("order_id")),
                        new CustomerId(result.getString("customer_id")),
                        new ProductId(result.getString("product_id")),
                        new Quantity(result.getInt("quantity")),
                        new Money(result.getBigDecimal("amount")),
                        Instant.parse(result.getString("placed_at")),
                        OrderStatus.valueOf(result.getString("status"))));
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("Could not load order " + orderId, failure);
        }
    }

    @Override
    public synchronized void save(Order order, List<DomainEvent> events) {
        try {
            writeOrder(order);
            for (DomainEvent event : events) {
                writeOutbox(translator.translate(event));
            }
            connection.commit();
        } catch (SQLException failure) {
            rollbackQuietly();
            throw new IllegalStateException("Could not save order " + order.id(), failure);
        } catch (RuntimeException failure) {
            rollbackQuietly();
            throw failure;
        }
    }

    @Override
    public void save(Order order) {
        save(order, List.of());
    }

    /**
     * A leitura-escrita atômica: {@code synchronized} sobre a mesma instância que
     * todo mundo usa, então os dois escritores do pedido — estoque e pagamento —
     * nunca se atropelam. Ver o javadoc no Port para o porquê.
     */
    @Override
    public synchronized boolean applyTransition(OrderId orderId, Consumer<Order> transition) {
        Optional<Order> found = findById(orderId);
        if (found.isEmpty()) {
            return false;
        }
        Order order = found.get();
        transition.accept(order);
        save(order);
        return true;
    }

    @Override
    public synchronized List<OutboxRecord> pending(int limit) {
        try (PreparedStatement statement = connection.prepareStatement("""
                select seq, topic, partition_key, event_type, payload
                from order_outbox where published_at is null order by seq limit ?
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
                "update order_outbox set published_at = ? where seq = ?")) {
            statement.setString(1, Instant.now().toString());
            statement.setLong(2, sequence);
            statement.executeUpdate();
            connection.commit();
        } catch (SQLException failure) {
            rollbackQuietly();
            throw new IllegalStateException("Could not mark outbox row " + sequence, failure);
        }
    }

    private void writeOrder(Order order) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                insert into orders(order_id, customer_id, product_id, quantity, amount, placed_at, status)
                values (?, ?, ?, ?, ?, ?, ?)
                on conflict(order_id) do update set status = excluded.status
                """)) {
            statement.setString(1, order.id().toString());
            statement.setString(2, order.customerId().value());
            statement.setString(3, order.productId().value());
            statement.setInt(4, order.quantity().value());
            statement.setBigDecimal(5, order.amount().amount());
            statement.setString(6, order.placedAt().toString());
            statement.setString(7, order.status().name());
            statement.executeUpdate();
        }
    }

    private void writeOutbox(OutboxRecord record) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                insert into order_outbox(topic, partition_key, event_type, payload, created_at)
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
                    create table if not exists orders(
                      order_id text primary key,
                      customer_id text not null,
                      product_id text not null,
                      quantity integer not null,
                      amount decimal not null,
                      placed_at text not null,
                      status text not null
                    )
                    """);
            statement.executeUpdate("""
                    create table if not exists order_outbox(
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
                    "create index if not exists idx_outbox_pending on order_outbox(published_at, seq)");
            connection.commit();
        } catch (SQLException failure) {
            throw new IllegalStateException("Could not initialize the order database", failure);
        }
    }

    private void rollbackQuietly() {
        try {
            connection.rollback();
        } catch (SQLException ignored) {
            // Preserva a falha original.
        }
    }
}
