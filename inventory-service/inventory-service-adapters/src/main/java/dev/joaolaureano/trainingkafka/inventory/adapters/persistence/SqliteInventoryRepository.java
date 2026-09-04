package dev.joaolaureano.trainingkafka.inventory.adapters.persistence;

import dev.joaolaureano.trainingkafka.inventory.domain.event.DomainEvent;
import dev.joaolaureano.trainingkafka.inventory.domain.model.Product;
import dev.joaolaureano.trainingkafka.inventory.domain.model.Quantity;
import dev.joaolaureano.trainingkafka.inventory.domain.model.Reservation;
import dev.joaolaureano.trainingkafka.inventory.domain.model.ReservationStatus;
import dev.joaolaureano.trainingkafka.inventory.domain.model.Sku;
import dev.joaolaureano.trainingkafka.inventory.domain.port.ConcurrentStockChangeException;
import dev.joaolaureano.trainingkafka.inventory.domain.port.InventoryRepository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * O estoque, as reservas e o outbox, no mesmo arquivo SQLite e na mesma transação.
 *
 * Descontar unidades, registrar a reserva e enfileirar o evento é UM commit. Ou os
 * três acontecem, ou nenhum — que é o que impede estoque descontado para um pedido
 * que ninguém jamais vai cobrar, e cobrança sobre uma reserva que não existe.
 *
 * <p><b>O UPDATE condicionado à versão é a peça central deste arquivo.</b> Dois
 * pedidos pela última unidade chegam por partições diferentes de {@code orders} —
 * a chave lá é o customerId, não o produto — e portanto são decididos por threads
 * diferentes, ao mesmo tempo, sobre a mesma leitura. Os dois agregados dizem "dá
 * para reservar", porque nenhum dos dois enxerga o outro. Quem desempata é o
 * {@code where version = ?}: a segunda gravação não encontra a linha na versão em
 * que a leu, atualiza zero linhas, e vira
 * {@link ConcurrentStockChangeException} para o caso de uso reler e decidir de novo.
 *
 * <p>Nenhuma escolha de chave de tópico resolveria isso sozinha — a atomicidade
 * tem que estar onde o estado mora.
 *
 * <p>Os métodos públicos são sincronizados: a {@code Connection} JDBC é única e não
 * é thread-safe, e o relay do outbox roda em paralelo com os listeners.
 */
public final class SqliteInventoryRepository implements InventoryRepository, OutboxStore {

    private final Connection connection;
    private final OutboxTranslator translator;

    public SqliteInventoryRepository(Connection connection, OutboxTranslator translator) {
        this.connection = connection;
        this.translator = translator;
        initialize();
    }

    @Override
    public synchronized Optional<Product> findBySku(Sku sku) {
        try (PreparedStatement statement = connection.prepareStatement(
                "select sku, name, available, version from products where sku = ?")) {
            statement.setString(1, sku.value());
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(readProduct(result)) : Optional.empty();
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("Could not load product " + sku, failure);
        }
    }

    @Override
    public synchronized List<Product> findAll() {
        try (PreparedStatement statement = connection.prepareStatement(
                "select sku, name, available, version from products order by sku")) {
            try (ResultSet result = statement.executeQuery()) {
                List<Product> products = new ArrayList<>();
                while (result.next()) {
                    products.add(readProduct(result));
                }
                return products;
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("Could not list products", failure);
        }
    }

    @Override
    public synchronized Optional<Reservation> findReservation(String orderId) {
        try (PreparedStatement statement = connection.prepareStatement("""
                select order_id, sku, quantity, customer_id, amount, correlation_id,
                       status, decided_at, reason
                from reservations where order_id = ?
                """)) {
            statement.setString(1, orderId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return Optional.empty();
                }
                String sku = result.getString("sku");
                return Optional.of(Reservation.reconstitute(
                        result.getString("order_id"),
                        sku == null ? null : Sku.of(sku),
                        new Quantity(result.getInt("quantity")),
                        result.getString("customer_id"),
                        result.getBigDecimal("amount"),
                        result.getString("correlation_id"),
                        ReservationStatus.valueOf(result.getString("status")),
                        instantOrNull(result.getString("decided_at")),
                        result.getString("reason")));
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("Could not load reservation for order " + orderId, failure);
        }
    }

    /**
     * Upsert do catálogo, sem condição de versão.
     *
     * É uma operação administrativa e absoluta: quem ajusta o inventário está
     * declarando a verdade, e ganha de qualquer decisão em voo. A versão é
     * incrementada assim mesmo — é o que faz uma reserva concorrente perceber que
     * leu um estado velho e reler, em vez de sobrescrever o ajuste.
     */
    @Override
    public synchronized void saveProduct(Product product) {
        try (PreparedStatement statement = connection.prepareStatement("""
                insert into products(sku, name, available, version) values (?, ?, ?, 1)
                on conflict(sku) do update set
                  name = excluded.name,
                  available = excluded.available,
                  version = products.version + 1
                """)) {
            statement.setString(1, product.sku().value());
            statement.setString(2, product.name());
            statement.setInt(3, product.available().value());
            statement.executeUpdate();
            connection.commit();
        } catch (SQLException failure) {
            rollbackQuietly();
            throw new IllegalStateException("Could not save product " + product.sku(), failure);
        }
    }

    @Override
    public synchronized void save(Reservation reservation, Product product, List<DomainEvent> events) {
        try {
            writeProductAtVersion(product);
            writeReservation(reservation);
            for (DomainEvent event : events) {
                writeOutbox(translator.translate(event));
            }
            connection.commit();
        } catch (SQLException failure) {
            rollbackQuietly();
            throw new IllegalStateException("Could not save reservation " + reservation.orderId(), failure);
        } catch (RuntimeException failure) {
            rollbackQuietly();
            throw failure;
        }
    }

    @Override
    public synchronized void save(Reservation reservation, List<DomainEvent> events) {
        try {
            writeReservation(reservation);
            for (DomainEvent event : events) {
                writeOutbox(translator.translate(event));
            }
            connection.commit();
        } catch (SQLException failure) {
            rollbackQuietly();
            throw new IllegalStateException("Could not save reservation " + reservation.orderId(), failure);
        } catch (RuntimeException failure) {
            rollbackQuietly();
            throw failure;
        }
    }

    @Override
    public synchronized List<OutboxRecord> pending(int limit) {
        try (PreparedStatement statement = connection.prepareStatement("""
                select seq, topic, partition_key, event_type, payload
                from inventory_outbox where published_at is null order by seq limit ?
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
                "update inventory_outbox set published_at = ? where seq = ?")) {
            statement.setString(1, Instant.now().toString());
            statement.setLong(2, sequence);
            statement.executeUpdate();
            connection.commit();
        } catch (SQLException failure) {
            rollbackQuietly();
            throw new IllegalStateException("Could not mark outbox row " + sequence, failure);
        }
    }

    /**
     * A gravação condicionada: só altera a linha se ela ainda estiver na versão em
     * que foi lida. Zero linhas afetadas significa que outra decisão passou na
     * frente — e a nossa foi tomada sobre um estoque que não existe mais.
     */
    private void writeProductAtVersion(Product product) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                update products set available = ?, version = version + 1
                where sku = ? and version = ?
                """)) {
            statement.setInt(1, product.available().value());
            statement.setString(2, product.sku().value());
            statement.setLong(3, product.version());
            if (statement.executeUpdate() == 0) {
                throw new ConcurrentStockChangeException(
                        "produto " + product.sku() + " mudou desde a leitura (versão "
                                + product.version() + ")");
            }
        }
    }

    /**
     * Insere a reserva, ou atualiza o desfecho de uma que já existe.
     *
     * O {@code on conflict} serve à liberação, que é a única transição legítima de
     * uma reserva já gravada (HELD para RELEASED) — os campos imutáveis ficam de
     * fora do update de propósito: sku, quantidade e cliente de uma reserva não
     * mudam, e deixá-los fora faz o banco recusar a corrupção em vez de aceitá-la.
     *
     * O que ele NÃO faz é abrir espaço para uma segunda reserva do mesmo pedido:
     * quem impede isso é a guarda de idempotência no caso de uso, que consulta
     * antes de decidir. Chegar aqui duas vezes com um StockReserved exigiria duas
     * threads decidindo o mesmo pedido ao mesmo tempo, e não há como: a chave de
     * {@code orders} é o customerId, então todos os pedidos de um cliente caem na
     * mesma partição e são consumidos por uma thread só.
     */
    private void writeReservation(Reservation reservation) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                insert into reservations(order_id, sku, quantity, customer_id, amount,
                                         correlation_id, status, decided_at, reason)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                on conflict(order_id) do update set
                  status = excluded.status,
                  decided_at = excluded.decided_at,
                  reason = excluded.reason
                """)) {
            statement.setString(1, reservation.orderId());
            statement.setString(2, reservation.sku() == null ? null : reservation.sku().value());
            statement.setInt(3, reservation.quantity().value());
            statement.setString(4, reservation.customerId());
            statement.setBigDecimal(5, reservation.amount());
            statement.setString(6, reservation.correlationId());
            statement.setString(7, reservation.status().name());
            statement.setString(8, reservation.decidedAt() == null ? null
                    : reservation.decidedAt().toString());
            statement.setString(9, reservation.reason());
            statement.executeUpdate();
        }
    }

    private void writeOutbox(OutboxRecord record) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                insert into inventory_outbox(topic, partition_key, event_type, payload, created_at)
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

    private static Product readProduct(ResultSet result) throws SQLException {
        return Product.reconstitute(
                Sku.of(result.getString("sku")),
                result.getString("name"),
                new Quantity(result.getInt("available")),
                result.getLong("version"));
    }

    private void initialize() {
        try (var statement = connection.createStatement()) {
            statement.executeUpdate("""
                    create table if not exists products(
                      sku text primary key,
                      name text not null,
                      available integer not null,
                      version integer not null default 0
                    )
                    """);
            statement.executeUpdate("""
                    create table if not exists reservations(
                      order_id text primary key,
                      -- Nulo só na lápide (VOIDED): quem a grava é o desfecho do
                      -- pagamento, que não conhece SKU nem quantidade.
                      sku text,
                      quantity integer not null,
                      customer_id text,
                      amount decimal,
                      correlation_id text,
                      status text not null,
                      decided_at text,
                      reason text
                    )
                    """);
            statement.executeUpdate("""
                    create table if not exists inventory_outbox(
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
                    "create index if not exists idx_inventory_outbox_pending "
                            + "on inventory_outbox(published_at, seq)");
            connection.commit();
        } catch (SQLException failure) {
            throw new IllegalStateException("Could not initialize the inventory database", failure);
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
