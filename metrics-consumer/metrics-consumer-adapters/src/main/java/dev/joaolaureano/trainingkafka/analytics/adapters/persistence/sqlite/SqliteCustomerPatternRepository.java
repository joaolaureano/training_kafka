package dev.joaolaureano.trainingkafka.analytics.adapters.persistence.sqlite;

import dev.joaolaureano.trainingkafka.analytics.adapters.persistence.MoneyCents;
import dev.joaolaureano.trainingkafka.analytics.domain.model.CustomerId;
import dev.joaolaureano.trainingkafka.analytics.domain.model.CustomerOrderPattern;
import dev.joaolaureano.trainingkafka.analytics.domain.model.OrderId;
import dev.joaolaureano.trainingkafka.analytics.domain.model.OrderPlacement;
import dev.joaolaureano.trainingkafka.analytics.domain.model.SuspicionPolicy;
import dev.joaolaureano.trainingkafka.analytics.domain.port.CustomerPatternRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementação SQLite (JDBC puro) de {@link CustomerPatternRepository}.
 *
 * A tabela guarda só a janela de pedidos recentes; a policy nunca é persistida —
 * ela chega sempre como parâmetro de {@link #findOrCreate}, conforme o contrato.
 */
public class SqliteCustomerPatternRepository implements CustomerPatternRepository {

    private static final Logger LOG = LoggerFactory.getLogger(SqliteCustomerPatternRepository.class);

    private static final String CREATE_TABLE = """
            CREATE TABLE IF NOT EXISTS customer_placements (
                customer_id        VARCHAR NOT NULL,
                order_id           VARCHAR NOT NULL,
                occurred_at_millis BIGINT  NOT NULL,
                amount_cents       BIGINT  NOT NULL,
                PRIMARY KEY (customer_id, order_id)
            )
            """;

    private static final String SELECT_BY_CUSTOMER = """
            SELECT order_id, occurred_at_millis, amount_cents
            FROM customer_placements
            WHERE customer_id = ?
            """;

    private static final String DELETE_BY_CUSTOMER = """
            DELETE FROM customer_placements WHERE customer_id = ?
            """;

    private static final String INSERT_PLACEMENT = """
            INSERT INTO customer_placements (customer_id, order_id, occurred_at_millis, amount_cents)
            VALUES (?, ?, ?, ?)
            """;

    private final Connection connection;

    public SqliteCustomerPatternRepository(Connection connection) {
        this.connection = connection;
        createTableIfMissing();
    }

    private void createTableIfMissing() {
        try (Statement statement = connection.createStatement()) {
            statement.execute(CREATE_TABLE);
        } catch (SQLException e) {
            throw new SqliteRepositoryException("criar a tabela customer_placements", e);
        }
    }

    @Override
    public CustomerOrderPattern findOrCreate(CustomerId customerId, SuspicionPolicy policy) {
        try (PreparedStatement statement = connection.prepareStatement(SELECT_BY_CUSTOMER)) {
            statement.setString(1, customerId.value());
            List<OrderPlacement> placements = new ArrayList<>();
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    placements.add(new OrderPlacement(
                            OrderId.of(rs.getString("order_id")),
                            Instant.ofEpochMilli(rs.getLong("occurred_at_millis")),
                            MoneyCents.fromCents(rs.getLong("amount_cents"))));
                }
            }
            if (placements.isEmpty()) {
                return CustomerOrderPattern.startFor(customerId, policy);
            }
            return CustomerOrderPattern.reconstitute(customerId, policy, placements);
        } catch (SQLException e) {
            throw new SqliteRepositoryException("buscar customer_placements de " + customerId, e);
        }
    }

    /**
     * Apaga toda a janela do cliente e reinsere a lista atual, garantindo que a
     * tabela fique idêntica a {@code pattern.recentOrders()}. A janela é podada
     * pelo próprio agregado e é pequena por construção, então isso é barato.
     */
    @Override
    public void save(CustomerOrderPattern pattern) {
        String customerId = pattern.customerId().value();
        boolean previousAutoCommit = true;
        try {
            // A janela é substituída inteira: apaga tudo do cliente e reinsere a
            // lista já podada pelo agregado. As duas operações PRECISAM ser uma só
            // — se o processo morresse entre o DELETE e o INSERT com autocommit
            // ligado, o cliente ficaria sem histórico nenhum, e um surto em curso
            // deixaria de ser detectado justamente por ter apagado a evidência.
            previousAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);

            try (PreparedStatement delete = connection.prepareStatement(DELETE_BY_CUSTOMER)) {
                delete.setString(1, customerId);
                delete.executeUpdate();
            }
            try (PreparedStatement insert = connection.prepareStatement(INSERT_PLACEMENT)) {
                for (OrderPlacement placement : pattern.recentOrders()) {
                    insert.setString(1, customerId);
                    insert.setString(2, placement.orderId().value().toString());
                    insert.setLong(3, placement.occurredAt().toEpochMilli());
                    insert.setLong(4, MoneyCents.toCents(placement.amount()));
                    insert.addBatch();
                }
                insert.executeBatch();
            }

            connection.commit();
        } catch (SQLException e) {
            rollbackQuietly(customerId);
            throw new SqliteRepositoryException("salvar customer_placements de " + customerId, e);
        } finally {
            restoreAutoCommit(previousAutoCommit);
        }
    }

    private void rollbackQuietly(String customerId) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            LOG.warn("Rollback falhou ao salvar a janela de {}: {}", customerId, rollbackFailure.getMessage());
        }
    }

    private void restoreAutoCommit(boolean previousAutoCommit) {
        try {
            connection.setAutoCommit(previousAutoCommit);
        } catch (SQLException restoreFailure) {
            LOG.warn("Não foi possível restaurar o autocommit: {}", restoreFailure.getMessage());
        }
    }
}
