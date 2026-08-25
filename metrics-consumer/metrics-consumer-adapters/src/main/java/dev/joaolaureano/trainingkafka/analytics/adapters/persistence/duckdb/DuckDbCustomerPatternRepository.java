package dev.joaolaureano.trainingkafka.analytics.adapters.persistence.duckdb;

import dev.joaolaureano.trainingkafka.analytics.adapters.persistence.MoneyCents;
import dev.joaolaureano.trainingkafka.analytics.domain.model.CustomerId;
import dev.joaolaureano.trainingkafka.analytics.domain.model.CustomerOrderPattern;
import dev.joaolaureano.trainingkafka.analytics.domain.model.OrderId;
import dev.joaolaureano.trainingkafka.analytics.domain.model.OrderPlacement;
import dev.joaolaureano.trainingkafka.analytics.domain.model.SuspicionPolicy;
import dev.joaolaureano.trainingkafka.analytics.domain.port.CustomerPatternRepository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementação DuckDB (JDBC puro) de {@link CustomerPatternRepository}.
 *
 * A conexão é injetada e permanece de responsabilidade de quem a criou.
 */
public class DuckDbCustomerPatternRepository implements CustomerPatternRepository {

    private static final String CREATE_TABLE = """
            CREATE TABLE IF NOT EXISTS customer_placements (
                customer_id        VARCHAR NOT NULL,
                order_id           VARCHAR NOT NULL,
                occurred_at_millis BIGINT  NOT NULL,
                amount_cents       BIGINT  NOT NULL,
                PRIMARY KEY (customer_id, order_id)
            )
            """;

    private final Connection connection;

    public DuckDbCustomerPatternRepository(Connection connection) {
        this.connection = connection;
        try (Statement statement = connection.createStatement()) {
            statement.execute(CREATE_TABLE);
        } catch (SQLException e) {
            throw new RuntimeException("falha ao criar a tabela customer_placements no DuckDB", e);
        }
    }

    @Override
    public CustomerOrderPattern findOrCreate(CustomerId customerId, SuspicionPolicy policy) {
        String sql = "SELECT order_id, occurred_at_millis, amount_cents FROM customer_placements "
                + "WHERE customer_id = ? ORDER BY occurred_at_millis";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, customerId.value());
            try (ResultSet rs = statement.executeQuery()) {
                List<OrderPlacement> recentOrders = new ArrayList<>();
                while (rs.next()) {
                    recentOrders.add(new OrderPlacement(
                            OrderId.of(rs.getString("order_id")),
                            Instant.ofEpochMilli(rs.getLong("occurred_at_millis")),
                            MoneyCents.fromCents(rs.getLong("amount_cents"))));
                }
                if (recentOrders.isEmpty()) {
                    return CustomerOrderPattern.startFor(customerId, policy);
                }
                // A policy nunca é lida do banco: vem sempre do parâmetro recebido.
                return CustomerOrderPattern.reconstitute(customerId, policy, recentOrders);
            }
        } catch (SQLException e) {
            throw new RuntimeException("falha ao buscar customer_placements no DuckDB para " + customerId, e);
        }
    }

    @Override
    public void save(CustomerOrderPattern pattern) {
        // O agregado já se poda sozinho (recentOrders() é a janela final e imutável).
        // Persistir "a janela persistida = pattern.recentOrders()" fica mais simples e
        // correto apagando todas as linhas do cliente e reinserindo a lista atual do
        // que tentando calcular um diff — a janela é pequena por construção, então
        // isso é barato. As duas operações vão na mesma transação para não deixar o
        // cliente momentaneamente sem histórico em caso de falha no meio do caminho.
        String delete = "DELETE FROM customer_placements WHERE customer_id = ?";
        String insert = "INSERT INTO customer_placements "
                + "(customer_id, order_id, occurred_at_millis, amount_cents) VALUES (?, ?, ?, ?)";
        try {
            connection.setAutoCommit(false);
            try (PreparedStatement deleteStatement = connection.prepareStatement(delete)) {
                deleteStatement.setString(1, pattern.customerId().value());
                deleteStatement.executeUpdate();
            }
            try (PreparedStatement insertStatement = connection.prepareStatement(insert)) {
                for (OrderPlacement placement : pattern.recentOrders()) {
                    insertStatement.setString(1, pattern.customerId().value());
                    insertStatement.setString(2, placement.orderId().toString());
                    insertStatement.setLong(3, placement.occurredAt().toEpochMilli());
                    insertStatement.setLong(4, MoneyCents.toCents(placement.amount()));
                    insertStatement.addBatch();
                }
                insertStatement.executeBatch();
            }
            connection.commit();
        } catch (SQLException e) {
            rollbackQuietly();
            throw new RuntimeException("falha ao salvar customer_placements no DuckDB para "
                    + pattern.customerId(), e);
        } finally {
            restoreAutoCommitQuietly();
        }
    }

    private void rollbackQuietly() {
        try {
            connection.rollback();
        } catch (SQLException ignored) {
            // já estamos tratando uma falha anterior; nada mais a fazer aqui.
        }
    }

    private void restoreAutoCommitQuietly() {
        try {
            connection.setAutoCommit(true);
        } catch (SQLException ignored) {
            // conexão possivelmente já inutilizável; a exceção original prevalece.
        }
    }
}
