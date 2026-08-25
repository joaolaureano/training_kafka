package dev.joaolaureano.trainingkafka.analytics.adapters.persistence.duckdb;

import dev.joaolaureano.trainingkafka.analytics.adapters.persistence.MoneyCents;
import dev.joaolaureano.trainingkafka.analytics.domain.model.OrderRecord;
import dev.joaolaureano.trainingkafka.analytics.domain.model.ProductId;
import dev.joaolaureano.trainingkafka.analytics.domain.model.RevenueWindow;
import dev.joaolaureano.trainingkafka.analytics.domain.model.TimeRange;
import dev.joaolaureano.trainingkafka.analytics.domain.port.OrderLedgerRepository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Implementação DuckDB (JDBC puro) de {@link OrderLedgerRepository}.
 *
 * A conexão é injetada e permanece de responsabilidade de quem a criou.
 */
public class DuckDbOrderLedgerRepository implements OrderLedgerRepository {

    private static final String CREATE_TABLE = """
            CREATE TABLE IF NOT EXISTS order_ledger (
                order_id         VARCHAR PRIMARY KEY,
                customer_id      VARCHAR NOT NULL,
                product_id       VARCHAR NOT NULL,
                quantity         INTEGER NOT NULL,
                amount_cents     BIGINT  NOT NULL,
                placed_at_millis BIGINT  NOT NULL
            )
            """;

    private final Connection connection;

    public DuckDbOrderLedgerRepository(Connection connection) {
        this.connection = connection;
        try (Statement statement = connection.createStatement()) {
            statement.execute(CREATE_TABLE);
        } catch (SQLException e) {
            throw new RuntimeException("falha ao criar a tabela order_ledger no DuckDB", e);
        }
    }

    @Override
    public void append(OrderRecord record) {
        // Ledger append-only: nunca há upsert aqui, só INSERT.
        String sql = "INSERT INTO order_ledger "
                + "(order_id, customer_id, product_id, quantity, amount_cents, placed_at_millis) "
                + "VALUES (?, ?, ?, ?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, record.orderId().toString());
            statement.setString(2, record.customerId().value());
            statement.setString(3, record.productId().value());
            statement.setInt(4, record.quantity().value());
            statement.setLong(5, MoneyCents.toCents(record.amount()));
            statement.setLong(6, record.placedAt().toEpochMilli());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("falha ao gravar no order_ledger do DuckDB para " + record.orderId(), e);
        }
    }

    @Override
    public RevenueWindow revenueOver(TimeRange range) {
        String sql = "SELECT SUM(amount_cents) AS total_cents, COUNT(*) AS order_count "
                + "FROM order_ledger WHERE placed_at_millis >= ? AND placed_at_millis <= ?";
        return queryRevenue(sql, range, null);
    }

    @Override
    public RevenueWindow revenueOver(TimeRange range, ProductId productId) {
        String sql = "SELECT SUM(amount_cents) AS total_cents, COUNT(*) AS order_count "
                + "FROM order_ledger WHERE placed_at_millis >= ? AND placed_at_millis <= ? AND product_id = ?";
        return queryRevenue(sql, range, productId);
    }

    private RevenueWindow queryRevenue(String sql, TimeRange range, ProductId productId) {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, range.start().toEpochMilli());
            statement.setLong(2, range.end().toEpochMilli());
            if (productId != null) {
                statement.setString(3, productId.value());
            }
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    return RevenueWindow.empty(range);
                }
                long totalCents = rs.getLong("total_cents");
                // SUM() devolve NULL quando não há nenhuma linha; getLong já traria 0
                // nesse caso, mas checamos wasNull() explicitamente por clareza.
                if (rs.wasNull()) {
                    return RevenueWindow.empty(range);
                }
                long orderCount = rs.getLong("order_count");
                return new RevenueWindow(range, MoneyCents.fromCents(totalCents), orderCount);
            }
        } catch (SQLException e) {
            throw new RuntimeException("falha ao calcular faturamento no DuckDB para " + range, e);
        }
    }
}
