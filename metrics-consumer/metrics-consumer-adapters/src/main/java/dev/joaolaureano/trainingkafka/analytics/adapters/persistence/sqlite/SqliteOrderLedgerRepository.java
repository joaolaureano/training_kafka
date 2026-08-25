package dev.joaolaureano.trainingkafka.analytics.adapters.persistence.sqlite;

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
 * Implementação SQLite (JDBC puro) de {@link OrderLedgerRepository}.
 *
 * O ledger é append-only: não existe update nem delete aqui, só {@code INSERT}
 * e consultas agregadas por período.
 */
public class SqliteOrderLedgerRepository implements OrderLedgerRepository {

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

    private static final String INSERT = """
            INSERT INTO order_ledger (order_id, customer_id, product_id, quantity, amount_cents, placed_at_millis)
            VALUES (?, ?, ?, ?, ?, ?)
            """;

    private static final String REVENUE_OVER = """
            SELECT SUM(amount_cents) AS total_cents, COUNT(*) AS order_count
            FROM order_ledger
            WHERE placed_at_millis BETWEEN ? AND ?
            """;

    private static final String REVENUE_OVER_FOR_PRODUCT = """
            SELECT SUM(amount_cents) AS total_cents, COUNT(*) AS order_count
            FROM order_ledger
            WHERE placed_at_millis BETWEEN ? AND ? AND product_id = ?
            """;

    private final Connection connection;

    public SqliteOrderLedgerRepository(Connection connection) {
        this.connection = connection;
        createTableIfMissing();
    }

    private void createTableIfMissing() {
        try (Statement statement = connection.createStatement()) {
            statement.execute(CREATE_TABLE);
        } catch (SQLException e) {
            throw new SqliteRepositoryException("criar a tabela order_ledger", e);
        }
    }

    @Override
    public void append(OrderRecord record) {
        try (PreparedStatement statement = connection.prepareStatement(INSERT)) {
            statement.setString(1, record.orderId().value().toString());
            statement.setString(2, record.customerId().value());
            statement.setString(3, record.productId().value());
            statement.setInt(4, record.quantity().value());
            statement.setLong(5, MoneyCents.toCents(record.amount()));
            statement.setLong(6, record.placedAt().toEpochMilli());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new SqliteRepositoryException("gravar order_ledger de " + record.orderId(), e);
        }
    }

    @Override
    public RevenueWindow revenueOver(TimeRange range) {
        try (PreparedStatement statement = connection.prepareStatement(REVENUE_OVER)) {
            statement.setLong(1, range.start().toEpochMilli());
            statement.setLong(2, range.end().toEpochMilli());
            return readRevenueWindow(statement, range);
        } catch (SQLException e) {
            throw new SqliteRepositoryException("calcular faturamento do período " + range, e);
        }
    }

    @Override
    public RevenueWindow revenueOver(TimeRange range, ProductId productId) {
        try (PreparedStatement statement = connection.prepareStatement(REVENUE_OVER_FOR_PRODUCT)) {
            statement.setLong(1, range.start().toEpochMilli());
            statement.setLong(2, range.end().toEpochMilli());
            statement.setString(3, productId.value());
            return readRevenueWindow(statement, range);
        } catch (SQLException e) {
            throw new SqliteRepositoryException(
                    "calcular faturamento do período " + range + " para " + productId, e);
        }
    }

    /** SUM() devolve NULL quando não há linhas — sem pedidos, o resultado é a janela zerada. */
    private RevenueWindow readRevenueWindow(PreparedStatement statement, TimeRange range) throws SQLException {
        try (ResultSet rs = statement.executeQuery()) {
            if (!rs.next()) {
                return RevenueWindow.empty(range);
            }
            long totalCents = rs.getLong("total_cents");
            if (rs.wasNull()) {
                return RevenueWindow.empty(range);
            }
            long orderCount = rs.getLong("order_count");
            return new RevenueWindow(range, MoneyCents.fromCents(totalCents), orderCount);
        }
    }
}
