package dev.joaolaureano.trainingkafka.analytics.adapters.persistence.sqlite;

import dev.joaolaureano.trainingkafka.analytics.adapters.persistence.MoneyCents;
import dev.joaolaureano.trainingkafka.analytics.domain.model.ProductId;
import dev.joaolaureano.trainingkafka.analytics.domain.model.ProductSalesRecord;
import dev.joaolaureano.trainingkafka.analytics.domain.model.Units;
import dev.joaolaureano.trainingkafka.analytics.domain.port.ProductSalesRepository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementação SQLite (JDBC puro) de {@link ProductSalesRepository}.
 *
 * A conexão é recebida pronta e não é aberta nem fechada por esta classe — quem
 * a criou é quem decide o ciclo de vida dela.
 */
public class SqliteProductSalesRepository implements ProductSalesRepository {

    private static final String CREATE_TABLE = """
            CREATE TABLE IF NOT EXISTS product_sales (
                product_id    VARCHAR PRIMARY KEY,
                units_sold    BIGINT  NOT NULL,
                revenue_cents BIGINT  NOT NULL,
                order_count   BIGINT  NOT NULL
            )
            """;

    private static final String SELECT_BY_ID = """
            SELECT units_sold, revenue_cents, order_count
            FROM product_sales
            WHERE product_id = ?
            """;

    private static final String UPSERT = """
            INSERT INTO product_sales (product_id, units_sold, revenue_cents, order_count)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(product_id) DO UPDATE SET
                units_sold = excluded.units_sold,
                revenue_cents = excluded.revenue_cents,
                order_count = excluded.order_count
            """;

    private static final String TOP_SELLING = """
            SELECT product_id, units_sold, revenue_cents, order_count
            FROM product_sales
            ORDER BY units_sold DESC
            LIMIT ?
            """;

    private final Connection connection;

    public SqliteProductSalesRepository(Connection connection) {
        this.connection = connection;
        createTableIfMissing();
    }

    private void createTableIfMissing() {
        try (Statement statement = connection.createStatement()) {
            statement.execute(CREATE_TABLE);
        } catch (SQLException e) {
            throw new SqliteRepositoryException("criar a tabela product_sales", e);
        }
    }

    @Override
    public ProductSalesRecord findOrCreate(ProductId productId) {
        try (PreparedStatement statement = connection.prepareStatement(SELECT_BY_ID)) {
            statement.setString(1, productId.value());
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    return ProductSalesRecord.startFor(productId);
                }
                return ProductSalesRecord.reconstitute(
                        productId,
                        new Units(rs.getLong("units_sold")),
                        MoneyCents.fromCents(rs.getLong("revenue_cents")),
                        rs.getLong("order_count"));
            }
        } catch (SQLException e) {
            throw new SqliteRepositoryException("buscar product_sales de " + productId, e);
        }
    }

    @Override
    public void save(ProductSalesRecord record) {
        try (PreparedStatement statement = connection.prepareStatement(UPSERT)) {
            statement.setString(1, record.productId().value());
            statement.setLong(2, record.unitsSold().value());
            statement.setLong(3, MoneyCents.toCents(record.revenue()));
            statement.setLong(4, record.orderCount());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new SqliteRepositoryException("salvar product_sales de " + record.productId(), e);
        }
    }

    @Override
    public List<ProductSalesRecord> topSelling(int limit) {
        List<ProductSalesRecord> ranking = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(TOP_SELLING)) {
            statement.setInt(1, limit);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    ProductId productId = new ProductId(rs.getString("product_id"));
                    ranking.add(ProductSalesRecord.reconstitute(
                            productId,
                            new Units(rs.getLong("units_sold")),
                            MoneyCents.fromCents(rs.getLong("revenue_cents")),
                            rs.getLong("order_count")));
                }
            }
            return ranking;
        } catch (SQLException e) {
            throw new SqliteRepositoryException("buscar o ranking de mais vendidos (limit=" + limit + ")", e);
        }
    }
}
