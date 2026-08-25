package dev.joaolaureano.trainingkafka.analytics.adapters.persistence.duckdb;

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
 * Implementação DuckDB (JDBC puro) de {@link ProductSalesRepository}.
 *
 * A conexão é injetada e permanece de responsabilidade de quem a criou: esta
 * classe não abre nem fecha {@link Connection} alguma.
 */
public class DuckDbProductSalesRepository implements ProductSalesRepository {

    private static final String CREATE_TABLE = """
            CREATE TABLE IF NOT EXISTS product_sales (
                product_id    VARCHAR PRIMARY KEY,
                units_sold    BIGINT  NOT NULL,
                revenue_cents BIGINT  NOT NULL,
                order_count   BIGINT  NOT NULL
            )
            """;

    private final Connection connection;

    public DuckDbProductSalesRepository(Connection connection) {
        this.connection = connection;
        try (Statement statement = connection.createStatement()) {
            statement.execute(CREATE_TABLE);
        } catch (SQLException e) {
            throw new RuntimeException("falha ao criar a tabela product_sales no DuckDB", e);
        }
    }

    @Override
    public ProductSalesRecord findOrCreate(ProductId productId) {
        String sql = "SELECT units_sold, revenue_cents, order_count FROM product_sales WHERE product_id = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
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
            throw new RuntimeException("falha ao buscar product_sales no DuckDB para " + productId, e);
        }
    }

    @Override
    public void save(ProductSalesRecord record) {
        // DuckDB 1.5.x suporta "INSERT ... ON CONFLICT DO UPDATE", mas essa sintaxe
        // exige repetir a lista de colunas do upsert e varia conforme a versão do
        // driver. Como cada produto é uma única linha e não há concorrência (o
        // agregado é reconstituído, mutado e salvo em série pelo mesmo consumer),
        // um UPDATE seguido de INSERT quando nenhuma linha foi afetada é portável,
        // legível e não depende de nenhuma sintaxe específica de upsert do DuckDB.
        String update = "UPDATE product_sales SET units_sold = ?, revenue_cents = ?, order_count = ? "
                + "WHERE product_id = ?";
        String insert = "INSERT INTO product_sales (product_id, units_sold, revenue_cents, order_count) "
                + "VALUES (?, ?, ?, ?)";
        try {
            try (PreparedStatement statement = connection.prepareStatement(update)) {
                statement.setLong(1, record.unitsSold().value());
                statement.setLong(2, MoneyCents.toCents(record.revenue()));
                statement.setLong(3, record.orderCount());
                statement.setString(4, record.productId().value());
                if (statement.executeUpdate() > 0) {
                    return;
                }
            }
            try (PreparedStatement statement = connection.prepareStatement(insert)) {
                statement.setString(1, record.productId().value());
                statement.setLong(2, record.unitsSold().value());
                statement.setLong(3, MoneyCents.toCents(record.revenue()));
                statement.setLong(4, record.orderCount());
                statement.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException("falha ao salvar product_sales no DuckDB para " + record.productId(), e);
        }
    }

    @Override
    public List<ProductSalesRecord> topSelling(int limit) {
        String sql = "SELECT product_id, units_sold, revenue_cents, order_count "
                + "FROM product_sales ORDER BY units_sold DESC LIMIT ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, limit);
            try (ResultSet rs = statement.executeQuery()) {
                List<ProductSalesRecord> result = new ArrayList<>();
                while (rs.next()) {
                    result.add(ProductSalesRecord.reconstitute(
                            new ProductId(rs.getString("product_id")),
                            new Units(rs.getLong("units_sold")),
                            MoneyCents.fromCents(rs.getLong("revenue_cents")),
                            rs.getLong("order_count")));
                }
                return result;
            }
        } catch (SQLException e) {
            throw new RuntimeException("falha ao buscar top selling no DuckDB", e);
        }
    }
}
