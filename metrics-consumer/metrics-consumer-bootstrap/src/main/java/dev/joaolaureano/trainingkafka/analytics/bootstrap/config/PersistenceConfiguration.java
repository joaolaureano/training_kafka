package dev.joaolaureano.trainingkafka.analytics.bootstrap.config;

import dev.joaolaureano.trainingkafka.analytics.adapters.persistence.duckdb.DuckDbOrderLedgerRepository;
import dev.joaolaureano.trainingkafka.analytics.adapters.persistence.duckdb.DuckDbProductSalesRepository;
import dev.joaolaureano.trainingkafka.analytics.adapters.persistence.inmemory.InMemoryOrderLedgerRepository;
import dev.joaolaureano.trainingkafka.analytics.adapters.persistence.inmemory.InMemoryProductSalesRepository;
import dev.joaolaureano.trainingkafka.analytics.adapters.persistence.sqlite.SqliteOrderLedgerRepository;
import dev.joaolaureano.trainingkafka.analytics.adapters.persistence.sqlite.SqliteProductSalesRepository;
import dev.joaolaureano.trainingkafka.analytics.domain.port.OrderLedgerRepository;
import dev.joaolaureano.trainingkafka.analytics.domain.port.ProductSalesRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * O ÚNICO arquivo do serviço que sabe quais tecnologias de persistência existem.
 *
 * Troque {@code --spring.profiles.active} entre {@code inmemory}, {@code sqlite} e
 * {@code duckdb} e o comportamento externo do App B não muda em nada — só o lugar
 * onde os bytes param. Nenhuma classe de domínio precisa ser tocada, recompilada
 * ou sequer aberta para isso acontecer. É a prova prática de que os Ports estão
 * bem desenhados.
 */
@Configuration
public class PersistenceConfiguration {

    private static final Logger log = LoggerFactory.getLogger(PersistenceConfiguration.class);

    // ---------------------------------------------------------------- em memória

    @Configuration
    @Profile("inmemory")
    static class InMemoryPersistence {

        @Bean
        ProductSalesRepository productSalesRepository() {
            log.info("Persistência: EM MEMÓRIA (os dados somem ao reiniciar)");
            return new InMemoryProductSalesRepository();
        }

        @Bean
        OrderLedgerRepository orderLedgerRepository() {
            return new InMemoryOrderLedgerRepository();
        }
    }

    // -------------------------------------------------------------------- SQLite

    @Configuration
    @Profile("sqlite")
    static class SqlitePersistence {

        @Bean(destroyMethod = "close")
        Connection sqliteConnection(@Value("${analytics.persistence.sqlite.path:data/metrics.db}") String path) {
            log.info("Persistência: SQLite em {}", path);
            return openConnection("jdbc:sqlite:" + path, path);
        }

        @Bean
        ProductSalesRepository productSalesRepository(Connection connection) {
            return new SqliteProductSalesRepository(connection);
        }

        @Bean
        OrderLedgerRepository orderLedgerRepository(Connection connection) {
            return new SqliteOrderLedgerRepository(connection);
        }
    }

    // -------------------------------------------------------------------- DuckDB

    @Configuration
    @Profile("duckdb")
    static class DuckDbPersistence {

        @Bean(destroyMethod = "close")
        Connection duckDbConnection(@Value("${analytics.persistence.duckdb.path:data/metrics.duckdb}") String path) {
            log.info("Persistência: DuckDB em {}", path);
            // DuckDB aceita UM ÚNICO processo escritor por arquivo. Se o App C
            // também estiver apontado para este arquivo, o segundo a subir falha
            // com erro de lock — por isso o App C usa arquivo próprio por padrão.
            return openConnection("jdbc:duckdb:" + path, path);
        }

        @Bean
        ProductSalesRepository productSalesRepository(Connection connection) {
            return new DuckDbProductSalesRepository(connection);
        }

        @Bean
        OrderLedgerRepository orderLedgerRepository(Connection connection) {
            return new DuckDbOrderLedgerRepository(connection);
        }
    }

    private static Connection openConnection(String jdbcUrl, String path) {
        try {
            Path parent = Path.of(path).toAbsolutePath().getParent();
            if (parent != null) {
                java.nio.file.Files.createDirectories(parent);
            }
            return DriverManager.getConnection(jdbcUrl);
        } catch (SQLException | java.io.IOException failure) {
            throw new IllegalStateException("Não foi possível abrir " + jdbcUrl, failure);
        }
    }
}
