package dev.joaolaureano.trainingkafka.logs.bootstrap.config;

import dev.joaolaureano.trainingkafka.logs.adapters.persistence.duckdb.DuckDbLogRepository;
import dev.joaolaureano.trainingkafka.logs.adapters.persistence.jsonl.JsonlFileLogRepository;
import dev.joaolaureano.trainingkafka.logs.adapters.persistence.stdout.StdoutLogRepository;
import dev.joaolaureano.trainingkafka.logs.domain.port.LogRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Escolhe onde os logs param: console, arquivo JSONL ou DuckDB.
 *
 * Três tecnologias sem nada em comum — uma nem sequer armazena — atrás da mesma
 * interface. É o caso mais extremo do projeto, e o que melhor mostra o valor de
 * ter modelado o Port em termos do que o domínio precisa, e não do que a
 * tecnologia oferece.
 */
@Configuration
public class LogPersistenceConfiguration {

    private static final Logger log = LoggerFactory.getLogger(LogPersistenceConfiguration.class);

    @Configuration
    @Profile("stdout")
    static class StdoutPersistence {

        @Bean
        LogRepository logRepository() {
            log.info("Persistência de logs: STDOUT (não armazena — GET /logs sempre volta vazio)");
            return new StdoutLogRepository();
        }
    }

    @Configuration
    @Profile("jsonl")
    static class JsonlPersistence {

        @Bean
        LogRepository logRepository(@Value("${logs.jsonl.path:data/logs.jsonl}") String path) {
            log.info("Persistência de logs: arquivo JSONL em {}", path);
            return new JsonlFileLogRepository(Path.of(path));
        }
    }

    @Configuration
    @Profile("duckdb")
    static class DuckDbPersistence {

        /**
         * Arquivo PRÓPRIO por padrão, separado do usado pelo App B.
         *
         * DuckDB aceita um único processo escritor por arquivo. Se este serviço e o
         * App B apontarem para o mesmo caminho ao mesmo tempo, o segundo a subir
         * falha com erro de lock. Apontar {@code logs.duckdb.path} para
         * {@code data/metrics.duckdb} funciona — desde que apenas um dos dois
         * esteja no profile duckdb naquele momento.
         */
        @Bean(destroyMethod = "close")
        Connection logsDuckDbConnection(@Value("${logs.duckdb.path:data/logs.duckdb}") String path) {
            log.info("Persistência de logs: DuckDB em {}", path);
            try {
                Path parent = Path.of(path).toAbsolutePath().getParent();
                if (parent != null) {
                    Files.createDirectories(parent);
                }
                return DriverManager.getConnection("jdbc:duckdb:" + path);
            } catch (SQLException | IOException failure) {
                throw new IllegalStateException("Não foi possível abrir o DuckDB em " + path, failure);
            }
        }

        @Bean
        LogRepository logRepository(Connection logsDuckDbConnection) {
            return new DuckDbLogRepository(logsDuckDbConnection);
        }
    }
}
