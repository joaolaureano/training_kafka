package dev.joaolaureano.trainingkafka.analytics.adapters.persistence;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fábrica de conexões descartáveis para os testes de contrato.
 *
 * Cada chamada devolve um banco novo em arquivo temporário, para que um teste
 * nunca enxergue o estado deixado por outro.
 */
final class TestRepositories {

    private static final AtomicLong COUNTER = new AtomicLong();
    private static final Path TEMP_DIR = createTempDir();

    private TestRepositories() {
    }

    static Connection freshSqlite() {
        return open("jdbc:sqlite:" + TEMP_DIR.resolve("test-" + COUNTER.incrementAndGet() + ".db"));
    }

    static Connection freshDuckDb() {
        return open("jdbc:duckdb:" + TEMP_DIR.resolve("test-" + COUNTER.incrementAndGet() + ".duckdb"));
    }

    private static Connection open(String jdbcUrl) {
        try {
            return DriverManager.getConnection(jdbcUrl);
        } catch (SQLException failure) {
            throw new IllegalStateException("não foi possível abrir " + jdbcUrl, failure);
        }
    }

    private static Path createTempDir() {
        try {
            Path dir = Files.createTempDirectory("training-kafka-tests");
            dir.toFile().deleteOnExit();
            return dir;
        } catch (IOException failure) {
            throw new IllegalStateException("não foi possível criar diretório temporário", failure);
        }
    }
}
