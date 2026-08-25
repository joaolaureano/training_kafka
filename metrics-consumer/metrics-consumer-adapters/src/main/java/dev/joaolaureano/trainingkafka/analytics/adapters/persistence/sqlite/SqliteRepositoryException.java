package dev.joaolaureano.trainingkafka.analytics.adapters.persistence.sqlite;

/**
 * Envolve uma {@link java.sql.SQLException} numa exceção não checada, com uma
 * mensagem que diz qual operação falhou.
 *
 * Os repositórios SQLite não propagam {@code SQLException} diretamente porque
 * ela é um detalhe de infraestrutura que os Ports (interfaces de domínio) não
 * declaram — e não deveriam declarar.
 */
public class SqliteRepositoryException extends RuntimeException {

    public SqliteRepositoryException(String operation, Throwable cause) {
        super("falha ao " + operation + " no SQLite: " + cause.getMessage(), cause);
    }
}
