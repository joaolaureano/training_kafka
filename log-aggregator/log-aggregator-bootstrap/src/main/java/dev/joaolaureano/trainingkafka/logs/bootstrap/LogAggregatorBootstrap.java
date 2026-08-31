package dev.joaolaureano.trainingkafka.logs.bootstrap;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Ponto de entrada do processo.
 *
 * O scan cobre {@code ...logs} inteiro porque as classes de borda moram no
 * módulo -adapters. O que elas não conseguem resolver sozinhas — quem atende os
 * Ports que declaram — é decidido em
 * {@link dev.joaolaureano.trainingkafka.logs.bootstrap.config.LogAggregatorWiring}.
 */
@SpringBootApplication(scanBasePackages = "dev.joaolaureano.trainingkafka.logs")
public class LogAggregatorBootstrap {

    public static void main(String[] args) {
        SpringApplication.run(LogAggregatorBootstrap.class, args);
    }
}
