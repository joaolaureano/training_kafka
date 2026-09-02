package dev.joaolaureano.trainingkafka.audit.bootstrap;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Ponto de entrada do processo.
 *
 * O scan cobre {@code ...audit} inteiro porque as classes de borda moram no
 * módulo -adapters. O que elas não conseguem resolver sozinhas — quem atende os
 * Ports que declaram — é decidido em
 * {@link dev.joaolaureano.trainingkafka.audit.bootstrap.config.AuditServiceWiring}.
 */
@SpringBootApplication(scanBasePackages = "dev.joaolaureano.trainingkafka.audit")
public class AuditServiceBootstrap {

    public static void main(String[] args) {
        SpringApplication.run(AuditServiceBootstrap.class, args);
    }
}
