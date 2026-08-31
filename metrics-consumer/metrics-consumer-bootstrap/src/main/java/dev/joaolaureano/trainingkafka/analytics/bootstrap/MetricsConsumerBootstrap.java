package dev.joaolaureano.trainingkafka.analytics.bootstrap;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Ponto de entrada do processo.
 *
 * O scan cobre {@code ...analytics} inteiro porque as classes de borda moram no
 * módulo -adapters. Quem atende os Ports que elas declaram é decidido em
 * {@link dev.joaolaureano.trainingkafka.analytics.bootstrap.config.AnalyticsWiring}.
 */
@SpringBootApplication(scanBasePackages = "dev.joaolaureano.trainingkafka.analytics")
public class MetricsConsumerBootstrap {

    public static void main(String[] args) {
        SpringApplication.run(MetricsConsumerBootstrap.class, args);
    }
}
