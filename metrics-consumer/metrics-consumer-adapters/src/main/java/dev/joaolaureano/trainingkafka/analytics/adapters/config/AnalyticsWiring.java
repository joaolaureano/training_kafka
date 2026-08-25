package dev.joaolaureano.trainingkafka.analytics.adapters.config;

import dev.joaolaureano.trainingkafka.analytics.adapters.messaging.KafkaDomainEventPublisher;
import dev.joaolaureano.trainingkafka.analytics.application.MetricsQueryService;
import dev.joaolaureano.trainingkafka.analytics.application.OrderPlacedHandler;
import dev.joaolaureano.trainingkafka.analytics.domain.model.SuspicionPolicy;
import dev.joaolaureano.trainingkafka.analytics.domain.port.CustomerPatternRepository;
import dev.joaolaureano.trainingkafka.analytics.domain.port.DomainEventPublisher;
import dev.joaolaureano.trainingkafka.analytics.domain.port.OrderLedgerRepository;
import dev.joaolaureano.trainingkafka.analytics.domain.port.ProductSalesRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Clock;

/**
 * Montagem do serviço.
 *
 * Repare que este arquivo não menciona SQLite, DuckDB nem mapa em memória: ele
 * recebe os três Ports já resolvidos e monta o caso de uso em cima deles. Quem
 * decide QUAL implementação entra é {@link PersistenceConfiguration}, por profile.
 * Nem esta classe nem o domínio ficam sabendo da escolha.
 */
@Configuration
@EnableConfigurationProperties(SuspicionProperties.class)
public class AnalyticsWiring {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    public SuspicionPolicy suspicionPolicy(SuspicionProperties properties) {
        return properties.toPolicy();
    }

    @Bean
    public DomainEventPublisher domainEventPublisher(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${spring.application.name}") String applicationName) {
        return new KafkaDomainEventPublisher(kafkaTemplate, applicationName);
    }

    @Bean
    public OrderPlacedHandler orderPlacedHandler(OrderLedgerRepository ledger,
                                                 ProductSalesRepository productSales,
                                                 CustomerPatternRepository customerPatterns,
                                                 DomainEventPublisher eventPublisher,
                                                 SuspicionPolicy suspicionPolicy) {
        return new OrderPlacedHandler(ledger, productSales, customerPatterns,
                eventPublisher, suspicionPolicy);
    }

    @Bean
    public MetricsQueryService metricsQueryService(ProductSalesRepository productSales,
                                                   OrderLedgerRepository ledger) {
        return new MetricsQueryService(productSales, ledger);
    }
}
