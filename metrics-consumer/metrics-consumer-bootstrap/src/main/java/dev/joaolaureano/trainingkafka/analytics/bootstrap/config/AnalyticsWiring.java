package dev.joaolaureano.trainingkafka.analytics.bootstrap.config;

import dev.joaolaureano.trainingkafka.analytics.adapters.messaging.OrderPlacedPort;
import dev.joaolaureano.trainingkafka.analytics.adapters.web.MetricsQueryPort;
import dev.joaolaureano.trainingkafka.analytics.application.MetricsQueryService;
import dev.joaolaureano.trainingkafka.analytics.application.OrderPlacedHandler;
import dev.joaolaureano.trainingkafka.analytics.bootstrap.facade.MetricsQueryFacade;
import dev.joaolaureano.trainingkafka.analytics.bootstrap.facade.OrderPlacedFacade;
import dev.joaolaureano.trainingkafka.analytics.domain.port.OrderLedgerRepository;
import dev.joaolaureano.trainingkafka.analytics.domain.port.ProductSalesRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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
public class AnalyticsWiring {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    public OrderPlacedHandler orderPlacedHandler(OrderLedgerRepository ledger,
                                                 ProductSalesRepository productSales) {
        return new OrderPlacedHandler(ledger, productSales);
    }

    @Bean
    public MetricsQueryService metricsQueryService(ProductSalesRepository productSales,
                                                   OrderLedgerRepository ledger) {
        return new MetricsQueryService(productSales, ledger);
    }

    // As facades: cada adapter declarou a interface de que precisa, e é aqui —
    // no único módulo que enxerga os dois lados — que ela ganha implementação.

    @Bean
    public OrderPlacedPort orderPlacedPort(OrderPlacedHandler orderPlacedHandler) {
        return new OrderPlacedFacade(orderPlacedHandler);
    }

    @Bean
    public MetricsQueryPort metricsQueryPort(MetricsQueryService metricsQueryService) {
        return new MetricsQueryFacade(metricsQueryService);
    }
}
