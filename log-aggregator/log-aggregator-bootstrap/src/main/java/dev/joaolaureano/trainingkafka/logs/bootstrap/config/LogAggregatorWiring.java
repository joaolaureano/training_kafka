package dev.joaolaureano.trainingkafka.logs.bootstrap.config;

import dev.joaolaureano.trainingkafka.logs.adapters.messaging.IngestLogPort;
import dev.joaolaureano.trainingkafka.logs.adapters.web.LogQueryPort;
import dev.joaolaureano.trainingkafka.logs.application.IngestLogService;
import dev.joaolaureano.trainingkafka.logs.application.LogQueryService;
import dev.joaolaureano.trainingkafka.logs.bootstrap.facade.IngestLogFacade;
import dev.joaolaureano.trainingkafka.logs.bootstrap.facade.LogQueryFacade;
import dev.joaolaureano.trainingkafka.logs.domain.port.LogRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Monta os casos de uso sobre o Port de persistência, sem saber qual
 * implementação chegou, e liga cada adapter de entrada ao caso de uso pela
 * facade correspondente.
 */
@Configuration
public class LogAggregatorWiring {

    @Bean
    public IngestLogService ingestLogService(LogRepository repository) {
        return new IngestLogService(repository);
    }

    @Bean
    public LogQueryService logQueryService(LogRepository repository) {
        return new LogQueryService(repository);
    }

    @Bean
    public IngestLogPort ingestLogPort(IngestLogService ingestLogService) {
        return new IngestLogFacade(ingestLogService);
    }

    @Bean
    public LogQueryPort logQueryPort(LogQueryService logQueryService) {
        return new LogQueryFacade(logQueryService);
    }
}
