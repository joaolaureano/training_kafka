package dev.joaolaureano.trainingkafka.logs.adapters.config;

import dev.joaolaureano.trainingkafka.logs.application.IngestLogService;
import dev.joaolaureano.trainingkafka.logs.application.LogQueryService;
import dev.joaolaureano.trainingkafka.logs.domain.port.LogRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Monta os casos de uso sobre o Port, sem saber qual implementação chegou. */
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
}
