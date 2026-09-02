package dev.joaolaureano.trainingkafka.audit.bootstrap.config;

import dev.joaolaureano.trainingkafka.audit.adapters.messaging.IngestAuditPort;
import dev.joaolaureano.trainingkafka.audit.adapters.web.AuditQueryPort;
import dev.joaolaureano.trainingkafka.audit.application.IngestAuditService;
import dev.joaolaureano.trainingkafka.audit.application.AuditQueryService;
import dev.joaolaureano.trainingkafka.audit.bootstrap.facade.IngestAuditFacade;
import dev.joaolaureano.trainingkafka.audit.bootstrap.facade.AuditQueryFacade;
import dev.joaolaureano.trainingkafka.audit.domain.port.AuditRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Monta os casos de uso sobre o Port de persistência, sem saber qual
 * implementação chegou, e liga cada adapter de entrada ao caso de uso pela
 * facade correspondente.
 */
@Configuration
public class AuditServiceWiring {

    @Bean
    public IngestAuditService ingestLogService(AuditRepository repository) {
        return new IngestAuditService(repository);
    }

    @Bean
    public AuditQueryService logQueryService(AuditRepository repository) {
        return new AuditQueryService(repository);
    }

    @Bean
    public IngestAuditPort ingestLogPort(IngestAuditService ingestLogService) {
        return new IngestAuditFacade(ingestLogService);
    }

    @Bean
    public AuditQueryPort logQueryPort(AuditQueryService logQueryService) {
        return new AuditQueryFacade(logQueryService);
    }
}
