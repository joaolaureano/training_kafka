package dev.joaolaureano.trainingkafka.analytics.adapters.messaging;

import dev.joaolaureano.trainingkafka.analytics.domain.event.DomainEvent;
import dev.joaolaureano.trainingkafka.analytics.domain.event.SuspiciousPatternDetected;
import dev.joaolaureano.trainingkafka.analytics.domain.port.DomainEventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Traduz fatos do domínio em logs estruturados no tópico "application-logs".
 *
 * É aqui — e só aqui — que se decide que um {@link SuspiciousPatternDetected}
 * merece nível WARN e vira uma linha de log. O agregado que emitiu o fato não
 * tem opinião sobre isso.
 */
public class KafkaDomainEventPublisher implements DomainEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaDomainEventPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String applicationName;

    public KafkaDomainEventPublisher(KafkaTemplate<String, Object> kafkaTemplate, String applicationName) {
        this.kafkaTemplate = kafkaTemplate;
        this.applicationName = applicationName;
    }

    @Override
    public void publish(DomainEvent event) {
        if (event instanceof SuspiciousPatternDetected alert) {
            publishAlert(alert);
            return;
        }
        log.debug("Evento de domínio sem tradução para log: {}", event.getClass().getSimpleName());
    }

    private void publishAlert(SuspiciousPatternDetected alert) {
        Map<String, String> context = new LinkedHashMap<>();
        context.put("customerId", alert.customerId().value());
        context.put("ordersInWindow", Integer.toString(alert.ordersInWindow()));
        context.put("windowSeconds", Long.toString(alert.window().toSeconds()));
        context.put("sampleOrderIds", alert.sample().stream()
                .map(Object::toString).collect(Collectors.joining(",")));

        ApplicationLogMessage message = new ApplicationLogMessage(
                "WARN",
                alert.occurredAt().toString(),
                applicationName,
                "suspicious order pattern detected",
                context);

        kafkaTemplate.send(Topics.APPLICATION_LOGS, applicationName, message)
                .whenComplete((result, failure) -> {
                    if (failure != null) {
                        log.warn("Falha ao publicar alerta de padrão suspeito: {}", failure.getMessage());
                    }
                });
    }
}
