package dev.joaolaureano.trainingkafka.orders.adapters.messaging;

import dev.joaolaureano.trainingkafka.orders.application.port.ActivityLog;
import dev.joaolaureano.trainingkafka.orders.application.port.ActivityLogPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

public class KafkaActivityLogPublisher implements ActivityLogPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaActivityLogPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String applicationName;

    public KafkaActivityLogPublisher(KafkaTemplate<String, Object> kafkaTemplate, String applicationName) {
        this.kafkaTemplate = kafkaTemplate;
        this.applicationName = applicationName;
    }

    @Override
    public void publish(ActivityLog activityLog) {
        ApplicationLogMessage message = new ApplicationLogMessage(
                activityLog.level().name(),
                activityLog.occurredAt().toString(),
                applicationName,
                activityLog.message(),
                activityLog.context());

        // Chave = nome da aplicação, para agrupar os logs de um mesmo serviço na
        // mesma partição e preservar a ordem cronológica dentro dele.
        kafkaTemplate.send(Topics.APPLICATION_LOGS, applicationName, message)
                .whenComplete((result, failure) -> {
                    // Log é telemetria: se o envio falhar, registramos localmente e
                    // seguimos. Derrubar um pedido válido porque o log não foi
                    // publicado inverteria a prioridade entre o essencial e o acessório.
                    if (failure != null) {
                        log.warn("Falha ao publicar log de atividade no tópico {}: {}",
                                Topics.APPLICATION_LOGS, failure.getMessage());
                    }
                });
    }
}
