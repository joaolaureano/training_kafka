package dev.joaolaureano.trainingkafka.orders.adapters.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.Map;

/**
 * Publica registros de atividade no tópico de logs.
 *
 * A assinatura é em tipos crus de propósito: esta classe não implementa
 * {@code ActivityLogPublisher} — fazer isso a obrigaria a compilar contra o
 * módulo -application. Quem faz a ponte é a facade montada no bootstrap, que
 * conhece os dois lados.
 */
public class KafkaActivityLogPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaActivityLogPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String applicationName;

    public KafkaActivityLogPublisher(KafkaTemplate<String, Object> kafkaTemplate, String applicationName) {
        this.kafkaTemplate = kafkaTemplate;
        this.applicationName = applicationName;
    }

    public void publish(String level, String message, Map<String, String> context, Instant occurredAt) {
        ApplicationLogMessage payload = new ApplicationLogMessage(
                level,
                occurredAt.toString(),
                applicationName,
                message,
                context);

        // Chave = nome da aplicação, para agrupar os logs de um mesmo serviço na
        // mesma partição e preservar a ordem cronológica dentro dele.
        kafkaTemplate.send(Topics.APPLICATION_LOGS, applicationName, payload)
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
