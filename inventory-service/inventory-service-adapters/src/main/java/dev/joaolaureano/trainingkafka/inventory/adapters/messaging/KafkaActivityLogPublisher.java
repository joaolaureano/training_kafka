package dev.joaolaureano.trainingkafka.inventory.adapters.messaging;

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
 * módulo -application. Quem faz a ponte é a facade montada no bootstrap.
 */
public class KafkaActivityLogPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaActivityLogPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String applicationName;

    public KafkaActivityLogPublisher(KafkaTemplate<String, Object> kafkaTemplate, String applicationName) {
        this.kafkaTemplate = kafkaTemplate;
        this.applicationName = applicationName;
    }

    public void publish(String level, String action, Map<String, String> context, Instant occurredAt) {
        AuditEventMessage payload = new AuditEventMessage(
                level, occurredAt.toString(), applicationName, action, context);

        // Chave = o pedido, não o serviço. Com o nome da aplicação, a cardinalidade
        // da chave era o número de serviços produtores: duas partições recebiam tudo
        // e a terceira ficava ociosa, por mais partições que o tópico tivesse. Por
        // pedido, todas as linhas de auditoria da mesma compra — aceite, cobrança,
        // estorno — caem na mesma partição, na ordem em que aconteceram. Que é a
        // ordem de que alguém investigando um incidente realmente precisa.
        kafkaTemplate.send(Topics.AUDIT_EVENTS, partitionKey(context), payload)
                .whenComplete((result, failure) -> {
                    // Log é telemetria: se o envio falhar, registramos localmente e
                    // seguimos. Recusar uma reserva porque o log não foi publicado
                    // inverteria a prioridade entre o essencial e o acessório.
                    if (failure != null) {
                        log.warn("Falha ao publicar log de atividade no tópico {}: {}",
                                Topics.AUDIT_EVENTS, failure.getMessage());
                    }
                });
    }

    /**
     * Chave de partição derivada do contexto: o correlationId da Saga, se houver,
     * senão o pedido. Nulo quando não há nenhum dos dois; aí o sticky partitioner
     * distribui em lotes, que é o comportamento desejado.
     *
     * O literal {@code "null"} é descartado de propósito: o contexto é
     * {@code Map<String, String>} passado por {@code Map.copyOf}, que recusa
     * valores nulos, então quem monta o log escreve {@code String.valueOf(...)} e
     * um correlationId ausente vira a string "null". Aceitá-la como chave jogaria
     * todos esses eventos numa partição só — exatamente o problema que este método
     * existe para evitar.
     */
    private static String partitionKey(Map<String, String> context) {
        if (context == null) {
            return null;
        }
        String correlationId = usable(context.get("correlationId"));
        return correlationId != null ? correlationId : usable(context.get("orderId"));
    }

    private static String usable(String value) {
        if (value == null || value.isBlank() || "null".equals(value)) {
            return null;
        }
        return value;
    }
}
