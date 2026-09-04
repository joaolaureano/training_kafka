package dev.joaolaureano.trainingkafka.inventory.adapters.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.joaolaureano.trainingkafka.inventory.adapters.persistence.OutboxDispatcher;
import dev.joaolaureano.trainingkafka.inventory.adapters.persistence.OutboxRecord;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;

/**
 * O despacho de fato: reidrata o JSON guardado e manda para o tópico.
 *
 * Vai como {@link JsonNode}, não como String: o produtor usa JsonSerializer, e
 * uma String seria serializada de novo — o consumidor receberia o JSON entre
 * aspas, escapado.
 *
 * Devolve o future sem esperar. Quem espera é o relay, depois de pôr o lote
 * inteiro em voo.
 */
public final class KafkaOutboxDispatcher implements OutboxDispatcher {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public KafkaOutboxDispatcher(KafkaTemplate<String, Object> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public CompletableFuture<Void> dispatch(OutboxRecord record) {
        JsonNode payload;
        try {
            payload = objectMapper.readTree(record.payload());
        } catch (Exception malformed) {
            // Payload ilegível não é falha transitória: falhar o future evita um
            // retry infinito sobre uma linha que nunca vai sair.
            return CompletableFuture.failedFuture(
                    new IllegalStateException("payload inválido no outbox: " + record.sequence(), malformed));
        }
        return kafkaTemplate.send(record.topic(), record.partitionKey(), payload)
                .thenApply(result -> null);
    }
}
