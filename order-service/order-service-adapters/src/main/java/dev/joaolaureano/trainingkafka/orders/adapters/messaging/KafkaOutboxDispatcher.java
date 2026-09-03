package dev.joaolaureano.trainingkafka.orders.adapters.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.joaolaureano.trainingkafka.orders.adapters.persistence.OutboxDispatcher;
import dev.joaolaureano.trainingkafka.orders.adapters.persistence.OutboxRecord;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.TimeUnit;

/**
 * O despacho de fato: reidrata o JSON guardado e manda para o tópico.
 *
 * Vai como {@link JsonNode}, não como String: o produtor está configurado com
 * JsonSerializer, e uma String seria serializada de novo — o consumidor receberia
 * o JSON entre aspas, escapado.
 */
public final class KafkaOutboxDispatcher implements OutboxDispatcher {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final long sendTimeoutSeconds;

    public KafkaOutboxDispatcher(KafkaTemplate<String, Object> kafkaTemplate,
                                 ObjectMapper objectMapper, long sendTimeoutSeconds) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.sendTimeoutSeconds = sendTimeoutSeconds;
    }

    @Override
    public void dispatch(OutboxRecord record) throws Exception {
        JsonNode payload = objectMapper.readTree(record.payload());
        kafkaTemplate.send(record.topic(), record.partitionKey(), payload)
                .get(sendTimeoutSeconds, TimeUnit.SECONDS);
    }
}
