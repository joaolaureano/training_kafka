package dev.joaolaureano.trainingkafka.analytics.adapters.messaging;

import dev.joaolaureano.trainingkafka.analytics.application.OrderPlacedHandler;
import dev.joaolaureano.trainingkafka.analytics.domain.model.InvalidValueException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Adapter de entrada: consome o tópico "orders".
 *
 * Traduz e delega — nada mais. A anotação {@code @KafkaListener} vive aqui, na
 * borda, e em nenhum outro lugar do serviço.
 */
@Component
public class OrderListener {

    private static final Logger log = LoggerFactory.getLogger(OrderListener.class);

    private final OrderPlacedHandler handler;

    public OrderListener(OrderPlacedHandler handler) {
        this.handler = handler;
    }

    @KafkaListener(topics = Topics.ORDERS, groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderPlaced(OrderPlacedMessage message) {
        try {
            handler.handle(OrderPlacedTranslator.toDomainEvent(message));
        } catch (InvalidValueException malformed) {
            // Mensagem envenenada: registrar e seguir. Relançar faria o Kafka
            // reentregar o mesmo payload inválido para sempre, travando a partição
            // inteira atrás de uma mensagem que nunca vai ser aceita.
            log.warn("Mensagem descartada por payload inválido: {}", malformed.getMessage());
        }
    }
}
