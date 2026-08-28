package dev.joaolaureano.trainingkafka.analytics.adapters.messaging;

import dev.joaolaureano.trainingkafka.analytics.application.OrderPlacedHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Adapter de entrada: consome o tópico "orders".
 *
 * Traduz e delega — nada mais. A anotação {@code @KafkaListener} vive aqui, na
 * borda, e em nenhum outro lugar do serviço.
 *
 * Não há try/catch: a falha sobe para o error handler do container, que decide
 * entre retentar e mandar para a DLQ conforme {@code kafka.dlq.*}. Engolir a
 * exceção aqui era o que impedia essa política de existir — e fazia a mensagem
 * problemática desaparecer sem deixar rastro inspecionável.
 */
@Component
public class OrderListener {

    private final OrderPlacedHandler handler;

    public OrderListener(OrderPlacedHandler handler) {
        this.handler = handler;
    }

    @KafkaListener(topics = Topics.ORDERS, groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderPlaced(OrderPlacedMessage message) {
        handler.handle(OrderPlacedTranslator.toDomainEvent(message));
    }
}
