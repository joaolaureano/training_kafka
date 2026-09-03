package dev.joaolaureano.trainingkafka.orders.adapters.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.joaolaureano.trainingkafka.orders.adapters.persistence.OutboxRecord;
import dev.joaolaureano.trainingkafka.orders.adapters.persistence.OutboxTranslator;
import dev.joaolaureano.trainingkafka.orders.domain.event.DomainEvent;
import dev.joaolaureano.trainingkafka.orders.domain.event.OrderPlaced;

/**
 * Toda a "kafkice" do evento de saída está confinada aqui: tópico, chave de
 * partição e serialização. O domínio nunca ouviu falar de nada disso — a única
 * diferença em relação a publicar direto é que o destino imediato é uma tabela,
 * não um broker.
 */
public final class OrderEventOutboxTranslator implements OutboxTranslator {

    private final ObjectMapper objectMapper;

    public OrderEventOutboxTranslator(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public OutboxRecord translate(DomainEvent event) {
        if (!(event instanceof OrderPlaced orderPlaced)) {
            throw new IllegalArgumentException(
                    "Evento de domínio sem tradução para o tópico de pedidos: "
                            + event.getClass().getSimpleName());
        }
        // A chave é o customerId: garante que todos os pedidos de um mesmo cliente
        // caiam na MESMA partição e sejam processados em ordem. É disso que a
        // detecção de padrão suspeito do App B depende para enxergar uma rajada
        // como sequência, e não como eventos soltos.
        return new OutboxRecord(Topics.ORDERS, orderPlaced.customerId().value(),
                "OrderPlaced", serialize(OrderPlacedMessage.from(orderPlaced)));
    }

    private String serialize(OrderPlacedMessage message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("Could not serialize OrderPlaced", failure);
        }
    }
}
