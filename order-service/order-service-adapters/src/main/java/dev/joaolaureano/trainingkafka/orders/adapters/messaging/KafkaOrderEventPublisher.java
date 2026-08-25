package dev.joaolaureano.trainingkafka.orders.adapters.messaging;

import dev.joaolaureano.trainingkafka.orders.domain.event.DomainEvent;
import dev.joaolaureano.trainingkafka.orders.domain.event.OrderPlaced;
import dev.joaolaureano.trainingkafka.orders.domain.port.OrderEventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Implementação Kafka do Port de saída do domínio.
 *
 * Toda a "kafkice" está confinada nesta classe: tópico, chave de partição,
 * serialização. O domínio nunca ouviu falar de nada disso.
 */
public class KafkaOrderEventPublisher implements OrderEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaOrderEventPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaOrderEventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void publish(DomainEvent event) {
        if (event instanceof OrderPlaced orderPlaced) {
            // A chave é o customerId: garante que todos os pedidos de um mesmo
            // cliente caiam na MESMA partição e sejam processados em ordem. É
            // disso que a detecção de padrão suspeito do App B depende para
            // enxergar uma rajada como sequência, e não como eventos soltos.
            String partitionKey = orderPlaced.customerId().value();

            kafkaTemplate.send(Topics.ORDERS, partitionKey, OrderPlacedMessage.from(orderPlaced));
            return;
        }

        log.warn("Evento de domínio sem tradução para o tópico de pedidos: {}",
                event.getClass().getSimpleName());
    }
}
