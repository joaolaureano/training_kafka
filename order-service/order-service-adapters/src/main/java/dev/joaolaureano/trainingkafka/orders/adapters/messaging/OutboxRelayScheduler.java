package dev.joaolaureano.trainingkafka.orders.adapters.messaging;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * O gatilho periódico do relay.
 *
 * Fica separado de {@link OutboxRelay} para que o relay em si continue sendo um
 * objeto comum, testável sem contexto Spring nem relógio de agendamento.
 */
@Component
public class OutboxRelayScheduler {

    private final OutboxRelay relay;

    public OutboxRelayScheduler(OutboxRelay relay) {
        this.relay = relay;
    }

    @Scheduled(fixedDelayString = "${order.outbox.relay-interval-ms:500}")
    public void drain() {
        relay.drain();
    }
}
