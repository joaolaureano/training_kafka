package dev.joaolaureano.trainingkafka.payment.adapters.messaging;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** O gatilho periódico do relay, separado para que o relay siga testável sem Spring. */
@Component
public class OutboxRelayScheduler {

    private final OutboxRelay relay;

    public OutboxRelayScheduler(OutboxRelay relay) {
        this.relay = relay;
    }

    @Scheduled(fixedDelayString = "${payment.outbox.relay-interval-ms:500}")
    public void drain() {
        relay.drain();
    }
}
