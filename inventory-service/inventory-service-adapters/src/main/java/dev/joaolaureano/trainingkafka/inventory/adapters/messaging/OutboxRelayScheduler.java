package dev.joaolaureano.trainingkafka.inventory.adapters.messaging;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** O gatilho periódico do relay, separado para que o relay siga testável sem Spring. */
@Component
public class OutboxRelayScheduler {

    private final OutboxRelay relay;

    public OutboxRelayScheduler(OutboxRelay relay) {
        this.relay = relay;
    }

    @Scheduled(fixedDelayString = "${inventory.outbox.relay-interval-ms:200}")
    public void drain() {
        relay.drain();
    }
}
