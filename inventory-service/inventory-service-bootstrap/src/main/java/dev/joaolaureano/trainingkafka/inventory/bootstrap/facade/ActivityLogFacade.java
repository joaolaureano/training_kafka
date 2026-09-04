package dev.joaolaureano.trainingkafka.inventory.bootstrap.facade;

import dev.joaolaureano.trainingkafka.inventory.adapters.messaging.KafkaActivityLogPublisher;
import dev.joaolaureano.trainingkafka.inventory.application.port.ActivityLog;
import dev.joaolaureano.trainingkafka.inventory.application.port.ActivityLogPublisher;

import java.util.Objects;

/**
 * A aplicação declara o Port {@link ActivityLogPublisher}, o adapter Kafka expõe
 * um método em tipos crus, e é aqui que os dois se encontram.
 *
 * Sem esta classe, o adapter precisaria de {@code implements ActivityLogPublisher}
 * — e voltaria a compilar contra o módulo -application.
 */
public class ActivityLogFacade implements ActivityLogPublisher {

    private final KafkaActivityLogPublisher publisher;

    public ActivityLogFacade(KafkaActivityLogPublisher publisher) {
        this.publisher = Objects.requireNonNull(publisher);
    }

    @Override
    public void publish(ActivityLog log) {
        publisher.publish(log.level().name(), log.action(), log.context(), log.occurredAt());
    }
}
