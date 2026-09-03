package dev.joaolaureano.trainingkafka.payment.bootstrap.facade;

import dev.joaolaureano.trainingkafka.payment.adapters.messaging.KafkaActivityLogPublisher;
import dev.joaolaureano.trainingkafka.payment.application.port.ActivityLog;
import dev.joaolaureano.trainingkafka.payment.application.port.ActivityLogPublisher;

import java.util.Objects;

/**
 * A aplicação declara o Port, o adapter Kafka expõe um método em tipos crus, e é
 * aqui que os dois se encontram — sem que nenhum dos dois conheça o outro.
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
