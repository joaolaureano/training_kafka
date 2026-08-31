package dev.joaolaureano.trainingkafka.orders.bootstrap.facade;

import dev.joaolaureano.trainingkafka.orders.adapters.messaging.KafkaActivityLogPublisher;
import dev.joaolaureano.trainingkafka.orders.application.port.ActivityLog;
import dev.joaolaureano.trainingkafka.orders.application.port.ActivityLogPublisher;

import java.util.Objects;

/**
 * Lado de saída da mesma ideia: a aplicação declara o Port
 * {@link ActivityLogPublisher}, o adapter Kafka expõe um método em tipos crus, e
 * é aqui que os dois se encontram.
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
        publisher.publish(
                log.level().name(),
                log.message(),
                log.context(),
                log.occurredAt());
    }
}
