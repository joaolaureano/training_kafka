package dev.joaolaureano.trainingkafka.payment.adapters.persistence;

/**
 * Entrega uma linha do outbox no destino, esperando confirmação.
 *
 * Existe para que o relay não precise conhecer Kafka: despachar ou confirma, ou
 * levanta — e nesse caso ele para e tenta de novo.
 */
@FunctionalInterface
public interface OutboxDispatcher {

    void dispatch(OutboxRecord record) throws Exception;
}
