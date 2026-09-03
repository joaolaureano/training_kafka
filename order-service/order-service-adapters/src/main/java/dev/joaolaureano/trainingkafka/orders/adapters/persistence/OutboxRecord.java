package dev.joaolaureano.trainingkafka.orders.adapters.persistence;

/**
 * Uma linha do outbox: o que publicar, em qual tópico, com qual chave.
 *
 * O payload já é o JSON do contrato de fio — o mesmo que iria para o tópico se a
 * publicação fosse síncrona. Guardar o contrato, e não o evento de domínio,
 * significa que uma refatoração no agregado não invalida o que está pendente de
 * entrega no banco.
 */
public record OutboxRecord(long sequence, String topic, String partitionKey,
                           String eventType, String payload) {

    public OutboxRecord(String topic, String partitionKey, String eventType, String payload) {
        this(0L, topic, partitionKey, eventType, payload);
    }
}
