package dev.joaolaureano.trainingkafka.orders.domain.model;

/**
 * Os estados por que um pedido passa, na ordem em que a Saga os produz.
 *
 * PENDING_STOCK é o primeiro porque reservar vem antes de cobrar: um pedido sem
 * estoque morre aqui, sem nunca ter tocado no dinheiro. Só depois de as unidades
 * estarem separadas o pedido passa a PENDING_PAYMENT e o pagamento entra em cena.
 */
public enum OrderStatus {
    PENDING_STOCK,
    PENDING_PAYMENT,
    PAID,
    CANCELLED
}
