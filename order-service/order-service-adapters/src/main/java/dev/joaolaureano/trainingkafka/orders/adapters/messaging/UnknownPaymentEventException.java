package dev.joaolaureano.trainingkafka.orders.adapters.messaging;

/**
 * Chegou em payment-events um tipo que este serviço não sabe interpretar.
 *
 * É erro permanente — retentar não vai fazer o contrato mudar — então vai direto
 * para a DLQ, onde a mensagem fica inspecionável em vez de sumir.
 */
public class UnknownPaymentEventException extends RuntimeException {

    public UnknownPaymentEventException(String eventType) {
        super("unknown payment event type: " + eventType);
    }
}
