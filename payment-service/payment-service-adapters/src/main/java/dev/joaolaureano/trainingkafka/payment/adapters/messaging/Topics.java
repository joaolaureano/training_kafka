package dev.joaolaureano.trainingkafka.payment.adapters.messaging;

public final class Topics {
    /** Gatilho da cobrança: o estoque já foi separado para este pedido. */
    public static final String INVENTORY_EVENTS = "inventory-events";
    public static final String PAYMENT_EVENTS = "payment-events";
    public static final String FRAUD_EVENTS = "fraud-events";
    public static final String AUDIT_EVENTS = "audit-events";

    private Topics() {
    }
}
