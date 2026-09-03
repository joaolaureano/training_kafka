package dev.joaolaureano.trainingkafka.payment.adapters.messaging;

public final class Topics {
    public static final String ORDERS = "orders";
    public static final String PAYMENT_EVENTS = "payment-events";
    public static final String FRAUD_EVENTS = "fraud-events";
    public static final String AUDIT_EVENTS = "audit-events";

    private Topics() {
    }
}
