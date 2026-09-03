package dev.joaolaureano.trainingkafka.fraud.adapters.messaging;

public final class Topics {

    public static final String ORDERS = "orders";
    public static final String AUDIT_EVENTS = "audit-events";
    public static final String FRAUD_EVENTS = "fraud-events";
    public static final String FRAUD_STATE_STORE = "customer-fraud-patterns";

    private Topics() {
    }
}
