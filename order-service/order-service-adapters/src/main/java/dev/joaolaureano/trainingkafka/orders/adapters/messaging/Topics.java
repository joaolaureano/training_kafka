package dev.joaolaureano.trainingkafka.orders.adapters.messaging;

public final class Topics {

    public static final String ORDERS = "orders";
    public static final String AUDIT_EVENTS = "audit-events";
    public static final String PAYMENT_EVENTS = "payment-events";
    /**
     * O App A não consome nem publica aqui — mas é ele quem declara os tópicos no
     * boot, porque a auto-criação está desligada e um só lugar de declaração evita
     * que dois serviços briguem pelo número de partições.
     */
    public static final String FRAUD_EVENTS = "fraud-events";

    private Topics() {
    }
}
