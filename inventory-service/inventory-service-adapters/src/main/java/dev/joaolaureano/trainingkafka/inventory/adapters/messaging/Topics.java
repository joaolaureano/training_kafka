package dev.joaolaureano.trainingkafka.inventory.adapters.messaging;

public final class Topics {

    /** Entrada: o pedido recém-registrado, que dispara a reserva. */
    public static final String ORDERS = "orders";

    /** Saída: o veredito do estoque, que dispara (ou encerra) a cobrança. */
    public static final String INVENTORY_EVENTS = "inventory-events";

    /** Entrada: o desfecho do pagamento, que dispara a devolução ao estoque. */
    public static final String PAYMENT_EVENTS = "payment-events";

    /** Saída: os logs estruturados. */
    public static final String AUDIT_EVENTS = "audit-events";

    private Topics() {
    }
}
