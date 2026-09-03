package dev.joaolaureano.trainingkafka.fraud.domain.event;

import dev.joaolaureano.trainingkafka.fraud.domain.model.FraudOrder;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;

/**
 * A rajada que cruzou a política.
 *
 * Carrega a janela INTEIRA, não uma amostra. Enquanto o evento servia só para
 * alertar, os últimos cinco pedidos bastavam para dar contexto a quem lesse o
 * log. Agora ele também dispara compensação — e compensar meia rajada deixaria o
 * cliente com pedidos fraudulentos pagos, que é pior do que não compensar nada.
 *
 * O valor de cada pedido viaja junto porque quem compensa pode não ter visto o
 * OrderPlaced ainda: com {@code orderId} e {@code amount} o payment-service
 * consegue registrar o pagamento já cancelado e impedir a cobrança que ainda
 * está a caminho.
 */
public record FraudDetected(
        String customerId,
        Duration window,
        List<FraudOrder> orders,
        Instant occurredAt) {

    private static final int SAMPLE_SIZE = 5;

    public FraudDetected {
        if (customerId == null || customerId.isBlank()) {
            throw new IllegalArgumentException("customerId is required");
        }
        if (orders == null || orders.isEmpty()) {
            throw new IllegalArgumentException("orders is required");
        }
        orders = List.copyOf(orders);
    }

    public int ordersInWindow() {
        return orders.size();
    }

    /** Só para o alerta legível: os mais recentes, em ordem de acontecimento. */
    public List<String> sampleOrderIds() {
        return orders.stream()
                .sorted(Comparator.comparing(FraudOrder::occurredAt))
                .skip(Math.max(0, orders.size() - SAMPLE_SIZE))
                .map(FraudOrder::orderId)
                .toList();
    }
}
