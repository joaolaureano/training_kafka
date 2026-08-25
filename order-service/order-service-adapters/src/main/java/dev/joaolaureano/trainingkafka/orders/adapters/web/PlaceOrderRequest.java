package dev.joaolaureano.trainingkafka.orders.adapters.web;

import java.math.BigDecimal;

/**
 * Payload HTTP de entrada — puro transporte, sem uma única regra.
 *
 * Não há Bean Validation aqui de propósito. Anotar {@code @Positive} neste record
 * criaria uma segunda cópia de uma regra que já existe em
 * {@code Order.place}, livre para divergir dela com o tempo. A validação acontece
 * num lugar só: o domínio.
 */
public record PlaceOrderRequest(
        String customerId,
        String product,
        Integer quantity,
        BigDecimal amount
) {
}
