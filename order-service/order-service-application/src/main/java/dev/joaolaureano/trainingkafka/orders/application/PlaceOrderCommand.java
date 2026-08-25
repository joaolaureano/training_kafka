package dev.joaolaureano.trainingkafka.orders.application;

import java.math.BigDecimal;

/**
 * Entrada do caso de uso, em tipos primitivos.
 *
 * É de propósito: a fronteira da aplicação aceita dados crus vindos do mundo
 * externo e os traduz em value objects. Se este record já exigisse
 * {@code CustomerId}, a tradução — e portanto a validação — teria vazado para o
 * adapter web.
 */
public record PlaceOrderCommand(
        String customerId,
        String product,
        Integer quantity,
        BigDecimal amount
) {
}
