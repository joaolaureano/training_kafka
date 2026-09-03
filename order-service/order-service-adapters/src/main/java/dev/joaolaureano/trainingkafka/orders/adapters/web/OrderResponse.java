package dev.joaolaureano.trainingkafka.orders.adapters.web;

import java.math.BigDecimal;

/** O contrato JSON de {@code GET /orders/{id}}. */
public record OrderResponse(String orderId, String status, String customerId, String product,
                            int quantity, BigDecimal amount, String placedAt) {
}
