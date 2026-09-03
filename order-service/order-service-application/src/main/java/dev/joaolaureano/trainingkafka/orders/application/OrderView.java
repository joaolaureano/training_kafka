package dev.joaolaureano.trainingkafka.orders.application;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * O pedido como quem pergunta de fora precisa ver.
 *
 * Não é o agregado: devolver {@code Order} pela borda exporia
 * {@code approvePayment()} e {@code cancelForFraud()} a quem só quer consultar, e
 * um dia alguém chamaria. Consulta não muda estado, e o tipo devolvido diz isso.
 */
public record OrderView(String orderId, String status, String customerId, String product,
                        int quantity, BigDecimal amount, Instant placedAt) {
}
