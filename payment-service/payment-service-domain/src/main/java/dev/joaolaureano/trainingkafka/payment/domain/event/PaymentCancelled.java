package dev.joaolaureano.trainingkafka.payment.domain.event;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * A compensação do lado do dinheiro.
 *
 * {@code refunded} distingue os dois caminhos que chegam aqui: o pagamento já
 * tinha sido aprovado e foi estornado, ou ainda estava pendente e apenas deixou
 * de ser cobrado. O pedido cancela nos dois casos, mas a contabilidade não é a
 * mesma — e o evento seria uma mentira se dissesse "estornado" quando nada saiu.
 */
public record PaymentCancelled(String paymentId, String orderId, String customerId,
                               BigDecimal amount, Instant occurredAt, String reason,
                               String correlationId, boolean refunded) implements DomainEvent {
}
