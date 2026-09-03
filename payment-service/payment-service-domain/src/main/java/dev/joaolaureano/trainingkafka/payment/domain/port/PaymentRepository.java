package dev.joaolaureano.trainingkafka.payment.domain.port;

import dev.joaolaureano.trainingkafka.payment.domain.event.DomainEvent;
import dev.joaolaureano.trainingkafka.payment.domain.model.Payment;

import java.util.List;
import java.util.Optional;

/**
 * O estado do pagamento e os fatos que ele ainda deve ao mundo.
 *
 * Mesma forma do lado do pedido, e pelo mesmo motivo: {@link #save(Payment, List)}
 * é a promessa de que gravar o desfecho e registrar o evento acontece numa
 * transação só. Sem ela, um processo morto entre as duas coisas deixaria um
 * pagamento resolvido cujo resultado ninguém nunca soube — e o pedido preso do
 * outro lado.
 */
public interface PaymentRepository {

    Optional<Payment> findByOrderId(String orderId);

    /** Grava o pagamento e enfileira seus eventos atomicamente. */
    void save(Payment payment, List<DomainEvent> events);

    /** Grava só o estado, quando a operação não produziu fato novo. */
    void save(Payment payment);
}
