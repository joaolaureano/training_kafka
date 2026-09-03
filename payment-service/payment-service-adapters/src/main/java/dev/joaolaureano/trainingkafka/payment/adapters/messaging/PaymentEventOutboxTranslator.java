package dev.joaolaureano.trainingkafka.payment.adapters.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.joaolaureano.trainingkafka.payment.adapters.persistence.OutboxRecord;
import dev.joaolaureano.trainingkafka.payment.adapters.persistence.OutboxTranslator;
import dev.joaolaureano.trainingkafka.payment.domain.event.DomainEvent;
import dev.joaolaureano.trainingkafka.payment.domain.event.PaymentApproved;
import dev.joaolaureano.trainingkafka.payment.domain.event.PaymentCancelled;
import dev.joaolaureano.trainingkafka.payment.domain.event.PaymentFailed;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Toda a tradução para o fio está aqui: tipo do evento, chave de partição e JSON.
 *
 * A chave é o orderId — o order-service aplica os resultados de um mesmo pedido em
 * ordem, e é dessa ordem que a guarda contra transições contraditórias depende.
 */
public final class PaymentEventOutboxTranslator implements OutboxTranslator {

    private final ObjectMapper objectMapper;

    public PaymentEventOutboxTranslator(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public OutboxRecord translate(DomainEvent event) {
        PaymentEventMessage message = switch (event) {
            case PaymentApproved approved -> new PaymentEventMessage("PaymentApproved",
                    eventId("PaymentApproved", approved.paymentId()), approved.correlationId(),
                    approved.paymentId(), approved.orderId(), approved.customerId(),
                    approved.amount(), approved.occurredAt().toString(), null, false);
            case PaymentFailed failed -> new PaymentEventMessage("PaymentFailed",
                    eventId("PaymentFailed", failed.paymentId()), failed.correlationId(),
                    failed.paymentId(), failed.orderId(), failed.customerId(),
                    failed.amount(), failed.occurredAt().toString(), failed.reason(), false);
            case PaymentCancelled cancelled -> new PaymentEventMessage("PaymentCancelled",
                    eventId("PaymentCancelled", cancelled.paymentId()), cancelled.correlationId(),
                    cancelled.paymentId(), cancelled.orderId(), cancelled.customerId(),
                    cancelled.amount(), cancelled.occurredAt().toString(), cancelled.reason(),
                    cancelled.refunded());
            default -> throw new IllegalArgumentException(
                    "Evento de pagamento sem tradução: " + event.getClass().getSimpleName());
        };
        return new OutboxRecord(Topics.PAYMENT_EVENTS, message.orderId(), message.eventType(),
                serialize(message));
    }

    private String serialize(PaymentEventMessage message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("Could not serialize " + message.eventType(), failure);
        }
    }

    /**
     * Derivado do pagamento e do tipo, não aleatório: uma reentrega carrega o MESMO
     * eventId, e quem quiser deduplicar consegue.
     */
    private static String eventId(String type, String paymentId) {
        return UUID.nameUUIDFromBytes((type + ':' + paymentId).getBytes(StandardCharsets.UTF_8))
                .toString();
    }
}
