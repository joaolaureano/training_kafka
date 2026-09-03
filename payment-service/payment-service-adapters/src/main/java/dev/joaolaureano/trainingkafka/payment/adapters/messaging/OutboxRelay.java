package dev.joaolaureano.trainingkafka.payment.adapters.messaging;

import dev.joaolaureano.trainingkafka.payment.adapters.persistence.OutboxDispatcher;
import dev.joaolaureano.trainingkafka.payment.adapters.persistence.OutboxRecord;
import dev.joaolaureano.trainingkafka.payment.adapters.persistence.OutboxStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Drena o outbox para o Kafka.
 *
 * Publica em ordem de sequência e para no primeiro erro — importa mais aqui do que
 * no lado do pedido: um PaymentCancelled que ultrapassasse o PaymentApproved do
 * mesmo pedido chegaria como contradição e iria para a DLQ.
 *
 * A garantia é at-least-once: morrer entre o ack do broker e a baixa da linha
 * reemite o evento. O order-service absorve a duplicata na guarda do agregado.
 */
public final class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxStore outbox;
    private final OutboxDispatcher dispatcher;
    private final int batchSize;

    public OutboxRelay(OutboxStore outbox, OutboxDispatcher dispatcher, int batchSize) {
        this.outbox = outbox;
        this.dispatcher = dispatcher;
        this.batchSize = batchSize;
    }

    /** Uma passagem pelo outbox. Devolve quantas linhas foram entregues. */
    public int drain() {
        List<OutboxRecord> pending = outbox.pending(batchSize);
        int delivered = 0;
        for (OutboxRecord record : pending) {
            try {
                dispatcher.dispatch(record);
                outbox.markPublished(record.sequence());
                delivered++;
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception failure) {
                log.warn("Outbox parado na linha {} ({}): {}", record.sequence(),
                        record.eventType(), failure.getMessage());
                break;
            }
        }
        return delivered;
    }
}
