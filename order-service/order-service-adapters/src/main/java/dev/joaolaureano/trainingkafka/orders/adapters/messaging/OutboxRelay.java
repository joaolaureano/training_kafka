package dev.joaolaureano.trainingkafka.orders.adapters.messaging;

import dev.joaolaureano.trainingkafka.orders.adapters.persistence.OutboxDispatcher;
import dev.joaolaureano.trainingkafka.orders.adapters.persistence.OutboxRecord;
import dev.joaolaureano.trainingkafka.orders.adapters.persistence.OutboxStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Drena o outbox para o Kafka.
 *
 * Publica em ordem de sequência e para no primeiro erro: a próxima passagem
 * recomeça da mesma linha, então uma indisponibilidade do broker atrasa a entrega
 * mas não a perde nem a embaralha.
 *
 * A garantia é at-least-once, e deliberadamente: se o processo morrer entre o ack
 * do broker e a baixa da linha, o evento sai de novo. Quem consome — o
 * payment-service — é idempotente por orderId, que é onde essa duplicata é
 * absorvida. Trocar isso por exactly-once exigiria uma transação englobando Kafka
 * e SQLite, que não existe.
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
                // Parar aqui preserva a ordem: a linha seguinte não pode ultrapassar
                // esta. O retry vem na próxima passagem.
                log.warn("Outbox parado na linha {} ({}): {}", record.sequence(),
                        record.eventType(), failure.getMessage());
                break;
            }
        }
        return delivered;
    }
}
