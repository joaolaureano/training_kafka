package dev.joaolaureano.trainingkafka.orders.adapters.persistence;

import java.util.List;

/** Leitura e baixa das linhas pendentes — o lado do outbox que o relay enxerga. */
public interface OutboxStore {

    List<OutboxRecord> pending(int limit);

    void markPublished(long sequence);
}
