package dev.joaolaureano.trainingkafka.orders.adapters.messaging;

import dev.joaolaureano.trainingkafka.orders.adapters.persistence.OutboxRecord;
import dev.joaolaureano.trainingkafka.orders.adapters.persistence.OutboxStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxRelayTest {

    @Test
    @DisplayName("entrega em ordem e dá baixa em cada linha")
    void drainsInOrder() {
        FakeStore store = new FakeStore(record(1), record(2), record(3));
        List<Long> sent = new ArrayList<>();

        int delivered = new OutboxRelay(store, r -> { sent.add(r.sequence()); return done(); }, 10, 1).drain();

        assertThat(delivered).isEqualTo(3);
        assertThat(sent).containsExactly(1L, 2L, 3L);
        assertThat(store.pending(10)).isEmpty();
    }

    @Test
    @DisplayName("a baixa para na primeira falha, mesmo com o lote inteiro em voo")
    void stopsAtFirstFailurePreservingOrder() {
        FakeStore store = new FakeStore(record(1), record(2), record(3));
        List<Long> sent = new ArrayList<>();

        OutboxRelay relay = new OutboxRelay(store, r -> {
            if (r.sequence() == 2L) {
                // Falha do BROKER, não do envio: o future é que quebra, como na vida real.
                return CompletableFuture.failedFuture(new IllegalStateException("broker fora do ar"));
            }
            sent.add(r.sequence());
            return done();
        }, 10, 1);

        assertThat(relay.drain()).isEqualTo(1);
        /*
         * A 3 FOI despachada — o lote inteiro vai em voo de uma vez — mas não foi
         * confirmada. O que para no primeiro erro é a baixa, não o envio, e é essa
         * distinção que mantém o outbox seguro: a 2 e a 3 continuam pendentes e
         * saem de novo na próxima passagem. Duplicata é o preço do at-least-once, e
         * quem consome é idempotente; confirmar a 3 aqui seria PERDER a 2, que é o
         * que não se pode admitir.
         */
        assertThat(sent).containsExactly(1L, 3L);
        assertThat(store.pending(10)).extracting(OutboxRecord::sequence).containsExactly(2L, 3L);
    }

    @Test
    @DisplayName("a passagem seguinte retoma exatamente de onde parou")
    void resumesFromTheFailedRow() {
        FakeStore store = new FakeStore(record(1), record(2));
        List<Long> sent = new ArrayList<>();
        boolean[] brokerDown = {true};

        OutboxRelay relay = new OutboxRelay(store, r -> {
            if (brokerDown[0]) {
                return CompletableFuture.failedFuture(new IllegalStateException("broker fora do ar"));
            }
            sent.add(r.sequence());
            return done();
        }, 10, 1);

        assertThat(relay.drain()).isZero();
        brokerDown[0] = false;

        assertThat(relay.drain()).isEqualTo(2);
        assertThat(sent).containsExactly(1L, 2L);
    }

    private static CompletableFuture<Void> done() {
        return CompletableFuture.completedFuture(null);
    }

    private static OutboxRecord record(long sequence) {
        return new OutboxRecord(sequence, "orders", "cust-1", "OrderPlaced", "{}");
    }

    private static final class FakeStore implements OutboxStore {
        private final Map<Long, OutboxRecord> rows = new LinkedHashMap<>();

        FakeStore(OutboxRecord... records) {
            for (OutboxRecord record : records) {
                rows.put(record.sequence(), record);
            }
        }

        @Override
        public List<OutboxRecord> pending(int limit) {
            return rows.values().stream().limit(limit).toList();
        }

        @Override
        public void markPublished(long sequence) {
            rows.remove(sequence);
        }
    }
}
