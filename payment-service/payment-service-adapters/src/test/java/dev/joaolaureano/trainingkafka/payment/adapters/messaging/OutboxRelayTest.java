package dev.joaolaureano.trainingkafka.payment.adapters.messaging;

import dev.joaolaureano.trainingkafka.payment.adapters.persistence.OutboxRecord;
import dev.joaolaureano.trainingkafka.payment.adapters.persistence.OutboxStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxRelayTest {

    @Test
    @DisplayName("entrega em ordem e dá baixa em cada linha")
    void drainsInOrder() {
        FakeStore store = new FakeStore(record(1, "PaymentApproved"), record(2, "PaymentCancelled"));
        List<Long> sent = new ArrayList<>();

        assertThat(new OutboxRelay(store, r -> sent.add(r.sequence()), 10).drain()).isEqualTo(2);
        assertThat(sent).containsExactly(1L, 2L);
        assertThat(store.pending(10)).isEmpty();
    }

    @Test
    @DisplayName("para na falha: o estorno não pode ultrapassar a aprovação do mesmo pedido")
    void stopsAtFirstFailurePreservingOrder() {
        FakeStore store = new FakeStore(record(1, "PaymentApproved"), record(2, "PaymentCancelled"));
        List<Long> sent = new ArrayList<>();

        OutboxRelay relay = new OutboxRelay(store, r -> {
            if (r.sequence() == 1L) {
                throw new IllegalStateException("broker fora do ar");
            }
            sent.add(r.sequence());
        }, 10);

        assertThat(relay.drain()).isZero();
        assertThat(sent).isEmpty();
        // Se a 2 tivesse passado, o order-service veria um cancelamento de um pedido
        // que para ele ainda nem foi pago.
        assertThat(store.pending(10)).extracting(OutboxRecord::sequence).containsExactly(1L, 2L);
    }

    @Test
    @DisplayName("a passagem seguinte retoma de onde parou")
    void resumesFromTheFailedRow() {
        FakeStore store = new FakeStore(record(1, "PaymentApproved"), record(2, "PaymentCancelled"));
        List<Long> sent = new ArrayList<>();
        boolean[] brokerDown = {true};

        OutboxRelay relay = new OutboxRelay(store, r -> {
            if (brokerDown[0]) {
                throw new IllegalStateException("broker fora do ar");
            }
            sent.add(r.sequence());
        }, 10);

        assertThat(relay.drain()).isZero();
        brokerDown[0] = false;

        assertThat(relay.drain()).isEqualTo(2);
        assertThat(sent).containsExactly(1L, 2L);
    }

    private static OutboxRecord record(long sequence, String eventType) {
        return new OutboxRecord(sequence, "payment-events", "order-1", eventType, "{}");
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
