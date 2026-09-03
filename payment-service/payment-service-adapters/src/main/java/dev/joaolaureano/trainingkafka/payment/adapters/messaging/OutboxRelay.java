package dev.joaolaureano.trainingkafka.payment.adapters.messaging;

import dev.joaolaureano.trainingkafka.payment.adapters.persistence.OutboxDispatcher;
import dev.joaolaureano.trainingkafka.payment.adapters.persistence.OutboxRecord;
import dev.joaolaureano.trainingkafka.payment.adapters.persistence.OutboxStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Drena o outbox para o Kafka.
 *
 * Duas coisas o mantêm correto sob carga:
 *
 * O lote vai em voo de uma vez, e só então as confirmações são aguardadas em
 * ordem. A versão anterior esperava cada envio antes do próximo, o que impunha um
 * teto de um round-trip por evento — com um lote de 100 a cada 500ms, cerca de
 * 170 eventos/s, enquanto a camada HTTP aceita ordens de magnitude mais. O
 * backlog crescia durante a carga e só drenava depois dela.
 *
 * E a passagem drena até esvaziar, em vez de um lote por tick. Um pico não fica
 * represado esperando o próximo agendamento.
 *
 * A ordem se preserva sem o bloqueio: o produtor é idempotente e limitado a uma
 * requisição em voo por conexão, então a sequência por partição é a de envio. E
 * a baixa para no primeiro erro — o que não foi confirmado é reenviado na
 * próxima passagem, at-least-once, absorvido pela idempotência de quem consome.
 */
public final class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxStore outbox;
    private final OutboxDispatcher dispatcher;
    private final int batchSize;
    private final long confirmTimeoutSeconds;

    public OutboxRelay(OutboxStore outbox, OutboxDispatcher dispatcher, int batchSize,
                       long confirmTimeoutSeconds) {
        this.outbox = outbox;
        this.dispatcher = dispatcher;
        this.batchSize = batchSize;
        this.confirmTimeoutSeconds = confirmTimeoutSeconds;
    }

    /** Drena até esvaziar. Devolve quantas linhas foram entregues. */
    public int drain() {
        int delivered = 0;
        while (true) {
            int batch = drainOnce();
            delivered += batch;
            // Lote incompleto significa fila vazia; lote cheio, pico ainda represado.
            if (batch < batchSize) {
                return delivered;
            }
        }
    }

    private int drainOnce() {
        List<OutboxRecord> pending = outbox.pending(batchSize);
        if (pending.isEmpty()) {
            return 0;
        }

        List<CompletableFuture<Void>> inFlight = new ArrayList<>(pending.size());
        for (OutboxRecord record : pending) {
            try {
                inFlight.add(dispatcher.dispatch(record));
            } catch (RuntimeException failure) {
                log.warn("Outbox não conseguiu enviar a linha {} ({}): {}", record.sequence(),
                        record.eventType(), failure.getMessage());
                break;
            }
        }

        int confirmed = 0;
        for (int i = 0; i < inFlight.size(); i++) {
            OutboxRecord record = pending.get(i);
            try {
                inFlight.get(i).get(confirmTimeoutSeconds, TimeUnit.SECONDS);
                outbox.markPublished(record.sequence());
                confirmed++;
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception failure) {
                // Parar aqui preserva a ordem da baixa: o que vier depois é reenviado
                // na próxima passagem, não confirmado por engano.
                log.warn("Outbox parado na linha {} ({}): {}", record.sequence(),
                        record.eventType(), failure.getMessage());
                break;
            }
        }
        return confirmed;
    }
}
