package dev.joaolaureano.trainingkafka.payment.adapters.persistence;

import java.util.concurrent.CompletableFuture;

/**
 * Envia uma linha do outbox para o destino, SEM esperar a confirmação.
 *
 * Esperar era o desenho anterior, e ele impunha um teto: um round-trip por
 * linha, com o relay parado no meio. Devolvendo o future, o lote inteiro fica em
 * voo de uma vez e o relay confirma em ordem depois — a ordenação continua
 * garantida pelo produtor (idempotente, com uma requisição em voo por conexão),
 * não pelo bloqueio.
 */
@FunctionalInterface
public interface OutboxDispatcher {

    CompletableFuture<Void> dispatch(OutboxRecord record);
}
