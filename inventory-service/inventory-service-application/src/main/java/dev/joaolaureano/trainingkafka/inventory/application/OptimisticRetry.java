package dev.joaolaureano.trainingkafka.inventory.application;

import dev.joaolaureano.trainingkafka.inventory.domain.port.ConcurrentStockChangeException;

/**
 * Repete uma decisão que perdeu a corrida do bloqueio otimista.
 *
 * Perder a corrida não é falha: dois pedidos disputando a última unidade é o caso
 * normal, e o perdedor precisa reler o estoque e decidir de novo — pode ser que
 * ainda haja unidades, pode ser que não haja mais. Reler é obrigatório; repetir a
 * gravação com o agregado velho apenas repetiria o conflito.
 *
 * Esgotadas as tentativas, a exceção sobe. Aí o error handler do container
 * reentrega a mensagem, o que é a mesma retentativa numa escala de tempo maior —
 * e se nem assim passar, a mensagem vai para a DLQ e o problema fica visível em
 * vez de silencioso.
 */
final class OptimisticRetry {

    private OptimisticRetry() {
    }

    static void run(int attempts, Runnable decision) {
        ConcurrentStockChangeException lastConflict = null;
        for (int attempt = 0; attempt < attempts; attempt++) {
            try {
                decision.run();
                return;
            } catch (ConcurrentStockChangeException conflict) {
                lastConflict = conflict;
            }
        }
        throw new IllegalStateException(
                "estoque em disputa: " + attempts + " tentativas sem sucesso", lastConflict);
    }
}
