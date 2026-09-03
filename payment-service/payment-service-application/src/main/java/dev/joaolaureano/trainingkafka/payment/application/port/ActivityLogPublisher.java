package dev.joaolaureano.trainingkafka.payment.application.port;

/**
 * Port de saída da camada de aplicação — não do domínio.
 *
 * Observabilidade é preocupação da aplicação: o agregado Payment não tem nada a
 * ganhar sabendo que alguém registra o que ele faz.
 *
 * Isto NÃO passa pelo outbox, e a diferença importa. O registro contábil do que
 * aconteceu com o dinheiro é a tabela {@code payments} mais o
 * {@code payment-events} — esses são transacionais. O que vai para
 * {@code audit-events} é a visão pesquisável desses fatos: perder uma linha num
 * crash é aceitável, e amarrá-la à transação do pagamento faria telemetria
 * atrasar dinheiro.
 */
public interface ActivityLogPublisher {

    void publish(ActivityLog log);
}
