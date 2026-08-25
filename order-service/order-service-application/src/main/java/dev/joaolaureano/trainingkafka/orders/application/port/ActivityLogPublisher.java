package dev.joaolaureano.trainingkafka.orders.application.port;

/**
 * Port de saída da camada de aplicação — não do domínio.
 *
 * Observabilidade é preocupação da aplicação: o agregado Order não tem nada a
 * ganhar sabendo que alguém registra o que ele faz. Por isso esta interface mora
 * aqui, e não no módulo de domínio.
 */
public interface ActivityLogPublisher {

    void publish(ActivityLog log);
}
