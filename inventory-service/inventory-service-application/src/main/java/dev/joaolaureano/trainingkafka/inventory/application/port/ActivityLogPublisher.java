package dev.joaolaureano.trainingkafka.inventory.application.port;

/**
 * Port de saída da camada de aplicação — não do domínio.
 *
 * Observabilidade é preocupação da aplicação: os agregados Product e Reservation
 * não têm nada a ganhar sabendo que alguém registra o que eles fazem.
 */
public interface ActivityLogPublisher {

    void publish(ActivityLog log);
}
