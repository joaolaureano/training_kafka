package dev.joaolaureano.trainingkafka.analytics.domain.port;

import dev.joaolaureano.trainingkafka.analytics.domain.model.CustomerId;
import dev.joaolaureano.trainingkafka.analytics.domain.model.CustomerOrderPattern;
import dev.joaolaureano.trainingkafka.analytics.domain.model.SuspicionPolicy;

/** Port de saída para o agregado {@link CustomerOrderPattern}. */
public interface CustomerPatternRepository {

    /**
     * Devolve o padrão do cliente, reconstituído com a policy informada, ou um
     * padrão vazio se o cliente ainda não tem histórico.
     *
     * A policy é parâmetro, e não estado guardado: mudar o limiar no
     * {@code application.yml} deve valer imediatamente, sem migrar dado nenhum.
     */
    CustomerOrderPattern findOrCreate(CustomerId customerId, SuspicionPolicy policy);

    void save(CustomerOrderPattern pattern);
}
