package dev.joaolaureano.trainingkafka.analytics.adapters.persistence.inmemory;

import dev.joaolaureano.trainingkafka.analytics.domain.model.CustomerId;
import dev.joaolaureano.trainingkafka.analytics.domain.model.CustomerOrderPattern;
import dev.joaolaureano.trainingkafka.analytics.domain.model.OrderPlacement;
import dev.joaolaureano.trainingkafka.analytics.domain.model.SuspicionPolicy;
import dev.joaolaureano.trainingkafka.analytics.domain.port.CustomerPatternRepository;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementação em memória de {@link CustomerPatternRepository}, para testes e
 * ambientes onde não vale a pena subir um banco de verdade.
 *
 * Guarda, por cliente, apenas a janela de pedidos recentes ({@link OrderPlacement}).
 * A policy nunca é armazenada — ela chega como parâmetro em cada
 * {@link #findOrCreate}, exatamente como o contrato exige.
 */
public class InMemoryCustomerPatternRepository implements CustomerPatternRepository {

    private final ConcurrentHashMap<CustomerId, List<OrderPlacement>> store = new ConcurrentHashMap<>();

    public InMemoryCustomerPatternRepository() {
    }

    @Override
    public CustomerOrderPattern findOrCreate(CustomerId customerId, SuspicionPolicy policy) {
        List<OrderPlacement> stored = store.get(customerId);
        if (stored == null) {
            return CustomerOrderPattern.startFor(customerId, policy);
        }
        // Reconstitui a partir da lista guardada, e não da instância do agregado:
        // aqui não guardamos o agregado inteiro (ele carrega a policy e eventos
        // pendentes), só a janela, que é o único dado persistente do contrato.
        return CustomerOrderPattern.reconstitute(customerId, policy, stored);
    }

    @Override
    public void save(CustomerOrderPattern pattern) {
        // "Apaga tudo e reinsere": a janela já vem podada pelo agregado, então
        // basta substituir a lista guardada pela atual para elas ficarem iguais.
        store.put(pattern.customerId(), List.copyOf(pattern.recentOrders()));
    }
}
