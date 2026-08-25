package dev.joaolaureano.trainingkafka.analytics.domain.model;

import java.time.Duration;

/**
 * Os NÚMEROS da regra de suspeita — quantos pedidos, em quanto tempo.
 *
 * Só os números. A decisão de o que fazer com eles pertence a
 * {@link CustomerOrderPattern}, e é lá que ela está escrita. Esta separação é o
 * que torna a regra configurável sem que ela saia do agregado: o valor vem do
 * {@code application.yml}, mas quem lê configuração é a camada de aplicação, que
 * entrega esta policy já montada. O domínio nunca vê um arquivo de propriedades.
 */
public record SuspicionPolicy(int maxOrders, Duration window) {

    public SuspicionPolicy {
        if (maxOrders < 2) {
            throw new InvalidValueException("maxOrders deve ser pelo menos 2, recebido: " + maxOrders);
        }
        if (window == null || window.isZero() || window.isNegative()) {
            throw new InvalidValueException("window deve ser uma duração positiva");
        }
    }

    public static SuspicionPolicy of(int maxOrders, Duration window) {
        return new SuspicionPolicy(maxOrders, window);
    }
}
