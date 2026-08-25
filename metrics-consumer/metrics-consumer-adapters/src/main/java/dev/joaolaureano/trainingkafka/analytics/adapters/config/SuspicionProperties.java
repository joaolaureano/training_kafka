package dev.joaolaureano.trainingkafka.analytics.adapters.config;

import dev.joaolaureano.trainingkafka.analytics.domain.model.SuspicionPolicy;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Lê os números da regra de suspeita do application.yml e os entrega ao domínio
 * já embrulhados numa {@link SuspicionPolicy}.
 *
 * Esta classe é a fronteira: ela conhece o Spring E o domínio. Do lado de dentro
 * do domínio, ninguém sabe que existe um arquivo de configuração.
 */
@ConfigurationProperties(prefix = "analytics.suspicion")
public class SuspicionProperties {

    /** Quantos pedidos, dentro da janela, caracterizam um padrão suspeito. */
    private int maxOrders = 5;

    /** O tamanho da janela deslizante. */
    private Duration window = Duration.ofSeconds(10);

    public SuspicionPolicy toPolicy() {
        return SuspicionPolicy.of(maxOrders, window);
    }

    public int getMaxOrders() {
        return maxOrders;
    }

    public void setMaxOrders(int maxOrders) {
        this.maxOrders = maxOrders;
    }

    public Duration getWindow() {
        return window;
    }

    public void setWindow(Duration window) {
        this.window = window;
    }
}
