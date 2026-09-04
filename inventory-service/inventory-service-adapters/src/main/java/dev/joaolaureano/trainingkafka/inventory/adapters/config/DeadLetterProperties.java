package dev.joaolaureano.trainingkafka.inventory.adapters.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Configuração da Dead Letter Queue, lida do application.yml (prefixo {@code kafka.dlq}).
 *
 * Mesma forma da que os demais serviços usam: a decisão de DLQ é de infraestrutura e
 * mora na borda. Nenhuma linha do domínio ou da aplicação sabe que ela existe.
 */
@ConfigurationProperties(prefix = "kafka.dlq")
public class DeadLetterProperties {

    private boolean enabled = true;
    private String topicSuffix = "-dlt";
    private boolean createTopics = true;
    private int partitions = 3;
    private short replicas = 1;
    private final Retry retry = new Retry();

    /**
     * Retentativa antes de desistir e mandar para a DLQ. Vale para falha
     * transitória; erro permanente vai direto, sem consumir tentativa.
     */
    public static class Retry {
        private int attempts = 2;
        private Duration initialInterval = Duration.ofSeconds(1);
        private double multiplier = 2.0;
        private Duration maxInterval = Duration.ofSeconds(10);

        public int getAttempts() { return attempts; }
        public void setAttempts(int attempts) { this.attempts = attempts; }
        public Duration getInitialInterval() { return initialInterval; }
        public void setInitialInterval(Duration initialInterval) { this.initialInterval = initialInterval; }
        public double getMultiplier() { return multiplier; }
        public void setMultiplier(double multiplier) { this.multiplier = multiplier; }
        public Duration getMaxInterval() { return maxInterval; }
        public void setMaxInterval(Duration maxInterval) { this.maxInterval = maxInterval; }
    }

    public String deadLetterTopicFor(String sourceTopic) {
        return sourceTopic + topicSuffix;
    }

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public String getTopicSuffix() { return topicSuffix; }
    public void setTopicSuffix(String topicSuffix) { this.topicSuffix = topicSuffix; }
    public boolean isCreateTopics() { return createTopics; }
    public void setCreateTopics(boolean createTopics) { this.createTopics = createTopics; }
    public int getPartitions() { return partitions; }
    public void setPartitions(int partitions) { this.partitions = partitions; }
    public short getReplicas() { return replicas; }
    public void setReplicas(short replicas) { this.replicas = replicas; }
    public Retry getRetry() { return retry; }
}
