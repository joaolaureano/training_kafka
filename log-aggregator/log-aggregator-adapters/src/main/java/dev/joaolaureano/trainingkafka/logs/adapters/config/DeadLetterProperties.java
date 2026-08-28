package dev.joaolaureano.trainingkafka.logs.adapters.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Configuração da Dead Letter Queue, lida do application.yml (prefixo {@code kafka.dlq}).
 *
 * Como {@code SuspicionProperties} do metrics-consumer, esta classe vive na borda: conhece o Spring e
 * traduz o arquivo de configuração em decisões de infraestrutura. Nenhuma linha do
 * domínio ou da aplicação sabe que existe DLQ.
 */
@ConfigurationProperties(prefix = "kafka.dlq")
public class DeadLetterProperties {

    /**
     * Desligado, a mensagem problemática é registrada e descartada — o comportamento
     * anterior. Ligado, ela é republicada no tópico de dead letter.
     */
    private boolean enabled = true;

    /** Sufixo aplicado ao nome do tópico de origem para formar o de dead letter. */
    private String topicSuffix = "-dlt";

    /**
     * Cria os tópicos de dead letter no boot. O broker está com auto-criação
     * desligada, então sem isto a republicação falharia com UNKNOWN_TOPIC.
     */
    private boolean createTopics = true;

    private int partitions = 3;

    private short replicas = 1;

    private final Retry retry = new Retry();

    /**
     * Retentativa antes de desistir e mandar para a DLQ.
     *
     * Vale para falhas transitórias (o banco caiu, a rede piscou). Payload inválido
     * é erro permanente e vai direto para a DLQ, sem consumir tentativa nenhuma.
     */
    public static class Retry {

        /** Reentregas depois da primeira falha. Zero significa DLQ na primeira. */
        private int attempts = 2;

        private Duration initialInterval = Duration.ofSeconds(1);

        private double multiplier = 2.0;

        private Duration maxInterval = Duration.ofSeconds(10);

        public int getAttempts() {
            return attempts;
        }

        public void setAttempts(int attempts) {
            this.attempts = attempts;
        }

        public Duration getInitialInterval() {
            return initialInterval;
        }

        public void setInitialInterval(Duration initialInterval) {
            this.initialInterval = initialInterval;
        }

        public double getMultiplier() {
            return multiplier;
        }

        public void setMultiplier(double multiplier) {
            this.multiplier = multiplier;
        }

        public Duration getMaxInterval() {
            return maxInterval;
        }

        public void setMaxInterval(Duration maxInterval) {
            this.maxInterval = maxInterval;
        }
    }

    public String deadLetterTopicFor(String sourceTopic) {
        return sourceTopic + topicSuffix;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getTopicSuffix() {
        return topicSuffix;
    }

    public void setTopicSuffix(String topicSuffix) {
        this.topicSuffix = topicSuffix;
    }

    public boolean isCreateTopics() {
        return createTopics;
    }

    public void setCreateTopics(boolean createTopics) {
        this.createTopics = createTopics;
    }

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    public short getReplicas() {
        return replicas;
    }

    public void setReplicas(short replicas) {
        this.replicas = replicas;
    }

    public Retry getRetry() {
        return retry;
    }
}
