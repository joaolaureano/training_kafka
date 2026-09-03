package dev.joaolaureano.trainingkafka.orders.adapters.config;

import dev.joaolaureano.trainingkafka.orders.adapters.messaging.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Cria os tópicos no boot.
 *
 * O broker está com auto-criação desligada de propósito, então declarar os
 * tópicos explicitamente é obrigatório — e é bom que seja: assim a contagem de
 * partições é uma decisão consciente, e não o default silencioso de 1 partição
 * que estrangularia o paralelismo dos consumidores.
 */
@Configuration
public class KafkaTopicsConfig {

    private static final int PARTITIONS = 3;
    private static final short REPLICAS = 1;

    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder.name(Topics.ORDERS)
                .partitions(PARTITIONS)
                .replicas(REPLICAS)
                .build();
    }

    @Bean
    public NewTopic applicationLogsTopic() {
        return TopicBuilder.name(Topics.AUDIT_EVENTS)
                .partitions(PARTITIONS)
                .replicas(REPLICAS)
                .build();
    }

    @Bean
    public NewTopic fraudEventsTopic() {
        return TopicBuilder.name(Topics.FRAUD_EVENTS)
                .partitions(PARTITIONS)
                .replicas(REPLICAS)
                .build();
    }

    @Bean
    public NewTopic paymentEventsTopic() {
        return TopicBuilder.name(Topics.PAYMENT_EVENTS)
                .partitions(PARTITIONS)
                .replicas(REPLICAS)
                .build();
    }
}
