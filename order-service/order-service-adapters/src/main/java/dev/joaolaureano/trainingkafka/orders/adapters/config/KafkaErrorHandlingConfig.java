package dev.joaolaureano.trainingkafka.orders.adapters.config;

import dev.joaolaureano.trainingkafka.orders.adapters.messaging.Topics;
import dev.joaolaureano.trainingkafka.orders.adapters.messaging.UnknownPaymentEventException;
import dev.joaolaureano.trainingkafka.orders.domain.model.InvalidOrderException;
import dev.joaolaureano.trainingkafka.orders.domain.model.InvalidOrderTransitionException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Tratamento de erro de consumo: retentativa para falha transitória, Dead Letter
 * Queue para o que nunca vai dar certo.
 *
 * Tudo é ligado, desligado e calibrado por {@code kafka.dlq.*} no application.yml
 * — não há decisão de DLQ escrita em código dentro do listener.
 */
@Configuration
@EnableConfigurationProperties(DeadLetterProperties.class)
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    /**
     * Produtor dedicado à DLQ.
     *
     * Não pode reaproveitar o KafkaTemplate de eventos: quando a falha é de
     * desserialização, o valor que chega ao recoverer é o array de bytes cru, e um
     * JsonSerializer o reserializaria como string base64. O tipo declarado é
     * {@link KafkaOperations} de propósito — declarar como KafkaTemplate faria a
     * autoconfiguração recuar e o template de eventos deixaria de existir.
     */
    @Bean
    @ConditionalOnProperty(prefix = "kafka.dlq", name = "enabled", havingValue = "true", matchIfMissing = true)
    public KafkaOperations<Object, Object> deadLetterKafkaTemplate(KafkaProperties kafkaProperties) {
        Map<String, Object> producerProperties = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers(),
                ProducerConfig.ACKS_CONFIG, "all");

        // LinkedHashMap: o primeiro compatível vence, então Object.class fica por último.
        Map<Class<?>, Serializer<?>> delegates = new LinkedHashMap<>();
        delegates.put(byte[].class, new ByteArraySerializer());
        delegates.put(String.class, new StringSerializer());
        delegates.put(Object.class, new JsonSerializer<>().noTypeInfo());

        DelegatingByTypeSerializer serializer = new DelegatingByTypeSerializer(delegates, true);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProperties, serializer, serializer));
    }

    @Bean
    @ConditionalOnProperty(prefix = "kafka.dlq", name = "enabled", havingValue = "true", matchIfMissing = true)
    public CommonErrorHandler deadLetterErrorHandler(KafkaOperations<Object, Object> deadLetterKafkaTemplate,
                                                     DeadLetterProperties properties) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                deadLetterKafkaTemplate,
                (record, exception) -> new TopicPartition(
                        properties.deadLetterTopicFor(record.topic()), -1));

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff(properties));
        notRetryable(errorHandler);
        return errorHandler;
    }

    /** Fallback com DLQ desligada: registra e segue, sem travar a partição. */
    @Bean
    @ConditionalOnProperty(prefix = "kafka.dlq", name = "enabled", havingValue = "false")
    public CommonErrorHandler logAndSkipErrorHandler(DeadLetterProperties properties) {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                (record, exception) -> log.warn(
                        "Mensagem descartada (DLQ desligada) de {}-{}@{}: {}",
                        record.topic(), record.partition(), record.offset(), exception.getMessage()),
                backOff(properties));
        notRetryable(errorHandler);
        return errorHandler;
    }

    @Bean
    @ConditionalOnProperty(prefix = "kafka.dlq", name = "create-topics", havingValue = "true", matchIfMissing = true)
    public NewTopic paymentEventsDeadLetterTopic(DeadLetterProperties properties) {
        return TopicBuilder.name(properties.deadLetterTopicFor(Topics.PAYMENT_EVENTS))
                .partitions(properties.getPartitions())
                .replicas(properties.getReplicas())
                .build();
    }

    @Bean
    @ConditionalOnProperty(prefix = "kafka.dlq", name = "create-topics", havingValue = "true", matchIfMissing = true)
    public NewTopic inventoryEventsDeadLetterTopic(DeadLetterProperties properties) {
        return TopicBuilder.name(properties.deadLetterTopicFor(Topics.INVENTORY_EVENTS))
                .partitions(properties.getPartitions())
                .replicas(properties.getReplicas())
                .build();
    }

    /** Erro permanente: retentar é só adiar o inevitável. */
    private static void notRetryable(DefaultErrorHandler errorHandler) {
        errorHandler.addNotRetryableExceptions(
                InvalidOrderException.class, InvalidOrderTransitionException.class,
                UnknownPaymentEventException.class);
    }

    private static BackOff backOff(DeadLetterProperties properties) {
        DeadLetterProperties.Retry retry = properties.getRetry();
        if (retry.getAttempts() <= 0) {
            return new FixedBackOff(0L, 0L);
        }
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(retry.getAttempts());
        backOff.setInitialInterval(retry.getInitialInterval().toMillis());
        backOff.setMultiplier(retry.getMultiplier());
        backOff.setMaxInterval(retry.getMaxInterval().toMillis());
        return backOff;
    }
}
