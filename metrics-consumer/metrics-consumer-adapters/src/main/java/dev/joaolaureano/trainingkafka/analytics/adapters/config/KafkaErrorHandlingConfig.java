package dev.joaolaureano.trainingkafka.analytics.adapters.config;

import dev.joaolaureano.trainingkafka.analytics.adapters.messaging.Topics;
import dev.joaolaureano.trainingkafka.analytics.domain.model.InvalidValueException;
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
 * Antes disto, o listener engolia payload inválido com um {@code log.warn} — a
 * partição não travava, mas a mensagem sumia sem deixar rastro inspecionável.
 * Agora ela é republicada em {@code <tópico>-dlt}, com os cabeçalhos de
 * diagnóstico que o {@link DeadLetterPublishingRecoverer} anexa (exceção, causa,
 * tópico/partição/offset de origem).
 *
 * Tudo aqui é ligado, desligado e calibrado por {@code kafka.dlq.*} no
 * application.yml — não há decisão de DLQ escrita em código dentro do listener.
 */
@Configuration
@EnableConfigurationProperties(DeadLetterProperties.class)
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    /**
     * Produtor dedicado à DLQ.
     *
     * Ele não pode reaproveitar o KafkaTemplate de eventos de domínio: quando a
     * falha é de desserialização, o valor que chega ao recoverer é o array de bytes
     * cru — e um JsonSerializer o reserializaria como uma string base64. O
     * {@link DelegatingByTypeSerializer} resolve isso escolhendo o serializador
     * pelo tipo real do que está sendo reenviado, preservando o payload original
     * byte a byte quando é isso que temos em mãos.
     */
    @Bean
    @ConditionalOnProperty(prefix = "kafka.dlq", name = "enabled", havingValue = "true", matchIfMissing = true)
    public KafkaTemplate<Object, Object> deadLetterKafkaTemplate(KafkaProperties kafkaProperties) {
        Map<String, Object> producerProperties = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers(),
                ProducerConfig.ACKS_CONFIG, "all");

        // LinkedHashMap: a busca é por atribuição e o primeiro compatível vence,
        // então Object.class precisa ficar por último.
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
                // Partição -1: deixa o particionador escolher. Fixar a partição de
                // origem quebraria se a DLQ fosse criada com menos partições que o
                // tópico original.
                (record, exception) -> new TopicPartition(
                        properties.deadLetterTopicFor(record.topic()), -1));

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff(properties));
        // Payload inválido é erro permanente: retentar é só adiar o inevitável.
        errorHandler.addNotRetryableExceptions(InvalidValueException.class);
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
        errorHandler.addNotRetryableExceptions(InvalidValueException.class);
        return errorHandler;
    }

    @Bean
    @ConditionalOnProperty(prefix = "kafka.dlq", name = "create-topics", havingValue = "true", matchIfMissing = true)
    public NewTopic ordersDeadLetterTopic(DeadLetterProperties properties) {
        return TopicBuilder.name(properties.deadLetterTopicFor(Topics.ORDERS))
                .partitions(properties.getPartitions())
                .replicas(properties.getReplicas())
                .build();
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
