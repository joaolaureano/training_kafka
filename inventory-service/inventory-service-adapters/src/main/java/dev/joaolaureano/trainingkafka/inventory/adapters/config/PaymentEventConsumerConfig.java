package dev.joaolaureano.trainingkafka.inventory.adapters.config;

import dev.joaolaureano.trainingkafka.inventory.adapters.messaging.PaymentEventMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Container próprio para {@code payment-events}.
 *
 * O consumidor padrão do serviço está fixado em {@code OrderPlacedMessage} — sem
 * cabeçalho de tipo na mensagem, que é o acoplamento que se quis evitar, o tipo
 * precisa ser decidido na configuração. Dois tópicos com contratos diferentes,
 * portanto, exigem dois containers.
 */
@Configuration
public class PaymentEventConsumerConfig {

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentEventMessage>
            paymentEventListenerContainerFactory(KafkaProperties kafkaProperties,
                                                 CommonErrorHandler errorHandler) {
        JsonDeserializer<PaymentEventMessage> delegate =
                new JsonDeserializer<>(PaymentEventMessage.class);
        delegate.setUseTypeHeaders(false);

        Map<String, Object> properties = new HashMap<>(kafkaProperties.buildConsumerProperties(null));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.remove(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);

        /*
         * As `spring.json.*` do application.yml precisam sair daqui: elas configuram o
         * consumidor PADRÃO, e o Spring Kafka recusa configurar o mesmo deserializer
         * por properties e por setter ao mesmo tempo — o contexto nem sobe.
         */
        properties.keySet().removeIf(key ->
                key.startsWith("spring.json.") || key.startsWith("spring.deserializer."));

        ConcurrentKafkaListenerContainerFactory<String, PaymentEventMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(properties,
                new StringDeserializer(), new ErrorHandlingDeserializer<>(delegate)));
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
