package dev.joaolaureano.trainingkafka.payment.adapters.config;

import dev.joaolaureano.trainingkafka.payment.adapters.messaging.FraudDetectedMessage;
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
 * Container próprio para {@code fraud-events}.
 *
 * O consumidor padrão do serviço está fixado em {@code InventoryEventMessage} — sem
 * cabeçalho de tipo na mensagem, que é o acoplamento que se quis evitar, o tipo
 * precisa ser decidido na configuração. Dois tópicos com contratos diferentes,
 * portanto, exigem dois containers; um único {@code default.type} desserializaria
 * o FraudDetected como se fosse pedido.
 */
@Configuration
public class FraudEventConsumerConfig {

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, FraudDetectedMessage>
            fraudEventListenerContainerFactory(KafkaProperties kafkaProperties,
                                               CommonErrorHandler errorHandler) {
        JsonDeserializer<FraudDetectedMessage> delegate =
                new JsonDeserializer<>(FraudDetectedMessage.class);
        delegate.setUseTypeHeaders(false);

        Map<String, Object> properties = new HashMap<>(kafkaProperties.buildConsumerProperties(null));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.remove(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);

        /*
         * As `spring.json.*` do application.yml precisam sair daqui.
         *
         * Elas configuram o consumidor PADRÃO, fixado em InventoryEventMessage. Este
         * deserializer é montado na mão, por setter — e o Spring Kafka recusa as duas
         * formas ao mesmo tempo, com "must be configured with property setters, or via
         * configuration properties; not both". O contexto nem sobe.
         */
        properties.keySet().removeIf(key ->
                key.startsWith("spring.json.") || key.startsWith("spring.deserializer."));

        ConcurrentKafkaListenerContainerFactory<String, FraudDetectedMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(properties,
                new StringDeserializer(), new ErrorHandlingDeserializer<>(delegate)));
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
