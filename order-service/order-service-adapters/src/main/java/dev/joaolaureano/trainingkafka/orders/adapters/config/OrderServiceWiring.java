package dev.joaolaureano.trainingkafka.orders.adapters.config;

import dev.joaolaureano.trainingkafka.orders.adapters.messaging.KafkaActivityLogPublisher;
import dev.joaolaureano.trainingkafka.orders.adapters.messaging.KafkaOrderEventPublisher;
import dev.joaolaureano.trainingkafka.orders.application.PlaceOrderService;
import dev.joaolaureano.trainingkafka.orders.application.PlaceOrderUseCase;
import dev.joaolaureano.trainingkafka.orders.application.port.ActivityLogPublisher;
import dev.joaolaureano.trainingkafka.orders.domain.port.OrderEventPublisher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Clock;

/**
 * O ponto de montagem do sistema.
 *
 * É AQUI que se decide quem implementa cada Port — e é o único lugar do serviço
 * onde essa decisão existe. As classes de domínio e de aplicação não têm uma
 * única anotação: elas são instanciadas com {@code new}, como objetos comuns,
 * porque é exatamente o que são.
 *
 * Trocar Kafka por outra coisa significa escrever outro adapter e mudar uma
 * linha deste arquivo. Nada além disso.
 */
@Configuration
public class OrderServiceWiring {

    @Bean
    public Clock clock() {
        // Injetado em vez de Instant.now() espalhado pelo código: é o que torna
        // o caso de uso testável com tempo congelado.
        return Clock.systemUTC();
    }

    @Bean
    public OrderEventPublisher orderEventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        return new KafkaOrderEventPublisher(kafkaTemplate);
    }

    @Bean
    public ActivityLogPublisher activityLogPublisher(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${spring.application.name}") String applicationName) {
        return new KafkaActivityLogPublisher(kafkaTemplate, applicationName);
    }

    @Bean
    public PlaceOrderUseCase placeOrderUseCase(OrderEventPublisher orderEventPublisher,
                                               ActivityLogPublisher activityLogPublisher,
                                               Clock clock) {
        return new PlaceOrderService(orderEventPublisher, activityLogPublisher, clock);
    }
}
