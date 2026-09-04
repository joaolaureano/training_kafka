package dev.joaolaureano.trainingkafka.orders.bootstrap.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.joaolaureano.trainingkafka.orders.adapters.messaging.KafkaActivityLogPublisher;
import dev.joaolaureano.trainingkafka.orders.adapters.messaging.OrderEventOutboxTranslator;
import dev.joaolaureano.trainingkafka.orders.adapters.messaging.KafkaOutboxDispatcher;
import dev.joaolaureano.trainingkafka.orders.adapters.messaging.OutboxRelay;
import dev.joaolaureano.trainingkafka.orders.adapters.persistence.OutboxDispatcher;
import dev.joaolaureano.trainingkafka.orders.adapters.persistence.OutboxStore;
import dev.joaolaureano.trainingkafka.orders.adapters.persistence.OutboxTranslator;
import dev.joaolaureano.trainingkafka.orders.adapters.persistence.SqliteOrderRepository;
import dev.joaolaureano.trainingkafka.orders.adapters.web.FindOrderPort;
import dev.joaolaureano.trainingkafka.orders.adapters.web.PlaceOrderPort;
import dev.joaolaureano.trainingkafka.orders.application.FindOrderService;
import dev.joaolaureano.trainingkafka.orders.application.FindOrderUseCase;
import dev.joaolaureano.trainingkafka.orders.application.PlaceOrderService;
import dev.joaolaureano.trainingkafka.orders.application.PlaceOrderUseCase;
import dev.joaolaureano.trainingkafka.orders.application.port.ActivityLogPublisher;
import dev.joaolaureano.trainingkafka.orders.adapters.messaging.InventoryEventPort;
import dev.joaolaureano.trainingkafka.orders.bootstrap.facade.ActivityLogFacade;
import dev.joaolaureano.trainingkafka.orders.bootstrap.facade.InventoryEventFacade;
import dev.joaolaureano.trainingkafka.orders.bootstrap.facade.FindOrderFacade;
import dev.joaolaureano.trainingkafka.orders.bootstrap.facade.PlaceOrderFacade;
import dev.joaolaureano.trainingkafka.orders.domain.port.OrderRepository;
import dev.joaolaureano.trainingkafka.orders.application.ApplyPaymentResult;
import dev.joaolaureano.trainingkafka.orders.application.ApplyStockResult;
import dev.joaolaureano.trainingkafka.orders.adapters.messaging.PaymentEventPort;
import dev.joaolaureano.trainingkafka.orders.bootstrap.facade.PaymentEventFacade;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Clock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;

/**
 * O ponto de montagem do sistema.
 *
 * É AQUI que se decide quem implementa cada Port — e é o único lugar do serviço
 * onde essa decisão existe. As classes de domínio e de aplicação não têm uma
 * única anotação: elas são instanciadas com {@code new}, como objetos comuns,
 * porque é exatamente o que são.
 *
 * Repare que este arquivo é o único que importa {@code ...adapters} e
 * {@code ...application} ao mesmo tempo. Nenhum dos dois módulos depende do
 * outro: o adapter declara a interface de que precisa, a aplicação implementa a
 * sua, e as facades deste pacote costuram os dois lados.
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

    @Bean(destroyMethod = "close")
    public Connection orderConnection(
            @Value("${order.persistence.sqlite.path:data/orders.db}") String path) {
        try {
            Path parent = Path.of(path).toAbsolutePath().getParent();
            if (parent != null) Files.createDirectories(parent);
            Connection connection = DriverManager.getConnection("jdbc:sqlite:" + path);
            connection.setAutoCommit(false);
            return connection;
        } catch (java.sql.SQLException | java.io.IOException failure) {
            throw new IllegalStateException("Could not open order database", failure);
        }
    }

    @Bean
    public OutboxTranslator outboxTranslator(ObjectMapper objectMapper) {
        return new OrderEventOutboxTranslator(objectMapper);
    }

    /**
     * Um único objeto implementa {@code OrderRepository} e {@code OutboxStore} —
     * é o que garante que o pedido e o seu evento entrem na mesma transação.
     *
     * Declarado pelo tipo concreto, e uma vez só. Expor além dele um @Bean por
     * interface, delegando para este, daria a Spring DOIS candidatos para
     * {@code OutboxStore} — o delegado e o próprio objeto concreto, que também
     * implementa a interface — e o contexto não sobe. Quem injeta pede o Port de
     * que precisa; a resolução é do container.
     */
    @Bean
    public SqliteOrderRepository orderRepository(Connection connection, OutboxTranslator translator) {
        return new SqliteOrderRepository(connection, translator);
    }

    @Bean
    public OutboxDispatcher outboxDispatcher(KafkaTemplate<String, Object> kafkaTemplate,
                                             ObjectMapper objectMapper) {
        return new KafkaOutboxDispatcher(kafkaTemplate, objectMapper);
    }

    @Bean
    public OutboxRelay outboxRelay(OutboxStore outbox, OutboxDispatcher dispatcher,
                                   @Value("${order.outbox.batch-size:500}") int batchSize,
                                   @Value("${order.outbox.send-timeout-seconds:10}") long confirmTimeout) {
        return new OutboxRelay(outbox, dispatcher, batchSize, confirmTimeout);
    }

    @Bean
    public KafkaActivityLogPublisher kafkaActivityLogPublisher(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${spring.application.name}") String applicationName) {
        return new KafkaActivityLogPublisher(kafkaTemplate, applicationName);
    }

    @Bean
    public ActivityLogPublisher activityLogPublisher(KafkaActivityLogPublisher publisher) {
        return new ActivityLogFacade(publisher);
    }

    @Bean
    public PlaceOrderUseCase placeOrderUseCase(OrderRepository orders,
                                               ActivityLogPublisher activityLogPublisher,
                                               Clock clock) {
        return new PlaceOrderService(orders, activityLogPublisher, clock);
    }

    @Bean
    public PlaceOrderPort placeOrderPort(PlaceOrderUseCase placeOrderUseCase) {
        return new PlaceOrderFacade(placeOrderUseCase);
    }

    @Bean
    public FindOrderUseCase findOrderUseCase(OrderRepository orders) {
        return new FindOrderService(orders);
    }

    @Bean
    public FindOrderPort findOrderPort(FindOrderUseCase findOrderUseCase) {
        return new FindOrderFacade(findOrderUseCase);
    }

    @Bean
    public ApplyPaymentResult applyPaymentResult(OrderRepository orders) {
        return new ApplyPaymentResult(orders);
    }

    @Bean
    public PaymentEventPort paymentEventPort(ApplyPaymentResult applyResult) {
        return new PaymentEventFacade(applyResult);
    }

    @Bean
    public ApplyStockResult applyStockResult(OrderRepository orders) {
        return new ApplyStockResult(orders);
    }

    @Bean
    public InventoryEventPort inventoryEventPort(ApplyStockResult applyResult) {
        return new InventoryEventFacade(applyResult);
    }
}
