package dev.joaolaureano.trainingkafka.inventory.bootstrap.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.joaolaureano.trainingkafka.inventory.adapters.messaging.InventoryEventOutboxTranslator;
import dev.joaolaureano.trainingkafka.inventory.adapters.messaging.KafkaActivityLogPublisher;
import dev.joaolaureano.trainingkafka.inventory.adapters.messaging.KafkaOutboxDispatcher;
import dev.joaolaureano.trainingkafka.inventory.adapters.messaging.OrderPlacedPort;
import dev.joaolaureano.trainingkafka.inventory.adapters.messaging.OutboxRelay;
import dev.joaolaureano.trainingkafka.inventory.adapters.messaging.PaymentEventPort;
import dev.joaolaureano.trainingkafka.inventory.adapters.persistence.OutboxDispatcher;
import dev.joaolaureano.trainingkafka.inventory.adapters.persistence.OutboxStore;
import dev.joaolaureano.trainingkafka.inventory.adapters.persistence.OutboxTranslator;
import dev.joaolaureano.trainingkafka.inventory.adapters.persistence.SqliteInventoryRepository;
import dev.joaolaureano.trainingkafka.inventory.application.ManageCatalog;
import dev.joaolaureano.trainingkafka.inventory.application.ReleaseStockForOrder;
import dev.joaolaureano.trainingkafka.inventory.application.ReserveStockForOrder;
import dev.joaolaureano.trainingkafka.inventory.application.port.ActivityLogPublisher;
import dev.joaolaureano.trainingkafka.inventory.bootstrap.facade.ActivityLogFacade;
import dev.joaolaureano.trainingkafka.inventory.bootstrap.facade.CatalogFacade;
import dev.joaolaureano.trainingkafka.inventory.bootstrap.facade.OrderPlacedFacade;
import dev.joaolaureano.trainingkafka.inventory.bootstrap.facade.PaymentEventFacade;
import dev.joaolaureano.trainingkafka.inventory.domain.port.InventoryRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Clock;

/**
 * A composition root do App F.
 *
 * É o único lugar do serviço que conhece os dois lados: aqui o caso de uso, que
 * só sabe de Ports, encontra o adapter, que só sabe de infraestrutura.
 */
@Configuration
public class InventoryWiring {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean(destroyMethod = "close")
    Connection inventoryConnection(
            @Value("${inventory.persistence.sqlite.path:data/inventory.db}") String path) {
        try {
            Path parent = Path.of(path).toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Connection connection = DriverManager.getConnection("jdbc:sqlite:" + path);
            connection.setAutoCommit(false);
            return connection;
        } catch (SQLException | IOException failure) {
            throw new IllegalStateException("Could not open inventory database", failure);
        }
    }

    @Bean
    OutboxTranslator outboxTranslator(ObjectMapper objectMapper) {
        return new InventoryEventOutboxTranslator(objectMapper);
    }

    /**
     * Um único objeto implementa {@code InventoryRepository} e {@code OutboxStore} —
     * é o que garante que o estoque, a reserva e o evento entrem na mesma transação.
     *
     * Declarado pelo tipo concreto, e uma vez só: um @Bean por interface delegando
     * para este daria dois candidatos para {@code OutboxStore} e o contexto não
     * subiria.
     */
    @Bean
    SqliteInventoryRepository inventoryRepository(Connection connection, OutboxTranslator translator) {
        return new SqliteInventoryRepository(connection, translator);
    }

    @Bean
    OutboxDispatcher outboxDispatcher(KafkaTemplate<String, Object> template, ObjectMapper objectMapper) {
        return new KafkaOutboxDispatcher(template, objectMapper);
    }

    @Bean
    OutboxRelay outboxRelay(OutboxStore outbox, OutboxDispatcher dispatcher,
                            @Value("${inventory.outbox.batch-size:500}") int batchSize,
                            @Value("${inventory.outbox.send-timeout-seconds:10}") long confirmTimeout) {
        return new OutboxRelay(outbox, dispatcher, batchSize, confirmTimeout);
    }

    @Bean
    KafkaActivityLogPublisher kafkaActivityLogPublisher(
            KafkaTemplate<String, Object> template,
            @Value("${spring.application.name}") String applicationName) {
        return new KafkaActivityLogPublisher(template, applicationName);
    }

    @Bean
    ActivityLogPublisher activityLogPublisher(KafkaActivityLogPublisher publisher) {
        return new ActivityLogFacade(publisher);
    }

    @Bean
    ReserveStockForOrder reserveStockForOrder(
            InventoryRepository inventory, ActivityLogPublisher activityLog, Clock clock,
            @Value("${inventory.stock.optimistic-attempts:5}") int attempts) {
        return new ReserveStockForOrder(inventory, activityLog, clock, attempts);
    }

    @Bean
    ReleaseStockForOrder releaseStockForOrder(
            InventoryRepository inventory, ActivityLogPublisher activityLog, Clock clock,
            @Value("${inventory.stock.optimistic-attempts:5}") int attempts) {
        return new ReleaseStockForOrder(inventory, activityLog, clock, attempts);
    }

    @Bean
    ManageCatalog manageCatalog(InventoryRepository inventory, ActivityLogPublisher activityLog,
                                Clock clock) {
        return new ManageCatalog(inventory, activityLog, clock);
    }

    @Bean
    OrderPlacedPort orderPlacedPort(ReserveStockForOrder reserveStock) {
        return new OrderPlacedFacade(reserveStock);
    }

    @Bean
    PaymentEventPort paymentEventPort(ReleaseStockForOrder releaseStock) {
        return new PaymentEventFacade(releaseStock);
    }

    /**
     * Uma instância só atende os dois Ports do controller, e é declarada uma vez só.
     *
     * Um @Bean por interface delegando para esta daria dois candidatos para
     * {@code UpsertProductPort} — a própria facade e o delegador — e o contexto não
     * subiria. Mesmo motivo pelo qual o repositório também é declarado pelo tipo
     * concreto.
     */
    @Bean
    CatalogFacade catalogFacade(ManageCatalog catalog) {
        return new CatalogFacade(catalog);
    }
}
