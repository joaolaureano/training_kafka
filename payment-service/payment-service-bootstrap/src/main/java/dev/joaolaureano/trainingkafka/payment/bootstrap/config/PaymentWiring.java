package dev.joaolaureano.trainingkafka.payment.bootstrap.config;

import dev.joaolaureano.trainingkafka.payment.adapters.gateway.DeterministicPaymentGateway;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.joaolaureano.trainingkafka.payment.adapters.messaging.KafkaOutboxDispatcher;
import dev.joaolaureano.trainingkafka.payment.adapters.messaging.OutboxRelay;
import dev.joaolaureano.trainingkafka.payment.adapters.messaging.PaymentEventOutboxTranslator;
import dev.joaolaureano.trainingkafka.payment.adapters.persistence.OutboxDispatcher;
import dev.joaolaureano.trainingkafka.payment.adapters.persistence.OutboxStore;
import dev.joaolaureano.trainingkafka.payment.adapters.persistence.OutboxTranslator;
import dev.joaolaureano.trainingkafka.payment.adapters.messaging.FraudEventPort;
import dev.joaolaureano.trainingkafka.payment.adapters.messaging.OrderPlacedPort;
import dev.joaolaureano.trainingkafka.payment.adapters.persistence.SqlitePaymentRepository;
import dev.joaolaureano.trainingkafka.payment.application.CompensateFraudulentOrders;
import dev.joaolaureano.trainingkafka.payment.application.ProcessOrderPayment;
import dev.joaolaureano.trainingkafka.payment.bootstrap.facade.FraudEventFacade;
import dev.joaolaureano.trainingkafka.payment.bootstrap.facade.OrderPlacedFacade;
import dev.joaolaureano.trainingkafka.payment.domain.port.PaymentGateway;
import dev.joaolaureano.trainingkafka.payment.domain.port.PaymentRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Clock;

@Configuration
public class PaymentWiring {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean(destroyMethod = "close")
    Connection paymentConnection(@Value("${payment.persistence.sqlite.path:data/payment.db}") String path) {
        try {
            Path parent = Path.of(path).toAbsolutePath().getParent();
            if (parent != null) Files.createDirectories(parent);
            Connection connection = DriverManager.getConnection("jdbc:sqlite:" + path);
            connection.setAutoCommit(false);
            return connection;
        } catch (SQLException | java.io.IOException failure) {
            throw new IllegalStateException("Could not open payment database", failure);
        }
    }

    @Bean
    OutboxTranslator outboxTranslator(ObjectMapper objectMapper) {
        return new PaymentEventOutboxTranslator(objectMapper);
    }

    /**
     * Um único objeto implementa {@code PaymentRepository} e {@code OutboxStore} —
     * é o que garante que o desfecho e o evento entrem na mesma transação.
     */
    @Bean
    SqlitePaymentRepository sqlitePaymentRepository(Connection connection, OutboxTranslator translator) {
        return new SqlitePaymentRepository(connection, translator);
    }

    @Bean
    PaymentRepository paymentRepository(SqlitePaymentRepository repository) {
        return repository;
    }

    @Bean
    OutboxStore outboxStore(SqlitePaymentRepository repository) {
        return repository;
    }

    @Bean
    OutboxDispatcher outboxDispatcher(KafkaTemplate<String, Object> template, ObjectMapper objectMapper,
                                      @Value("${payment.outbox.send-timeout-seconds:10}") long sendTimeout) {
        return new KafkaOutboxDispatcher(template, objectMapper, sendTimeout);
    }

    @Bean
    OutboxRelay outboxRelay(OutboxStore outbox, OutboxDispatcher dispatcher,
                            @Value("${payment.outbox.batch-size:100}") int batchSize) {
        return new OutboxRelay(outbox, dispatcher, batchSize);
    }

    @Bean
    PaymentGateway paymentGateway(@Value("${payment.gateway.approval-limit:1000.00}") BigDecimal limit) {
        return new DeterministicPaymentGateway(limit);
    }

    @Bean
    ProcessOrderPayment processOrderPayment(PaymentRepository repository, PaymentGateway gateway,
                                            Clock clock) {
        return new ProcessOrderPayment(repository, gateway, clock);
    }

    @Bean
    OrderPlacedPort orderPlacedPort(ProcessOrderPayment processPayment) {
        return new OrderPlacedFacade(processPayment);
    }

    @Bean
    CompensateFraudulentOrders compensateFraudulentOrders(PaymentRepository repository, Clock clock) {
        return new CompensateFraudulentOrders(repository, clock);
    }

    @Bean
    FraudEventPort fraudEventPort(CompensateFraudulentOrders compensate) {
        return new FraudEventFacade(compensate);
    }
}
