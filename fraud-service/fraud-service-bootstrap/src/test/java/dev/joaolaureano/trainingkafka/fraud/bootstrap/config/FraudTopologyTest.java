package dev.joaolaureano.trainingkafka.fraud.bootstrap.config;

import dev.joaolaureano.trainingkafka.fraud.adapters.messaging.AuditEventMessage;
import dev.joaolaureano.trainingkafka.fraud.adapters.messaging.FraudDetectedMessage;
import dev.joaolaureano.trainingkafka.fraud.adapters.messaging.OrderPlacedMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class FraudTopologyTest {

    private static final Instant T0 = Instant.parse("2026-08-25T12:00:00Z");

    private TopologyTestDriver driver;
    private TestInputTopic<String, OrderPlacedMessage> input;
    private TestOutputTopic<String, AuditEventMessage> output;
    private TestOutputTopic<String, FraudDetectedMessage> fraudEvents;

    @BeforeEach
    void setUp() {
        FraudTopology topologyConfiguration = new FraudTopology();
        StreamsBuilder builder = new StreamsBuilder();
        JsonSerde<OrderPlacedMessage> orderSerde = new JsonSerde<>(OrderPlacedMessage.class);
        JsonSerde<AuditEventMessage> auditSerde = new JsonSerde<>(AuditEventMessage.class);
        JsonSerde<FraudDetectedMessage> fraudSerde = new JsonSerde<>(FraudDetectedMessage.class);
        topologyConfiguration.fraudStream(builder, new FraudProperties(5, Duration.ofSeconds(10), Duration.ofSeconds(2)));

        Properties properties = new Properties();
        properties.put("application.id", "fraud-topology-test");
        properties.put("bootstrap.servers", "dummy:1234");
        properties.put("default.key.serde", Serdes.String().getClass());
        Topology topology = builder.build();
        driver = new TopologyTestDriver(topology, properties);
        input = driver.createInputTopic("orders", new StringSerde().serializer(), orderSerde.serializer());
        output = driver.createOutputTopic("audit-events", new StringSerde().deserializer(), auditSerde.deserializer());
        fraudEvents = driver.createOutputTopic("fraud-events", new StringSerde().deserializer(),
                fraudSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void emitsOneAlertOnThresholdAndKeepsCustomerKey() {
        for (int index = 0; index < 5; index++) {
            input.pipeInput("cust-1", order("order-" + index, "cust-1", index));
        }

        assertThat(output.getQueueSize()).isEqualTo(1);
        var result = output.readKeyValue();
        assertThat(result.key).isEqualTo("cust-1");
        assertThat(result.value.app()).isEqualTo("fraud-service");
        assertThat(result.value.context()).containsEntry("ordersInWindow", "5");
    }

    @Test
    void publishesTheWholeWindowForCompensation() {
        for (int index = 0; index < 5; index++) {
            input.pipeInput("cust-1", order("order-" + index, "cust-1", index));
        }

        assertThat(fraudEvents.getQueueSize()).isEqualTo(1);
        var detected = fraudEvents.readKeyValue();
        assertThat(detected.key).isEqualTo("cust-1");
        assertThat(detected.value.customerId()).isEqualTo("cust-1");
        assertThat(detected.value.ordersInWindow()).isEqualTo(5);
        // A janela INTEIRA, não uma amostra: compensar metade da rajada seria pior
        // do que não compensar.
        assertThat(detected.value.orders())
                .extracting(FraudDetectedMessage.FraudulentOrder::orderId)
                .containsExactly("order-0", "order-1", "order-2", "order-3", "order-4");
        assertThat(detected.value.orders())
                .allSatisfy(order -> assertThat(order.amount()).isEqualByComparingTo(BigDecimal.TEN));
    }

    @Test
    void theAlertKeepsCarryingOnlyASample() {
        for (int index = 0; index < 7; index++) {
            input.pipeInput("cust-1", order("order-" + index, "cust-1", index));
        }

        // O tópico de auditoria não mudou de formato: continua com no máximo 5 ids.
        assertThat(output.readKeyValue().value.context().get("sampleOrderIds").split(","))
                .hasSize(5);
    }

    @Test
    void duplicateOrderDoesNotCountTwice() {
        for (int index = 0; index < 4; index++) {
            input.pipeInput("cust-1", order("order-" + index, "cust-1", index));
        }
        input.pipeInput("cust-1", order("order-3", "cust-1", 4));

        assertThat(output.isEmpty()).isTrue();
        assertThat(fraudEvents.isEmpty()).isTrue();
    }

    @Test
    void ignoresEventsOlderThanTheGracePeriod() {
        input.pipeInput("cust-1", order("order-1", "cust-1", 0));
        input.pipeInput("cust-1", order("late-order", "cust-1", -3));

        KeyValueStore<String, dev.joaolaureano.trainingkafka.fraud.adapters.streams.CustomerFraudState> state =
                driver.getKeyValueStore(FraudTopology.STATE_STORE);
        assertThat(state.get("cust-1").knownOrderIds()).containsExactly("order-1");
    }

    private OrderPlacedMessage order(String orderId, String customerId, long seconds) {
        return new OrderPlacedMessage(orderId, customerId, "Keyboard", 1,
                BigDecimal.TEN, T0.plusSeconds(seconds).toString());
    }
}
