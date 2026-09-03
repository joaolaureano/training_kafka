package dev.joaolaureano.trainingkafka.fraud.bootstrap.config;

import dev.joaolaureano.trainingkafka.fraud.adapters.messaging.AuditEventMessage;
import dev.joaolaureano.trainingkafka.fraud.adapters.messaging.FraudDetectedMessage;
import dev.joaolaureano.trainingkafka.fraud.adapters.messaging.OrderPlacedMessage;
import dev.joaolaureano.trainingkafka.fraud.adapters.messaging.Topics;
import dev.joaolaureano.trainingkafka.fraud.adapters.streams.CustomerFraudState;
import dev.joaolaureano.trainingkafka.fraud.adapters.streams.OccurredAtTimestampExtractor;
import dev.joaolaureano.trainingkafka.fraud.application.FraudDetectionService;
import dev.joaolaureano.trainingkafka.fraud.domain.event.FraudDetected;
import dev.joaolaureano.trainingkafka.fraud.domain.model.CustomerFraudPattern;
import dev.joaolaureano.trainingkafka.fraud.domain.model.FraudOrder;
import dev.joaolaureano.trainingkafka.fraud.domain.model.FraudPolicy;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@Configuration
public class FraudTopology {

    public static final String STATE_STORE = "customer-fraud-state";

    @Bean
    public KStream<String, FraudDetectedMessage> fraudStream(StreamsBuilder builder,
                                                             FraudProperties properties) {
        JsonSerde<OrderPlacedMessage> orderSerde = new JsonSerde<>(OrderPlacedMessage.class);
        JsonSerde<CustomerFraudState> stateSerde = new JsonSerde<>(CustomerFraudState.class);
        JsonSerde<AuditEventMessage> auditSerde = new JsonSerde<>(AuditEventMessage.class);
        JsonSerde<FraudDetectedMessage> fraudSerde = new JsonSerde<>(FraudDetectedMessage.class);
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STATE_STORE), Serdes.String(), stateSerde));

        KStream<String, OrderPlacedMessage> orders = builder.stream(
                Topics.ORDERS,
                Consumed.with(Serdes.String(), orderSerde)
                        .withTimestampExtractor(new OccurredAtTimestampExtractor())
                        .withName("orders-source"));

        KStream<String, FraudDetectedMessage> detections = orders
                .filter((key, order) -> key != null && order != null && order.customerId() != null)
                .selectKey((key, order) -> order.customerId())
                .transform(new FraudTransformerSupplier(
                                new FraudPolicy(properties.maxOrders(), properties.window(), properties.gracePeriod()),
                                new FraudDetectionService()),
                        Named.as("stateful-fraud-detection"), STATE_STORE)
                .filter((key, detected) -> detected != null);

        // Duas saídas para o mesmo fato: uma para humano ler, outra para a Saga agir.
        // Ambas chaveadas por customerId, que é a unidade de consistência do detector.
        detections.mapValues(FraudTopology::toAuditMessage)
                .to(Topics.AUDIT_EVENTS, Produced.with(Serdes.String(), auditSerde));
        detections.to(Topics.FRAUD_EVENTS, Produced.with(Serdes.String(), fraudSerde));
        return detections;
    }

    private static AuditEventMessage toAuditMessage(FraudDetectedMessage detected) {
        Map<String, String> context = new LinkedHashMap<>();
        context.put("ordersInWindow", Integer.toString(detected.ordersInWindow()));
        context.put("windowSeconds", Long.toString(detected.windowSeconds()));
        context.put("sampleOrderIds", detected.orders().stream()
                .skip(Math.max(0, detected.orders().size() - 5))
                .map(FraudDetectedMessage.FraudulentOrder::orderId)
                .collect(Collectors.joining(",")));
        return new AuditEventMessage("WARN", detected.occurredAt(), "fraud-service",
                "suspicious.order.pattern.detected", context);
    }

    private record FraudTransformerSupplier(FraudPolicy policy, FraudDetectionService detector)
            implements TransformerSupplier<String, OrderPlacedMessage, KeyValue<String, FraudDetectedMessage>> {
        @Override
        public Transformer<String, OrderPlacedMessage, KeyValue<String, FraudDetectedMessage>> get() {
            return new FraudTransformer(policy, detector);
        }
    }

    private static final class FraudTransformer
            implements Transformer<String, OrderPlacedMessage, KeyValue<String, FraudDetectedMessage>> {

        private final FraudPolicy policy;
        private final FraudDetectionService detector;
        private KeyValueStore<String, CustomerFraudState> store;

        private FraudTransformer(FraudPolicy policy, FraudDetectionService detector) {
            this.policy = policy;
            this.detector = detector;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            store = (KeyValueStore<String, CustomerFraudState>) context.getStateStore(STATE_STORE);
        }

        @Override
        public KeyValue<String, FraudDetectedMessage> transform(String customerId, OrderPlacedMessage message) {
            Instant occurredAt = Instant.parse(message.occurredAt());
            CustomerFraudState previous = Optional.ofNullable(store.get(customerId))
                    .orElseGet(CustomerFraudState::empty);
            if (previous.latestEventTime() != null
                    && occurredAt.isBefore(previous.latestEventTime().minus(policy.gracePeriod()))) {
                return null;
            }

            CustomerFraudPattern pattern = CustomerFraudPattern.reconstitute(
                    customerId, policy, previous.recentOrders(), previous.knownOrderIds());
            Optional<FraudDetected> detected = detector.detect(pattern,
                    new FraudOrder(message.orderId(), occurredAt, message.amount()));
            store.put(customerId, new CustomerFraudState(pattern.recentOrders(), pattern.knownOrderIds(),
                    latest(previous.latestEventTime(), occurredAt)));
            return detected.map(alert -> KeyValue.pair(customerId, toMessage(alert))).orElse(null);
        }

        private FraudDetectedMessage toMessage(FraudDetected alert) {
            return new FraudDetectedMessage(
                    // Derivado do cliente e do instante: reprocessar a partition não
                    // muda o eventId, então quem deduplica consegue.
                    UUID.nameUUIDFromBytes((alert.customerId() + ':' + alert.occurredAt())
                            .getBytes(StandardCharsets.UTF_8)).toString(),
                    UUID.randomUUID().toString(),
                    alert.customerId(),
                    alert.ordersInWindow(),
                    alert.window().toSeconds(),
                    alert.orders().stream()
                            .map(order -> new FraudDetectedMessage.FraudulentOrder(
                                    order.orderId(), order.amount()))
                            .toList(),
                    alert.occurredAt().toString());
        }

        private Instant latest(Instant previous, Instant current) {
            return previous == null || current.isAfter(previous) ? current : previous;
        }

        @Override
        public void close() {
        }
    }
}
