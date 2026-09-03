# Graph Report - training_kafka  (2026-09-03)

## Corpus Check
- 225 files · ~40,361 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1413 nodes · 3677 edges · 97 communities (51 shown, 46 thin omitted)
- Extraction: 89% EXTRACTED · 11% INFERRED · 0% AMBIGUOUS · INFERRED: 414 edges (avg confidence: 0.81)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `a5aee46d`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- AuditController.java
- ProductSalesRecord
- SqlitePaymentRepository
- java.sql.Connection
- DeadLetterProperties
- .handle
- metrics-consumer (App B)
- TimeRange
- org.springframework.context.annotation.Configuration
- Violation
- org.junit.jupiter.api.Test
- Money
- FraudTopologyTest
- DeadLetterProperties
- orders-load.js
- package.json
- org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
- org.springframework.boot.autoconfigure.SpringBootApplication
- AuditServiceWiring.java
- AuditEvent
- SqliteRepositoryException
- dev.joaolaureano.trainingkafka:training-kafka
- CustomerFraudPattern
- Payment
- OutboxRecord
- AuditRepository
- metrics-consumer
- metrics-consumer-adapters
- metrics-consumer-application
- metrics-consumer-domain
- order-service
- order-service-adapters
- order-service-application
- order-service-domain
- FraudTopology.java
- AuditFilter
- OrderPlaced
- audit-service
- audit-service-adapters
- FraudDetectedMessage
- analytics/adapters/messaging/AuditEventMessage.java
- audit-service-application
- org.springframework.context.annotation.Bean
- Topics
- audit-service-bootstrap
- audit-service-domain
- OrderServiceWiring.java
- PaymentWiring.java
- AuditLevel
- OrderId
- KafkaActivityLogPublisher
- ProductSalesRepository
- FraudStreamsConfiguration.java
- Anticorruption Layer de tradução na fronteira
- fraud-service
- metrics-consumer-bootstrap
- order-service-bootstrap
- fraud-service-adapters
- fraud-service-application
- fraud-service-bootstrap
- Topics
- fraud-service-domain
- MetricsQueryService
- Retry
- OccurredAtTimestampExtractor.java
- ProcessOrderPaymentTest
- DuckDbAuditRepository
- DeadLetterProperties
- DeadLetterProperties
- JsonlFileAuditRepository
- com.fasterxml.jackson.databind.ObjectMapper
- .fraudStream
- ApplyPaymentResultTest
- Retry
- PaymentEventOutboxTranslator
- SqliteOrderRepository.java
- FraudOrder
- OutboxRecord
- Money
- SqliteOrderRepositoryTest.java
- Retry
- UnknownOrderException
- InvalidAuditException
- payment-service
- payment-service-adapters
- payment-service-application
- payment-service-bootstrap
- payment-service-domain
- CustomerId
- .charge
- FraudEventConsumerConfig.java
- Topics
- Topics
- fraud/adapters/messaging/AuditEventMessage.java
- Money
- ProductId
- Quantity

## God Nodes (most connected - your core abstractions)
1. `Payment` - 51 edges
2. `ProductSalesRecord` - 49 edges
3. `Order` - 48 edges
4. `ProductId` - 45 edges
5. `AuditEvent` - 43 edges
6. `ProductSalesRepository` - 34 edges
7. `AuditRepository` - 33 edges
8. `TimeRange` - 32 edges
9. `OrderId` - 31 edges
10. `OrderLedgerRepository` - 31 edges

## Surprising Connections (you probably didn't know these)
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` --conceptually_related_to--> `order-service (App A)`  [INFERRED]
  docker-compose.yml → README.md
- `training_kafka (laboratório Kafka + DDD)` --references--> `Kafka UI (kafbat/kafka-ui:8090)`  [EXTRACTED]
  README.md → docker-compose.yml
- `training_kafka (laboratório Kafka + DDD)` --references--> `Broker Kafka (apache/kafka:4.1.2)`  [EXTRACTED]
  README.md → docker-compose.yml
- `FraudTransformer` --references--> `FraudDetectedMessage`  [EXTRACTED]
  fraud-service/fraud-service-bootstrap/src/main/java/dev/joaolaureano/trainingkafka/fraud/bootstrap/config/FraudTopology.java → fraud-service/fraud-service-adapters/src/main/java/dev/joaolaureano/trainingkafka/fraud/adapters/messaging/FraudDetectedMessage.java
- `FraudTransformerSupplier` --references--> `FraudDetectedMessage`  [EXTRACTED]
  fraud-service/fraud-service-bootstrap/src/main/java/dev/joaolaureano/trainingkafka/fraud/bootstrap/config/FraudTopology.java → fraud-service/fraud-service-adapters/src/main/java/dev/joaolaureano/trainingkafka/fraud/adapters/messaging/FraudDetectedMessage.java

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Pipeline de eventos A → orders → B → application-logs → C** — readme_order_service, readme_orders_topic, readme_metrics_consumer, readme_application_logs_topic, readme_log_aggregator, docker_compose_kafka_broker [EXTRACTED 1.00]
- **Modelo de domínio do App B (3 raízes + VO derivado + handler)** — readme_productsalesrecord, readme_customerorderpattern, readme_orderrecord, readme_revenuewindow, readme_orderplacedhandler [EXTRACTED 1.00]

## Communities (97 total, 46 thin omitted)

### Community 0 - "AuditController.java"
Cohesion: 0.10
Nodes (16): AuditController, MetricsController, ProductSalesView, RevenueView, OrderController, PlaceOrderPort, PlaceOrderRequest, PlaceOrderResponse (+8 more)

### Community 1 - "ProductSalesRecord"
Cohesion: 0.09
Nodes (17): Override, ProductId, InMemoryProductSalesRepository, Override, Money, Override, ProductId, FakeProductSales (+9 more)

### Community 2 - "SqlitePaymentRepository"
Cohesion: 0.16
Nodes (5): OutboxRecord, Override, SqlitePaymentRepository, SqlitePaymentRepositoryTest, PaymentTest

### Community 3 - "java.sql.Connection"
Cohesion: 0.14
Nodes (8): java.sql.Connection, java.sql.PreparedStatement, java.sql.ResultSet, DuckDbProductSalesRepository, MoneyCents, SqliteProductSalesRepository, TestRepositories, org.junit.jupiter.params.provider.Arguments

### Community 5 - ".handle"
Cohesion: 0.31
Nodes (4): FraudulentOrder, CompensateFraudulentOrdersTest, FraudulentOrder, Override

### Community 6 - "metrics-consumer (App B)"
Cohesion: 0.08
Nodes (38): KAFKA_AUTO_CREATE_TOPICS_ENABLE=false, Listeners PLAINTEXT (9092) e PLAINTEXT_HOST (9094), Broker Kafka (apache/kafka:4.1.2), Kafka UI (kafbat/kafka-ui:8090), KRaft: nó único broker+controller, Fatores de replicação 1 para nó único, Tópico Kafka "application-logs", Gargalo: commit por mensagem no consumidor (+30 more)

### Community 7 - "TimeRange"
Cohesion: 0.11
Nodes (12): DuckDbOrderLedgerRepository, Override, InMemoryOrderLedgerRepository, Override, Override, SqliteOrderLedgerRepository, OrderLedgerRepositoryContractTest, FakeLedger (+4 more)

### Community 8 - "org.springframework.context.annotation.Configuration"
Cohesion: 0.16
Nodes (10): AuditPersistenceConfiguration, DuckDbPersistence, JsonlPersistence, StdoutPersistence, DuckDbPersistence, InMemoryPersistence, PersistenceConfiguration, SqlitePersistence (+2 more)

### Community 9 - "Violation"
Cohesion: 0.12
Nodes (13): AuditExceptionHandler, java.time.format.DateTimeParseException, AnalyticsExceptionHandler, ApiError, ApiExceptionHandler, InvalidOrderException, Override, Violation (+5 more)

### Community 10 - "org.junit.jupiter.api.Test"
Cohesion: 0.05
Nodes (28): KafkaErrorHandlingConfigTest, ApplicationName, Override, AuditFilterTest, CustomerId, KafkaErrorHandlingConfigTest, TimeRange, RevenueWindowTest (+20 more)

### Community 11 - "Money"
Cohesion: 0.10
Nodes (10): DomainEvent, OrderPlaced, CustomerId, Override, Override, Money, Override, ProductId (+2 more)

### Community 12 - "FraudTopologyTest"
Cohesion: 0.14
Nodes (10): dev.joaolaureano.trainingkafka.fraud.adapters.messaging.AuditEventMessage, dev.joaolaureano.trainingkafka.fraud.adapters.messaging.OrderPlacedMessage, AuditEventMessage, FraudTopologyTest, OrderPlacedMessage, org.apache.kafka.streams.state.KeyValueStore, org.apache.kafka.streams.TestInputTopic, org.apache.kafka.streams.TestOutputTopic (+2 more)

### Community 13 - "DeadLetterProperties"
Cohesion: 0.11
Nodes (4): FraudProperties, DeadLetterProperties, KafkaErrorHandlingConfig, org.springframework.boot.context.properties.ConfigurationProperties

### Community 14 - "orders-load.js"
Cohesion: 0.17
Nodes (13): acceptanceRate, CATALOG, options, orderLatency, ordersAccepted, ordersRejected, placeNormalOrder(), placeOrder() (+5 more)

### Community 15 - "package.json"
Cohesion: 0.14
Nodes (13): esbuild, @faker-js/faker, description, devDependencies, esbuild, @faker-js/faker, name, private (+5 more)

### Community 16 - "org.springframework.boot.autoconfigure.condition.ConditionalOnProperty"
Cohesion: 0.24
Nodes (11): KafkaErrorHandlingConfig, org.slf4j.Logger, org.springframework.boot.autoconfigure.condition.ConditionalOnProperty, org.springframework.boot.autoconfigure.kafka.KafkaProperties, org.springframework.boot.context.properties.EnableConfigurationProperties, org.springframework.kafka.core.KafkaOperations, org.springframework.kafka.listener.CommonErrorHandler, org.springframework.kafka.listener.DefaultErrorHandler (+3 more)

### Community 17 - "org.springframework.boot.autoconfigure.SpringBootApplication"
Cohesion: 0.18
Nodes (7): AuditServiceBootstrap, FraudServiceBootstrap, MetricsConsumerBootstrap, OrderServiceBootstrap, org.springframework.boot.autoconfigure.SpringBootApplication, org.springframework.scheduling.annotation.EnableScheduling, PaymentServiceBootstrap

### Community 18 - "AuditServiceWiring.java"
Cohesion: 0.14
Nodes (9): AuditEventListener, IngestAuditPort, AuditQueryPort, AuditQueryService, IngestAuditService, AuditServiceWiring, AuditQueryFacade, Override (+1 more)

### Community 19 - "AuditEvent"
Cohesion: 0.20
Nodes (5): Override, StdoutAuditRepository, StdoutAuditRepositoryTest, Override, AuditEvent

### Community 22 - "CustomerFraudPattern"
Cohesion: 0.21
Nodes (5): CustomerFraudPattern, FraudOrder, FraudPolicy, FraudPolicy, CustomerFraudPatternTest

### Community 23 - "Payment"
Cohesion: 0.09
Nodes (16): FakeRepository, Override, FakeRepository, Override, DomainEvent, PaymentApproved, PaymentCancelled, PaymentFailed (+8 more)

### Community 24 - "OutboxRecord"
Cohesion: 0.05
Nodes (22): OrderListener, OrderPlacedMessage, OutboxRelayScheduler, PaymentEventListener, org.springframework.kafka.annotation.KafkaListener, org.springframework.scheduling.annotation.Scheduled, org.springframework.stereotype.Component, FraudDetectedMessage (+14 more)

### Community 25 - "AuditRepository"
Cohesion: 0.33
Nodes (6): AuditRepositoryContractTest, AuditLevel, Connection, AuditRepository, org.junit.jupiter.params.ParameterizedTest, org.junit.jupiter.params.provider.MethodSource

### Community 34 - "FraudTopology.java"
Cohesion: 0.21
Nodes (13): CustomerFraudState, dev.joaolaureano.trainingkafka.fraud.adapters.streams.CustomerFraudState, dev.joaolaureano.trainingkafka.fraud.application.FraudDetectionService, dev.joaolaureano.trainingkafka.fraud.domain.model.FraudPolicy, FraudTopology, FraudTransformer, FraudTransformerSupplier, Override (+5 more)

### Community 35 - "AuditFilter"
Cohesion: 0.14
Nodes (3): AuditEventView, AuditFilter, TimeRange

### Community 36 - "OrderPlaced"
Cohesion: 0.10
Nodes (12): OrderPlacedPort, OrderPlacedHandler, OrderPlaced, ProductId, OrderPlacedHandlerTest, Override, OrderPlacedFacade, OrderPlaced (+4 more)

### Community 39 - "FraudDetectedMessage"
Cohesion: 0.24
Nodes (5): dev.joaolaureano.trainingkafka.fraud.domain.model.FraudOrder, FraudDetectedMessage, FraudulentOrder, FraudDetectedMessage, FraudDetected

### Community 42 - "org.springframework.context.annotation.Bean"
Cohesion: 0.23
Nodes (4): KafkaTopicsConfig, Topics, org.apache.kafka.clients.admin.NewTopic, org.springframework.context.annotation.Bean

### Community 46 - "OrderServiceWiring.java"
Cohesion: 0.11
Nodes (10): dev.joaolaureano.trainingkafka.orders.adapters.messaging.KafkaActivityLogPublisher, dev.joaolaureano.trainingkafka.orders.adapters.web.PlaceOrderPort, dev.joaolaureano.trainingkafka.orders.application.PlaceOrderUseCase, KafkaActivityLogPublisher, PaymentEventMessage, PaymentEventPort, UnknownPaymentEventException, ApplyPaymentResult (+2 more)

### Community 47 - "PaymentWiring.java"
Cohesion: 0.09
Nodes (13): DeterministicPaymentGateway, FraudEventPort, OrderPlacedListener, OrderPlacedMessage, OrderPlacedPort, CompensateFraudulentOrders, ProcessOrderPayment, PaymentWiring (+5 more)

### Community 48 - "AuditLevel"
Cohesion: 0.24
Nodes (7): AuditLevel, DEBUG, ERROR, INFO, WARN, isAtLeast(), parse()

### Community 49 - "OrderId"
Cohesion: 0.09
Nodes (14): dev.joaolaureano.trainingkafka.orders.application.port.ActivityLog, dev.joaolaureano.trainingkafka.orders.application.port.ActivityLogPublisher, dev.joaolaureano.trainingkafka.orders.domain.event.DomainEvent, PlaceOrderService, FakeRepository, Override, Override, RecordingLogPublisher (+6 more)

### Community 50 - "KafkaActivityLogPublisher"
Cohesion: 0.11
Nodes (10): AuditEventMessage, KafkaActivityLogPublisher, ActivityLog, ActivityLogPublisher, AuditLevel, ERROR, INFO, WARN (+2 more)

### Community 51 - "ProductSalesRepository"
Cohesion: 0.20
Nodes (5): ProductId, ProductSalesRepositoryContractTest, Override, Quantity, ProductSalesRepository

### Community 52 - "FraudStreamsConfiguration.java"
Cohesion: 0.47
Nodes (4): FraudStreamsConfiguration, KafkaStreamsConfiguration, org.springframework.kafka.annotation.EnableKafkaStreams, org.springframework.kafka.config.KafkaStreamsConfiguration

### Community 53 - "Anticorruption Layer de tradução na fronteira"
Cohesion: 0.50
Nodes (5): Anticorruption Layer de tradução na fronteira, Contratos de evento duplicados por serviço, LogEntryTranslator, spring.json.add.type.headers=false, OrderPlacedTranslator

### Community 62 - "MetricsQueryService"
Cohesion: 0.18
Nodes (5): MetricsQueryPort, MetricsQueryService, AnalyticsWiring, Override, MetricsQueryFacade

### Community 64 - "OccurredAtTimestampExtractor.java"
Cohesion: 0.32
Nodes (5): OrderPlacedMessage, Override, OccurredAtTimestampExtractor, org.apache.kafka.clients.consumer.ConsumerRecord, org.apache.kafka.streams.processor.TimestampExtractor

### Community 65 - "ProcessOrderPaymentTest"
Cohesion: 0.22
Nodes (5): Override, RecordingGateway, ProcessOrderPaymentTest, RecordingGateway, GatewayResult

### Community 67 - "DeadLetterProperties"
Cohesion: 0.16
Nodes (3): DeadLetterProperties, DefaultErrorHandler, KafkaErrorHandlingConfig

### Community 69 - "JsonlFileAuditRepository"
Cohesion: 0.33
Nodes (4): ObjectMapper, Override, JsonlFileAuditRepository, StoredLine

### Community 70 - "com.fasterxml.jackson.databind.ObjectMapper"
Cohesion: 0.33
Nodes (5): com.fasterxml.jackson.databind.ObjectMapper, Override, KafkaOutboxDispatcher, org.springframework.kafka.core.KafkaTemplate, KafkaOutboxDispatcher

### Community 71 - ".fraudStream"
Cohesion: 0.25
Nodes (6): FraudPolicy, FraudDetectionService, FraudProperties, org.apache.kafka.streams.kstream.KStream, org.apache.kafka.streams.StreamsBuilder, TopologyTestDriver

### Community 74 - "PaymentEventOutboxTranslator"
Cohesion: 0.13
Nodes (6): PaymentEventMessage, OutboxRecord, Override, PaymentEventMessage, PaymentEventOutboxTranslator, OutboxTranslator

### Community 75 - "SqliteOrderRepository.java"
Cohesion: 0.19
Nodes (9): dev.joaolaureano.trainingkafka.orders.domain.model.CustomerId, dev.joaolaureano.trainingkafka.orders.domain.model.Money, dev.joaolaureano.trainingkafka.orders.domain.model.ProductId, dev.joaolaureano.trainingkafka.orders.domain.model.Quantity, InvalidOrderTransitionException, OrderStatus, CANCELLED, PAID (+1 more)

### Community 76 - "FraudOrder"
Cohesion: 0.33
Nodes (3): CustomerFraudState, FraudDetectionService, FraudOrder

### Community 77 - "OutboxRecord"
Cohesion: 0.12
Nodes (10): OutboxRelay, FunctionalInterface, OutboxDispatcher, OutboxRecord, OutboxStore, FakeStore, OutboxRecord, Override (+2 more)

### Community 78 - "Money"
Cohesion: 0.14
Nodes (6): OrderPlaced, Quantity, OrderPlacedTranslator, InvalidValueException, Override, Money

### Community 79 - "SqliteOrderRepositoryTest.java"
Cohesion: 0.13
Nodes (6): dev.joaolaureano.trainingkafka.orders.domain.event.OrderPlaced, OutboxRecord, Override, OrderEventOutboxTranslator, OrderPlacedMessage, OutboxTranslator

### Community 82 - "InvalidAuditException"
Cohesion: 0.23
Nodes (3): AuditEventMessage, AuditEventTranslator, InvalidAuditException

### Community 90 - "FraudEventConsumerConfig.java"
Cohesion: 0.50
Nodes (3): ConcurrentKafkaListenerContainerFactory, org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory, FraudEventConsumerConfig

## Knowledge Gaps
- **62 isolated node(s):** `order-service-adapters`, `PENDING_PAYMENT`, `PAID`, `CANCELLED`, `payment-service-adapters` (+57 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **46 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `OrderId` connect `OrderId` to `AuditController.java`, `Money`, `org.junit.jupiter.api.Test`, `SqliteOrderRepository.java`?**
  _High betweenness centrality (0.062) - this node is a cross-community bridge._
- **Why does `DeadLetterProperties` connect `DeadLetterProperties` to `org.springframework.boot.autoconfigure.condition.ConditionalOnProperty`, `org.springframework.context.annotation.Bean`, `DeadLetterProperties`?**
  _High betweenness centrality (0.040) - this node is a cross-community bridge._
- **Why does `AuditEvent` connect `AuditEvent` to `AuditController.java`, `DuckDbAuditRepository`, `AuditFilter`, `java.sql.Connection`, `JsonlFileAuditRepository`, `org.junit.jupiter.api.Test`, `AuditLevel`, `InvalidAuditException`, `AuditServiceWiring.java`, `AuditRepository`?**
  _High betweenness centrality (0.036) - this node is a cross-community bridge._
- **What connects `order-service-adapters`, `PENDING_PAYMENT`, `PAID` to the rest of the system?**
  _62 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `AuditController.java` be split into smaller, more focused modules?**
  _Cohesion score 0.1 - nodes in this community are weakly interconnected._
- **Should `ProductSalesRecord` be split into smaller, more focused modules?**
  _Cohesion score 0.08672699849170437 - nodes in this community are weakly interconnected._
- **Should `java.sql.Connection` be split into smaller, more focused modules?**
  _Cohesion score 0.13911290322580644 - nodes in this community are weakly interconnected._