# Graph Report - training_kafka  (2026-09-03)

## Corpus Check
- 225 files · ~40,361 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1383 nodes · 3708 edges · 91 communities (51 shown, 40 thin omitted)
- Extraction: 88% EXTRACTED · 12% INFERRED · 0% AMBIGUOUS · INFERRED: 434 edges (avg confidence: 0.81)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `839053a1`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- PlaceOrderUseCase
- Units
- .request
- PersistenceConfiguration.java
- DeadLetterProperties
- PaymentRepository
- metrics-consumer (App B)
- ProductId
- java.sql.Connection
- Violation
- org.junit.jupiter.api.Test
- Money
- FraudTopology.java
- DeadLetterProperties
- orders-load.js
- package.json
- org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
- org.springframework.boot.autoconfigure.SpringBootApplication
- AuditServiceWiring.java
- AuditPersistenceConfiguration.java
- SqliteRepositoryException
- dev.joaolaureano.trainingkafka:training-kafka
- .place
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
- SqliteOrderRepository
- AuditFilter
- OrderRecord
- audit-service
- audit-service-adapters
- .validOrder
- analytics/adapters/messaging/AuditEventMessage.java
- audit-service-application
- org.springframework.context.annotation.Bean
- Topics
- audit-service-bootstrap
- audit-service-domain
- org.springframework.stereotype.Component
- PaymentWiring.java
- org.junit.jupiter.api.DisplayName
- Order
- org.junit.jupiter.api.BeforeEach
- ProductSalesRecord
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
- OutboxStore
- SqlitePaymentRepository
- ProcessOrderPaymentTest
- DuckDbAuditRepository
- DeadLetterProperties
- DeadLetterProperties
- AuditEvent
- OrderServiceWiring.java
- .averageTicket
- ApplyPaymentResult
- Retry
- SqlitePaymentRepositoryTest.java
- SqliteOrderRepository.java
- .handle
- OutboxRecord
- Money
- OrderServiceWiring
- Retry
- UnknownOrderException
- InvalidAuditException
- payment-service
- payment-service-adapters
- payment-service-application
- payment-service-bootstrap
- payment-service-domain
- .place
- .translate
- .publish

## God Nodes (most connected - your core abstractions)
1. `Payment` - 51 edges
2. `ProductSalesRecord` - 49 edges
3. `Order` - 48 edges
4. `ProductId` - 45 edges
5. `AuditEvent` - 43 edges
6. `ProductSalesRepository` - 34 edges
7. `AuditRepository` - 33 edges
8. `TimeRange` - 32 edges
9. `OrderLedgerRepository` - 31 edges
10. `OrderId` - 31 edges

## Surprising Connections (you probably didn't know these)
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` --conceptually_related_to--> `order-service (App A)`  [INFERRED]
  docker-compose.yml → README.md
- `training_kafka (laboratório Kafka + DDD)` --references--> `Kafka UI (kafbat/kafka-ui:8090)`  [EXTRACTED]
  README.md → docker-compose.yml
- `training_kafka (laboratório Kafka + DDD)` --references--> `Broker Kafka (apache/kafka:4.1.2)`  [EXTRACTED]
  README.md → docker-compose.yml
- `DuckDbAuditRepository` --implements--> `AuditRepository`  [EXTRACTED]
  audit-service/audit-service-adapters/src/main/java/dev/joaolaureano/trainingkafka/audit/adapters/persistence/duckdb/DuckDbAuditRepository.java → audit-service/audit-service-domain/src/main/java/dev/joaolaureano/trainingkafka/audit/domain/port/AuditRepository.java
- `JsonlFileAuditRepository` --implements--> `AuditRepository`  [EXTRACTED]
  audit-service/audit-service-adapters/src/main/java/dev/joaolaureano/trainingkafka/audit/adapters/persistence/jsonl/JsonlFileAuditRepository.java → audit-service/audit-service-domain/src/main/java/dev/joaolaureano/trainingkafka/audit/domain/port/AuditRepository.java

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Pipeline de eventos A → orders → B → application-logs → C** — readme_order_service, readme_orders_topic, readme_metrics_consumer, readme_application_logs_topic, readme_log_aggregator, docker_compose_kafka_broker [EXTRACTED 1.00]
- **Modelo de domínio do App B (3 raízes + VO derivado + handler)** — readme_productsalesrecord, readme_customerorderpattern, readme_orderrecord, readme_revenuewindow, readme_orderplacedhandler [EXTRACTED 1.00]

## Communities (91 total, 40 thin omitted)

### Community 0 - "PlaceOrderUseCase"
Cohesion: 0.18
Nodes (5): PlaceOrderPort, PlaceOrderRequest, PlaceOrderUseCase, Override, PlaceOrderFacade

### Community 1 - "Units"
Cohesion: 0.15
Nodes (8): Override, ProductId, Money, Override, ProductId, TopSellerPolicy, Override, Units

### Community 2 - ".request"
Cohesion: 0.21
Nodes (4): FraudulentOrder, CompensateFraudulentOrdersTest, FraudulentOrder, PaymentTest

### Community 3 - "PersistenceConfiguration.java"
Cohesion: 0.16
Nodes (9): java.sql.PreparedStatement, java.sql.ResultSet, DuckDbProductSalesRepository, InMemoryOrderLedgerRepository, Override, MoneyCents, SqliteOrderLedgerRepository, SqliteProductSalesRepository (+1 more)

### Community 5 - "PaymentRepository"
Cohesion: 0.14
Nodes (8): FraudDetectedMessage, FraudulentOrder, FraudEventListener, FraudEventPort, CompensateFraudulentOrders, FraudEventFacade, Override, PaymentRepository

### Community 6 - "metrics-consumer (App B)"
Cohesion: 0.08
Nodes (38): KAFKA_AUTO_CREATE_TOPICS_ENABLE=false, Listeners PLAINTEXT (9092) e PLAINTEXT_HOST (9094), Broker Kafka (apache/kafka:4.1.2), Kafka UI (kafbat/kafka-ui:8090), KRaft: nó único broker+controller, Fatores de replicação 1 para nó único, Tópico Kafka "application-logs", Gargalo: commit por mensagem no consumidor (+30 more)

### Community 7 - "ProductId"
Cohesion: 0.15
Nodes (8): DuckDbOrderLedgerRepository, OrderLedgerRepositoryContractTest, Override, Override, ProductId, RevenueWindow, TimeRange, OrderLedgerRepository

### Community 8 - "java.sql.Connection"
Cohesion: 0.10
Nodes (12): AuditPersistenceConfiguration, DuckDbPersistence, JsonlPersistence, StdoutPersistence, java.sql.Connection, TestRepositories, DuckDbPersistence, InMemoryPersistence (+4 more)

### Community 9 - "Violation"
Cohesion: 0.12
Nodes (13): AuditExceptionHandler, java.time.format.DateTimeParseException, AnalyticsExceptionHandler, ApiError, ApiExceptionHandler, InvalidOrderException, Override, Violation (+5 more)

### Community 10 - "org.junit.jupiter.api.Test"
Cohesion: 0.15
Nodes (5): KafkaErrorHandlingConfigTest, AuditFilterTest, KafkaErrorHandlingConfigTest, org.junit.jupiter.api.Test, org.springframework.boot.test.context.runner.ApplicationContextRunner

### Community 11 - "Money"
Cohesion: 0.11
Nodes (10): OrderPlacedMessage, OrderPlaced, CustomerId, Override, Override, Money, Override, ProductId (+2 more)

### Community 12 - "FraudTopology.java"
Cohesion: 0.05
Nodes (37): AuditEventMessage, FraudDetectedMessage, FraudulentOrder, OrderPlacedMessage, Topics, CustomerFraudState, Override, OccurredAtTimestampExtractor (+29 more)

### Community 13 - "DeadLetterProperties"
Cohesion: 0.08
Nodes (4): FraudProperties, DeadLetterProperties, Retry, org.springframework.boot.context.properties.ConfigurationProperties

### Community 14 - "orders-load.js"
Cohesion: 0.17
Nodes (13): acceptanceRate, CATALOG, options, orderLatency, ordersAccepted, ordersRejected, placeNormalOrder(), placeOrder() (+5 more)

### Community 15 - "package.json"
Cohesion: 0.14
Nodes (13): esbuild, @faker-js/faker, description, devDependencies, esbuild, @faker-js/faker, name, private (+5 more)

### Community 16 - "org.springframework.boot.autoconfigure.condition.ConditionalOnProperty"
Cohesion: 0.18
Nodes (13): ConcurrentKafkaListenerContainerFactory, KafkaErrorHandlingConfig, org.slf4j.Logger, org.springframework.boot.autoconfigure.condition.ConditionalOnProperty, org.springframework.boot.autoconfigure.kafka.KafkaProperties, org.springframework.boot.context.properties.EnableConfigurationProperties, org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory, org.springframework.kafka.core.KafkaOperations (+5 more)

### Community 17 - "org.springframework.boot.autoconfigure.SpringBootApplication"
Cohesion: 0.18
Nodes (7): AuditServiceBootstrap, FraudServiceBootstrap, MetricsConsumerBootstrap, OrderServiceBootstrap, org.springframework.boot.autoconfigure.SpringBootApplication, org.springframework.scheduling.annotation.EnableScheduling, PaymentServiceBootstrap

### Community 18 - "AuditServiceWiring.java"
Cohesion: 0.10
Nodes (11): AuditEventListener, IngestAuditPort, AuditController, AuditQueryPort, AuditQueryService, IngestAuditService, AuditServiceWiring, AuditQueryFacade (+3 more)

### Community 19 - "AuditPersistenceConfiguration.java"
Cohesion: 0.26
Nodes (3): Override, StdoutAuditRepository, StdoutAuditRepositoryTest

### Community 22 - ".place"
Cohesion: 0.33
Nodes (5): OrderController, PlaceOrderResponse, org.springframework.web.bind.annotation.PostMapping, org.springframework.web.bind.annotation.RequestMapping, org.springframework.web.bind.annotation.RestController

### Community 23 - "Payment"
Cohesion: 0.08
Nodes (16): FakeRepository, Override, FakeRepository, Override, DomainEvent, PaymentApproved, PaymentCancelled, PaymentFailed (+8 more)

### Community 24 - "OutboxRecord"
Cohesion: 0.10
Nodes (11): Override, OutboxRelay, FunctionalInterface, OutboxDispatcher, OutboxRecord, OutboxStore, FakeStore, OutboxRecord (+3 more)

### Community 25 - "AuditRepository"
Cohesion: 0.33
Nodes (6): AuditRepositoryContractTest, AuditLevel, Connection, AuditRepository, org.junit.jupiter.params.ParameterizedTest, org.junit.jupiter.params.provider.MethodSource

### Community 34 - "SqliteOrderRepository"
Cohesion: 0.23
Nodes (5): ObjectMapper, OutboxRecord, Override, SqliteOrderRepository, SqliteOrderRepositoryTest

### Community 35 - "AuditFilter"
Cohesion: 0.11
Nodes (12): ApplicationName, Override, AuditFilter, AuditLevel, DEBUG, ERROR, INFO, WARN (+4 more)

### Community 36 - "OrderRecord"
Cohesion: 0.11
Nodes (13): OrderPlacedHandler, FakeLedger, OrderPlaced, ProductId, OrderPlacedHandlerTest, Override, OrderPlacedFacade, OrderPlaced (+5 more)

### Community 42 - "org.springframework.context.annotation.Bean"
Cohesion: 0.23
Nodes (4): KafkaTopicsConfig, Topics, org.apache.kafka.clients.admin.NewTopic, org.springframework.context.annotation.Bean

### Community 46 - "org.springframework.stereotype.Component"
Cohesion: 0.08
Nodes (13): OrderListener, OrderPlacedMessage, OrderPlacedPort, OutboxRelayScheduler, PaymentEventListener, PaymentEventMessage, PaymentEventPort, UnknownPaymentEventException (+5 more)

### Community 47 - "PaymentWiring.java"
Cohesion: 0.11
Nodes (9): DeterministicPaymentGateway, OrderPlacedMessage, OrderPlacedPort, DeterministicPaymentGatewayTest, ProcessOrderPayment, PaymentWiring, Override, OrderPlacedFacade (+1 more)

### Community 48 - "org.junit.jupiter.api.DisplayName"
Cohesion: 0.20
Nodes (5): ProductSalesRecordTest, MoneyRules, WhenInvalid, org.junit.jupiter.api.DisplayName, org.junit.jupiter.api.Nested

### Community 49 - "Order"
Cohesion: 0.13
Nodes (10): FakeRepository, Override, Override, RecordingRepository, DomainEvent, Override, Order, Override (+2 more)

### Community 50 - "org.junit.jupiter.api.BeforeEach"
Cohesion: 0.11
Nodes (10): PlaceOrderService, ActivityLog, ActivityLogPublisher, AuditLevel, ERROR, INFO, WARN, RecordingLogPublisher (+2 more)

### Community 51 - "ProductSalesRecord"
Cohesion: 0.13
Nodes (10): InMemoryProductSalesRepository, Override, ProductId, ProductSalesRepositoryContractTest, FakeProductSales, Override, ProductSalesRecord, Override (+2 more)

### Community 52 - "FraudStreamsConfiguration.java"
Cohesion: 0.47
Nodes (4): FraudStreamsConfiguration, KafkaStreamsConfiguration, org.springframework.kafka.annotation.EnableKafkaStreams, org.springframework.kafka.config.KafkaStreamsConfiguration

### Community 53 - "Anticorruption Layer de tradução na fronteira"
Cohesion: 0.50
Nodes (5): Anticorruption Layer de tradução na fronteira, Contratos de evento duplicados por serviço, LogEntryTranslator, spring.json.add.type.headers=false, OrderPlacedTranslator

### Community 62 - "MetricsQueryService"
Cohesion: 0.12
Nodes (8): MetricsController, ProductSalesView, RevenueView, MetricsQueryPort, MetricsQueryService, AnalyticsWiring, Override, MetricsQueryFacade

### Community 63 - "OutboxStore"
Cohesion: 0.26
Nodes (5): OutboxRelay, FunctionalInterface, OutboxDispatcher, OutboxStore, OutboxRelay

### Community 64 - "SqlitePaymentRepository"
Cohesion: 0.25
Nodes (4): OutboxRecord, Override, SqlitePaymentRepository, SqlitePaymentRepositoryTest

### Community 65 - "ProcessOrderPaymentTest"
Cohesion: 0.24
Nodes (5): Override, RecordingGateway, ProcessOrderPaymentTest, RecordingGateway, GatewayResult

### Community 67 - "DeadLetterProperties"
Cohesion: 0.16
Nodes (3): DeadLetterProperties, DefaultErrorHandler, KafkaErrorHandlingConfig

### Community 68 - "DeadLetterProperties"
Cohesion: 0.10
Nodes (4): DeadLetterProperties, Retry, DefaultErrorHandler, KafkaErrorHandlingConfig

### Community 69 - "AuditEvent"
Cohesion: 0.31
Nodes (5): Override, JsonlFileAuditRepository, StoredLine, AuditEventView, AuditEvent

### Community 70 - "OrderServiceWiring.java"
Cohesion: 0.24
Nodes (7): com.fasterxml.jackson.databind.ObjectMapper, KafkaActivityLogPublisher, KafkaOutboxDispatcher, OrderEventOutboxTranslator, ActivityLogFacade, org.springframework.kafka.core.KafkaTemplate, KafkaOutboxDispatcher

### Community 72 - "ApplyPaymentResult"
Cohesion: 0.22
Nodes (4): ApplyPaymentResult, ApplyPaymentResultTest, Override, PaymentEventFacade

### Community 74 - "SqlitePaymentRepositoryTest.java"
Cohesion: 0.15
Nodes (6): PaymentEventMessage, OutboxRecord, Override, PaymentEventMessage, PaymentEventOutboxTranslator, OutboxTranslator

### Community 75 - "SqliteOrderRepository.java"
Cohesion: 0.27
Nodes (5): InvalidOrderTransitionException, OrderStatus, CANCELLED, PAID, PENDING_PAYMENT

### Community 76 - ".handle"
Cohesion: 0.42
Nodes (3): PlaceOrderCommand, Override, PlaceOrderServiceTest

### Community 77 - "OutboxRecord"
Cohesion: 0.16
Nodes (6): Override, OutboxRecord, FakeStore, OutboxRecord, Override, OutboxRelayTest

### Community 78 - "Money"
Cohesion: 0.11
Nodes (8): OrderPlaced, Quantity, OrderPlacedTranslator, Override, Override, InvalidValueException, Override, Money

### Community 82 - "InvalidAuditException"
Cohesion: 0.27
Nodes (3): AuditEventMessage, AuditEventTranslator, InvalidAuditException

### Community 88 - ".place"
Cohesion: 0.29
Nodes (4): CustomerId, Money, ProductId, Quantity

## Knowledge Gaps
- **61 isolated node(s):** `audit-service-adapters`, `audit-service-application`, `audit-service-bootstrap`, `audit-service-domain`, `DEBUG` (+56 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **40 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `AuditEvent` connect `AuditEvent` to `DuckDbAuditRepository`, `AuditFilter`, `org.junit.jupiter.api.Test`, `AuditServiceWiring.java`, `AuditPersistenceConfiguration.java`, `InvalidAuditException`, `AuditRepository`?**
  _High betweenness centrality (0.045) - this node is a cross-community bridge._
- **Why does `DeadLetterProperties` connect `DeadLetterProperties` to `org.springframework.boot.autoconfigure.condition.ConditionalOnProperty`, `org.springframework.context.annotation.Bean`?**
  _High betweenness centrality (0.042) - this node is a cross-community bridge._
- **Why does `DeadLetterProperties` connect `DeadLetterProperties` to `Retry`, `org.springframework.context.annotation.Bean`, `DeadLetterProperties`?**
  _High betweenness centrality (0.032) - this node is a cross-community bridge._
- **What connects `audit-service-adapters`, `audit-service-application`, `audit-service-bootstrap` to the rest of the system?**
  _61 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `PaymentRepository` be split into smaller, more focused modules?**
  _Cohesion score 0.14285714285714285 - nodes in this community are weakly interconnected._
- **Should `metrics-consumer (App B)` be split into smaller, more focused modules?**
  _Cohesion score 0.07539118065433854 - nodes in this community are weakly interconnected._
- **Should `java.sql.Connection` be split into smaller, more focused modules?**
  _Cohesion score 0.10084033613445378 - nodes in this community are weakly interconnected._