# Graph Report - training_kafka  (2026-09-03)

## Corpus Check
- 240 files · ~48,081 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1505 nodes · 4011 edges · 98 communities (55 shown, 43 thin omitted)
- Extraction: 88% EXTRACTED · 12% INFERRED · 0% AMBIGUOUS · INFERRED: 463 edges (avg confidence: 0.81)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `1de2dc3c`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- AuditController.java
- ProductSalesRecord
- SqlitePaymentRepository
- ProductId
- DeadLetterProperties
- org.junit.jupiter.api.DisplayName
- metrics-consumer (App B)
- MetricsQueryService
- org.springframework.context.annotation.Bean
- .place
- Order
- Money
- FraudTopology.java
- SqliteOrderRepositoryTest.java
- orders-load.js
- package.json
- org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
- org.springframework.boot.autoconfigure.SpringBootApplication
- AuditServiceWiring.java
- AuditEvent
- SqliteRepositoryException
- dev.joaolaureano.trainingkafka:training-kafka
- Payment
- DomainEvent
- OrderServiceWiring.java
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
- OrderPlaced
- audit-service
- audit-service-adapters
- org.springframework.stereotype.Component
- analytics/adapters/messaging/AuditEventMessage.java
- audit-service-application
- Topics
- Topics
- audit-service-bootstrap
- audit-service-domain
- ApplyPaymentResultTest
- .charge
- OutboxRecord
- run.sh
- .validOrder
- ProductSalesRepository
- .request
- Anticorruption Layer de tradução na fronteira
- fraud-service
- metrics-consumer-bootstrap
- order-service-bootstrap
- fraud-service-adapters
- fraud-service-application
- fraud-service-bootstrap
- PaymentWiring.java
- fraud-service-domain
- OrderServiceWiring
- DeadLetterProperties
- OutboxRecord
- ProcessOrderPaymentTest
- ApplicationName
- DeadLetterProperties
- DeadLetterProperties
- JsonlFileAuditRepository
- KafkaActivityLogPublisher
- PlaceOrderServiceTest.java
- .handle
- OrderStatus
- PaymentEventPort
- InvalidAuditException
- org.junit.jupiter.api.Test
- TestRepositories
- Money
- OutboxRelayTest
- Retry
- UnknownOrderException
- FindOrderServiceTest.java
- payment-service
- payment-service-adapters
- payment-service-application
- payment-service-bootstrap
- payment-service-domain
- FraudStreamsConfiguration.java
- AuditEventMessageContractTest.java
- Topics
- PersistenceConfiguration.java
- ActivityLog
- FraudEventConsumerConfig.java
- PaymentEventOutboxTranslator
- com.fasterxml.jackson.databind.ObjectMapper
- OrderRepository
- Topics

## God Nodes (most connected - your core abstractions)
1. `Order` - 55 edges
2. `Payment` - 53 edges
3. `ProductSalesRecord` - 49 edges
4. `ProductId` - 45 edges
5. `AuditEvent` - 43 edges
6. `OrderId` - 35 edges
7. `ProductSalesRepository` - 34 edges
8. `AuditRepository` - 33 edges
9. `TimeRange` - 32 edges
10. `OrderLedgerRepository` - 31 edges

## Surprising Connections (you probably didn't know these)
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` --conceptually_related_to--> `order-service (App A)`  [INFERRED]
  docker-compose.yml → README.md
- `training_kafka (laboratório Kafka + DDD)` --references--> `Kafka UI (kafbat/kafka-ui:8090)`  [EXTRACTED]
  README.md → docker-compose.yml
- `training_kafka (laboratório Kafka + DDD)` --references--> `Broker Kafka (apache/kafka:4.1.2)`  [EXTRACTED]
  README.md → docker-compose.yml
- `KafkaErrorHandlingConfig` --references--> `DeadLetterProperties`  [EXTRACTED]
  audit-service/audit-service-adapters/src/main/java/dev/joaolaureano/trainingkafka/audit/adapters/config/KafkaErrorHandlingConfig.java → audit-service/audit-service-adapters/src/main/java/dev/joaolaureano/trainingkafka/audit/adapters/config/DeadLetterProperties.java
- `DuckDbAuditRepository` --implements--> `AuditRepository`  [EXTRACTED]
  audit-service/audit-service-adapters/src/main/java/dev/joaolaureano/trainingkafka/audit/adapters/persistence/duckdb/DuckDbAuditRepository.java → audit-service/audit-service-domain/src/main/java/dev/joaolaureano/trainingkafka/audit/domain/port/AuditRepository.java

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Pipeline de eventos A → orders → B → application-logs → C** — readme_order_service, readme_orders_topic, readme_metrics_consumer, readme_application_logs_topic, readme_log_aggregator, docker_compose_kafka_broker [EXTRACTED 1.00]
- **Modelo de domínio do App B (3 raízes + VO derivado + handler)** — readme_productsalesrecord, readme_customerorderpattern, readme_orderrecord, readme_revenuewindow, readme_orderplacedhandler [EXTRACTED 1.00]

## Communities (98 total, 43 thin omitted)

### Community 0 - "AuditController.java"
Cohesion: 0.08
Nodes (16): AuditController, MetricsController, ProductSalesView, RevenueView, OrderController, OrderResponse, PlaceOrderPort, PlaceOrderRequest (+8 more)

### Community 1 - "ProductSalesRecord"
Cohesion: 0.10
Nodes (13): Override, ProductId, InMemoryProductSalesRepository, Override, Money, Override, ProductId, Override (+5 more)

### Community 2 - "SqlitePaymentRepository"
Cohesion: 0.26
Nodes (4): OutboxRecord, Override, SqlitePaymentRepository, SqlitePaymentRepositoryTest

### Community 3 - "ProductId"
Cohesion: 0.12
Nodes (12): DuckDbOrderLedgerRepository, Override, InMemoryOrderLedgerRepository, Override, OrderLedgerRepositoryContractTest, OrderRecord, Override, ProductId (+4 more)

### Community 4 - "DeadLetterProperties"
Cohesion: 0.12
Nodes (3): DeadLetterProperties, FraudProperties, org.springframework.boot.context.properties.ConfigurationProperties

### Community 5 - "org.junit.jupiter.api.DisplayName"
Cohesion: 0.11
Nodes (7): StdoutAuditRepositoryTest, TimeRange, MoneyRules, OrderTest, WhenInvalid, org.junit.jupiter.api.DisplayName, org.junit.jupiter.api.Nested

### Community 6 - "metrics-consumer (App B)"
Cohesion: 0.08
Nodes (38): KAFKA_AUTO_CREATE_TOPICS_ENABLE=false, Listeners PLAINTEXT (9092) e PLAINTEXT_HOST (9094), Broker Kafka (apache/kafka:4.1.2), Kafka UI (kafbat/kafka-ui:8090), KRaft: nó único broker+controller, Fatores de replicação 1 para nó único, Tópico Kafka "application-logs", Gargalo: commit por mensagem no consumidor (+30 more)

### Community 7 - "MetricsQueryService"
Cohesion: 0.21
Nodes (5): MetricsQueryPort, MetricsQueryService, AnalyticsWiring, Override, MetricsQueryFacade

### Community 8 - "org.springframework.context.annotation.Bean"
Cohesion: 0.11
Nodes (14): AuditPersistenceConfiguration, DuckDbPersistence, JsonlPersistence, StdoutPersistence, java.sql.Connection, DuckDbPersistence, InMemoryPersistence, PersistenceConfiguration (+6 more)

### Community 9 - ".place"
Cohesion: 0.07
Nodes (22): AuditExceptionHandler, java.time.format.DateTimeParseException, OrderPlacedMessage, OrderPlaced, Quantity, OrderPlacedTranslator, AnalyticsExceptionHandler, InvalidValueException (+14 more)

### Community 10 - "Order"
Cohesion: 0.11
Nodes (11): FakeRepository, Override, FakeRepository, Override, Override, RecordingRepository, DomainEvent, Override (+3 more)

### Community 11 - "Money"
Cohesion: 0.11
Nodes (10): OrderPlacedMessage, OrderPlaced, CustomerId, Override, Override, Money, Override, ProductId (+2 more)

### Community 12 - "FraudTopology.java"
Cohesion: 0.05
Nodes (37): AuditEventMessage, FraudDetectedMessage, FraudulentOrder, OrderPlacedMessage, Topics, CustomerFraudState, Override, OccurredAtTimestampExtractor (+29 more)

### Community 13 - "SqliteOrderRepositoryTest.java"
Cohesion: 0.32
Nodes (3): OutboxRecord, Override, OrderEventOutboxTranslator

### Community 14 - "orders-load.js"
Cohesion: 0.07
Nodes (36): acceptanceRate, APPROVAL_LIMIT, awaitStatus(), CATALOG, COMPENSATION_METRICS, COMPENSATION_TIMEOUT_MS, compensationDuration, compensationSettled (+28 more)

### Community 15 - "package.json"
Cohesion: 0.14
Nodes (13): esbuild, @faker-js/faker, description, devDependencies, esbuild, @faker-js/faker, name, private (+5 more)

### Community 16 - "org.springframework.boot.autoconfigure.condition.ConditionalOnProperty"
Cohesion: 0.26
Nodes (10): KafkaErrorHandlingConfig, KafkaErrorHandlingConfig, org.slf4j.Logger, org.springframework.boot.autoconfigure.condition.ConditionalOnProperty, org.springframework.boot.autoconfigure.kafka.KafkaProperties, org.springframework.boot.context.properties.EnableConfigurationProperties, org.springframework.kafka.core.KafkaOperations, org.springframework.kafka.listener.CommonErrorHandler (+2 more)

### Community 17 - "org.springframework.boot.autoconfigure.SpringBootApplication"
Cohesion: 0.18
Nodes (7): AuditServiceBootstrap, FraudServiceBootstrap, MetricsConsumerBootstrap, OrderServiceBootstrap, org.springframework.boot.autoconfigure.SpringBootApplication, org.springframework.scheduling.annotation.EnableScheduling, PaymentServiceBootstrap

### Community 18 - "AuditServiceWiring.java"
Cohesion: 0.14
Nodes (8): AuditEventListener, IngestAuditPort, AuditQueryPort, AuditQueryService, IngestAuditService, AuditServiceWiring, AuditQueryFacade, IngestAuditFacade

### Community 19 - "AuditEvent"
Cohesion: 0.20
Nodes (5): Override, StdoutAuditRepository, Override, Override, AuditEvent

### Community 22 - "Payment"
Cohesion: 0.10
Nodes (13): FunctionalInterface, LogFactory, FakeRepository, Override, FakeRepository, Override, Payment, PaymentId (+5 more)

### Community 23 - "DomainEvent"
Cohesion: 0.16
Nodes (5): DomainEvent, PaymentApproved, PaymentCancelled, PaymentFailed, Override

### Community 24 - "OrderServiceWiring.java"
Cohesion: 0.23
Nodes (5): FindOrderPort, FindOrderUseCase, OrderView, FindOrderFacade, Override

### Community 25 - "AuditRepository"
Cohesion: 0.33
Nodes (6): AuditRepositoryContractTest, AuditLevel, Connection, AuditRepository, org.junit.jupiter.params.ParameterizedTest, org.junit.jupiter.params.provider.MethodSource

### Community 34 - "SqliteOrderRepository"
Cohesion: 0.17
Nodes (6): ObjectMapper, OutboxRecord, Override, SqliteOrderRepository, SqliteOrderRepositoryTest, org.junit.jupiter.api.BeforeEach

### Community 35 - "AuditFilter"
Cohesion: 0.16
Nodes (3): AuditEventView, AuditFilter, TimeRange

### Community 36 - "OrderPlaced"
Cohesion: 0.08
Nodes (16): OrderListener, OrderPlacedPort, OrderPlacedHandler, FakeLedger, FakeProductSales, OrderPlaced, Override, ProductId (+8 more)

### Community 39 - "org.springframework.stereotype.Component"
Cohesion: 0.11
Nodes (11): OutboxRelayScheduler, org.springframework.kafka.annotation.KafkaListener, org.springframework.scheduling.annotation.Scheduled, org.springframework.stereotype.Component, FraudDetectedMessage, FraudulentOrder, FraudEventListener, FraudEventPort (+3 more)

### Community 48 - "OutboxRecord"
Cohesion: 0.12
Nodes (9): Override, OutboxRelay, FunctionalInterface, OutboxDispatcher, OutboxRecord, OutboxStore, FakeStore, Override (+1 more)

### Community 49 - "run.sh"
Cohesion: 0.21
Nodes (15): await_http(), await_log(), cleanup(), die(), log(), LOG_DIR, REPO_ROOT, require() (+7 more)

### Community 51 - "ProductSalesRepository"
Cohesion: 0.28
Nodes (5): ProductId, ProductSalesRepositoryContractTest, Override, Quantity, ProductSalesRepository

### Community 52 - ".request"
Cohesion: 0.20
Nodes (4): FraudulentOrder, CompensateFraudulentOrdersTest, FraudulentOrder, PaymentTest

### Community 53 - "Anticorruption Layer de tradução na fronteira"
Cohesion: 0.50
Nodes (5): Anticorruption Layer de tradução na fronteira, Contratos de evento duplicados por serviço, LogEntryTranslator, spring.json.add.type.headers=false, OrderPlacedTranslator

### Community 60 - "PaymentWiring.java"
Cohesion: 0.10
Nodes (13): DeterministicPaymentGateway, OrderPlacedListener, OrderPlacedMessage, OrderPlacedPort, OutboxTranslator, CompensateFraudulentOrders, ActivityLogPublisher, ProcessOrderPayment (+5 more)

### Community 62 - "OrderServiceWiring"
Cohesion: 0.20
Nodes (3): OutboxTranslator, KafkaActivityLogPublisher, OrderServiceWiring

### Community 64 - "OutboxRecord"
Cohesion: 0.11
Nodes (10): OutboxRelay, FunctionalInterface, OutboxDispatcher, OutboxRecord, OutboxStore, FakeStore, OutboxRecord, Override (+2 more)

### Community 65 - "ProcessOrderPaymentTest"
Cohesion: 0.22
Nodes (5): Override, RecordingGateway, ProcessOrderPaymentTest, RecordingGateway, GatewayResult

### Community 66 - "ApplicationName"
Cohesion: 0.13
Nodes (11): DuckDbAuditRepository, Override, ApplicationName, Override, AuditLevel, DEBUG, ERROR, INFO (+3 more)

### Community 67 - "DeadLetterProperties"
Cohesion: 0.10
Nodes (4): DeadLetterProperties, Retry, DefaultErrorHandler, KafkaErrorHandlingConfig

### Community 68 - "DeadLetterProperties"
Cohesion: 0.10
Nodes (4): DeadLetterProperties, Retry, DefaultErrorHandler, KafkaErrorHandlingConfig

### Community 69 - "JsonlFileAuditRepository"
Cohesion: 0.48
Nodes (3): Override, JsonlFileAuditRepository, StoredLine

### Community 70 - "KafkaActivityLogPublisher"
Cohesion: 0.22
Nodes (4): AuditEventMessage, KafkaActivityLogPublisher, ActivityLogFacade, Override

### Community 71 - "PlaceOrderServiceTest.java"
Cohesion: 0.13
Nodes (8): PlaceOrderService, ActivityLog, ActivityLogPublisher, AuditLevel, ERROR, INFO, WARN, RecordingLogPublisher

### Community 72 - ".handle"
Cohesion: 0.42
Nodes (3): PlaceOrderCommand, Override, PlaceOrderServiceTest

### Community 73 - "OrderStatus"
Cohesion: 0.31
Nodes (5): InvalidOrderTransitionException, OrderStatus, CANCELLED, PAID, PENDING_PAYMENT

### Community 74 - "PaymentEventPort"
Cohesion: 0.19
Nodes (5): PaymentEventListener, PaymentEventMessage, PaymentEventPort, UnknownPaymentEventException, PaymentEventFacade

### Community 75 - "InvalidAuditException"
Cohesion: 0.27
Nodes (3): AuditEventMessage, AuditEventTranslator, InvalidAuditException

### Community 76 - "org.junit.jupiter.api.Test"
Cohesion: 0.16
Nodes (5): KafkaErrorHandlingConfigTest, AuditFilterTest, KafkaErrorHandlingConfigTest, org.junit.jupiter.api.Test, org.springframework.boot.test.context.runner.ApplicationContextRunner

### Community 88 - "FraudStreamsConfiguration.java"
Cohesion: 0.47
Nodes (4): FraudStreamsConfiguration, KafkaStreamsConfiguration, org.springframework.kafka.annotation.EnableKafkaStreams, org.springframework.kafka.config.KafkaStreamsConfiguration

### Community 91 - "PersistenceConfiguration.java"
Cohesion: 0.17
Nodes (7): java.sql.PreparedStatement, java.sql.ResultSet, DuckDbProductSalesRepository, MoneyCents, Override, SqliteOrderLedgerRepository, SqliteProductSalesRepository

### Community 92 - "ActivityLog"
Cohesion: 0.15
Nodes (7): ActivityLog, AuditLevel, ERROR, INFO, WARN, RecordingLogPublisher, RecordingLogPublisher

### Community 93 - "FraudEventConsumerConfig.java"
Cohesion: 0.50
Nodes (3): ConcurrentKafkaListenerContainerFactory, org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory, FraudEventConsumerConfig

### Community 94 - "PaymentEventOutboxTranslator"
Cohesion: 0.24
Nodes (5): PaymentEventMessage, OutboxRecord, Override, PaymentEventMessage, PaymentEventOutboxTranslator

### Community 95 - "com.fasterxml.jackson.databind.ObjectMapper"
Cohesion: 0.17
Nodes (9): com.fasterxml.jackson.databind.ObjectMapper, KafkaOutboxDispatcher, org.springframework.kafka.core.KafkaTemplate, KafkaActivityLogPublisher, Override, KafkaOutboxDispatcher, KafkaActivityLogPublisher, ActivityLogFacade (+1 more)

### Community 96 - "OrderRepository"
Cohesion: 0.39
Nodes (3): ApplyPaymentResult, FindOrderService, OrderRepository

## Knowledge Gaps
- **88 isolated node(s):** `audit-service-adapters`, `audit-service-application`, `audit-service-bootstrap`, `audit-service-domain`, `DEBUG` (+83 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **43 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `AuditEvent` connect `AuditEvent` to `AuditController.java`, `ApplicationName`, `AuditFilter`, `JsonlFileAuditRepository`, `org.junit.jupiter.api.DisplayName`, `InvalidAuditException`, `org.junit.jupiter.api.Test`, `AuditServiceWiring.java`, `AuditRepository`?**
  _High betweenness centrality (0.042) - this node is a cross-community bridge._
- **Why does `Payment` connect `Payment` to `ProcessOrderPaymentTest`, `SqlitePaymentRepository`, `.charge`, `.request`, `DomainEvent`, `PaymentWiring.java`?**
  _High betweenness centrality (0.037) - this node is a cross-community bridge._
- **Why does `DeadLetterProperties` connect `DeadLetterProperties` to `org.springframework.context.annotation.Bean`, `DeadLetterProperties`?**
  _High betweenness centrality (0.031) - this node is a cross-community bridge._
- **What connects `audit-service-adapters`, `audit-service-application`, `audit-service-bootstrap` to the rest of the system?**
  _88 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `AuditController.java` be split into smaller, more focused modules?**
  _Cohesion score 0.08130081300813008 - nodes in this community are weakly interconnected._
- **Should `ProductSalesRecord` be split into smaller, more focused modules?**
  _Cohesion score 0.10196078431372549 - nodes in this community are weakly interconnected._
- **Should `ProductId` be split into smaller, more focused modules?**
  _Cohesion score 0.11591836734693878 - nodes in this community are weakly interconnected._