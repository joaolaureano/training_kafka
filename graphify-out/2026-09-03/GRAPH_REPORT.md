# Graph Report - training_kafka  (2026-09-03)

## Corpus Check
- 233 files · ~43,797 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1448 nodes · 3863 edges · 94 communities (54 shown, 40 thin omitted)
- Extraction: 88% EXTRACTED · 12% INFERRED · 0% AMBIGUOUS · INFERRED: 450 edges (avg confidence: 0.81)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `5c356e2e`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- OrderServiceWiring.java
- ProductSalesRecord
- SqlitePaymentRepository
- ProductId
- DeadLetterProperties
- .handle
- metrics-consumer (App B)
- OrderLedgerRepository
- org.springframework.context.annotation.Bean
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
- AuditEvent
- SqliteRepositoryException
- dev.joaolaureano.trainingkafka:training-kafka
- Payment
- PaymentStatus
- ProcessOrderPayment
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
- org.springframework.stereotype.Component
- analytics/adapters/messaging/AuditEventMessage.java
- audit-service-application
- org.apache.kafka.clients.admin.NewTopic
- Topics
- audit-service-bootstrap
- audit-service-domain
- OrderRepository
- PaymentWiring.java
- OutboxStore
- run.sh
- ActivityLog
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
- OutboxRecord
- ProcessOrderPaymentTest
- com.fasterxml.jackson.databind.ObjectMapper
- DeadLetterProperties
- DeadLetterProperties
- JsonlFileAuditRepository
- org.springframework.kafka.core.KafkaTemplate
- .handle
- Order
- Retry
- ApplicationName
- TestRepositories
- .place
- OutboxRecord
- Money
- OrderId
- Retry
- UnknownOrderException
- InvalidAuditException
- payment-service
- payment-service-adapters
- payment-service-application
- payment-service-bootstrap
- payment-service-domain
- .translate
- OutboxRelayTest
- FraudEventConsumerConfig.java
- FakeProductSales
- Topics
- .publish

## God Nodes (most connected - your core abstractions)
1. `Order` - 55 edges
2. `Payment` - 51 edges
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

## Communities (94 total, 40 thin omitted)

### Community 0 - "OrderServiceWiring.java"
Cohesion: 0.07
Nodes (21): AuditController, MetricsController, ProductSalesView, RevenueView, FindOrderPort, OrderController, OrderResponse, PlaceOrderPort (+13 more)

### Community 1 - "ProductSalesRecord"
Cohesion: 0.09
Nodes (15): DuckDbProductSalesRepository, Override, ProductId, InMemoryProductSalesRepository, Override, Money, Override, ProductId (+7 more)

### Community 2 - "SqlitePaymentRepository"
Cohesion: 0.18
Nodes (5): OutboxRecord, Override, SqlitePaymentRepository, SqlitePaymentRepositoryTest, PaymentTest

### Community 3 - "ProductId"
Cohesion: 0.10
Nodes (12): java.sql.PreparedStatement, DuckDbOrderLedgerRepository, Override, InMemoryOrderLedgerRepository, Override, MoneyCents, Override, SqliteOrderLedgerRepository (+4 more)

### Community 4 - "DeadLetterProperties"
Cohesion: 0.11
Nodes (3): DeadLetterProperties, FraudProperties, org.springframework.boot.context.properties.ConfigurationProperties

### Community 5 - ".handle"
Cohesion: 0.40
Nodes (3): FraudulentOrder, CompensateFraudulentOrdersTest, FraudulentOrder

### Community 6 - "metrics-consumer (App B)"
Cohesion: 0.08
Nodes (38): KAFKA_AUTO_CREATE_TOPICS_ENABLE=false, Listeners PLAINTEXT (9092) e PLAINTEXT_HOST (9094), Broker Kafka (apache/kafka:4.1.2), Kafka UI (kafbat/kafka-ui:8090), KRaft: nó único broker+controller, Fatores de replicação 1 para nó único, Tópico Kafka "application-logs", Gargalo: commit por mensagem no consumidor (+30 more)

### Community 7 - "OrderLedgerRepository"
Cohesion: 0.32
Nodes (3): OrderLedgerRepositoryContractTest, OrderLedgerRepository, RevenueWindowTest

### Community 8 - "org.springframework.context.annotation.Bean"
Cohesion: 0.11
Nodes (14): AuditPersistenceConfiguration, DuckDbPersistence, JsonlPersistence, StdoutPersistence, java.sql.Connection, DuckDbPersistence, InMemoryPersistence, PersistenceConfiguration (+6 more)

### Community 9 - "Violation"
Cohesion: 0.12
Nodes (13): AuditExceptionHandler, java.time.format.DateTimeParseException, AnalyticsExceptionHandler, ApiError, ApiExceptionHandler, InvalidOrderException, Override, Violation (+5 more)

### Community 10 - "org.junit.jupiter.api.Test"
Cohesion: 0.09
Nodes (11): KafkaErrorHandlingConfigTest, KafkaErrorHandlingConfigTest, TimeRange, ApplyPaymentResultTest, MoneyRules, OrderTest, WhenInvalid, org.junit.jupiter.api.DisplayName (+3 more)

### Community 11 - "Money"
Cohesion: 0.12
Nodes (9): OrderPlaced, CustomerId, Override, Override, Money, Override, ProductId, Override (+1 more)

### Community 12 - "FraudTopology.java"
Cohesion: 0.05
Nodes (37): AuditEventMessage, FraudDetectedMessage, FraudulentOrder, OrderPlacedMessage, Topics, CustomerFraudState, Override, OccurredAtTimestampExtractor (+29 more)

### Community 14 - "orders-load.js"
Cohesion: 0.09
Nodes (28): acceptanceRate, APPROVAL_LIMIT, awaitStatus(), CATALOG, COMPENSATION_TIMEOUT_MS, compensationsObserved, FRAUD_MAX_ORDERS, options (+20 more)

### Community 15 - "package.json"
Cohesion: 0.14
Nodes (13): esbuild, @faker-js/faker, description, devDependencies, esbuild, @faker-js/faker, name, private (+5 more)

### Community 16 - "org.springframework.boot.autoconfigure.condition.ConditionalOnProperty"
Cohesion: 0.30
Nodes (9): KafkaErrorHandlingConfig, org.slf4j.Logger, org.springframework.boot.autoconfigure.condition.ConditionalOnProperty, org.springframework.boot.autoconfigure.kafka.KafkaProperties, org.springframework.boot.context.properties.EnableConfigurationProperties, org.springframework.kafka.core.KafkaOperations, org.springframework.kafka.listener.CommonErrorHandler, org.springframework.kafka.listener.DefaultErrorHandler (+1 more)

### Community 17 - "org.springframework.boot.autoconfigure.SpringBootApplication"
Cohesion: 0.18
Nodes (7): AuditServiceBootstrap, FraudServiceBootstrap, MetricsConsumerBootstrap, OrderServiceBootstrap, org.springframework.boot.autoconfigure.SpringBootApplication, org.springframework.scheduling.annotation.EnableScheduling, PaymentServiceBootstrap

### Community 18 - "AuditServiceWiring.java"
Cohesion: 0.14
Nodes (8): AuditEventListener, IngestAuditPort, AuditQueryPort, AuditQueryService, IngestAuditService, AuditServiceWiring, AuditQueryFacade, IngestAuditFacade

### Community 19 - "AuditEvent"
Cohesion: 0.21
Nodes (5): Override, StdoutAuditRepository, StdoutAuditRepositoryTest, Override, AuditEvent

### Community 22 - "Payment"
Cohesion: 0.12
Nodes (7): FakeRepository, Override, FakeRepository, Override, DomainEvent, Payment, PaymentId

### Community 23 - "PaymentStatus"
Cohesion: 0.08
Nodes (14): PaymentEventMessage, OutboxRecord, Override, PaymentEventMessage, PaymentEventOutboxTranslator, PaymentApproved, PaymentCancelled, PaymentFailed (+6 more)

### Community 24 - "ProcessOrderPayment"
Cohesion: 0.19
Nodes (6): OrderPlacedListener, OrderPlacedMessage, OrderPlacedPort, ProcessOrderPayment, Override, OrderPlacedFacade

### Community 25 - "AuditRepository"
Cohesion: 0.33
Nodes (6): AuditRepositoryContractTest, AuditLevel, Connection, AuditRepository, org.junit.jupiter.params.ParameterizedTest, org.junit.jupiter.params.provider.MethodSource

### Community 34 - "SqliteOrderRepository"
Cohesion: 0.32
Nodes (4): OutboxRecord, Override, SqliteOrderRepository, SqliteOrderRepositoryTest

### Community 35 - "AuditFilter"
Cohesion: 0.10
Nodes (11): AuditEventView, Override, AuditFilter, AuditLevel, DEBUG, ERROR, INFO, WARN (+3 more)

### Community 36 - "OrderRecord"
Cohesion: 0.11
Nodes (13): OrderPlacedHandler, FakeLedger, OrderPlaced, ProductId, OrderPlacedHandlerTest, Override, OrderPlacedFacade, OrderPlaced (+5 more)

### Community 39 - "org.springframework.stereotype.Component"
Cohesion: 0.06
Nodes (16): OrderListener, OrderPlacedMessage, OrderPlacedPort, OutboxRelayScheduler, PaymentEventListener, PaymentEventMessage, PaymentEventPort, UnknownPaymentEventException (+8 more)

### Community 42 - "org.apache.kafka.clients.admin.NewTopic"
Cohesion: 0.25
Nodes (3): KafkaTopicsConfig, Topics, org.apache.kafka.clients.admin.NewTopic

### Community 46 - "OrderRepository"
Cohesion: 0.12
Nodes (9): ApplyPaymentResult, FindOrderService, Override, FakeRepository, FindOrderServiceTest, Override, Override, PaymentEventFacade (+1 more)

### Community 47 - "PaymentWiring.java"
Cohesion: 0.11
Nodes (9): DeterministicPaymentGateway, FraudEventPort, OutboxTranslator, DeterministicPaymentGatewayTest, CompensateFraudulentOrders, PaymentWiring, FraudEventFacade, PaymentGateway (+1 more)

### Community 48 - "OutboxStore"
Cohesion: 0.19
Nodes (5): OutboxRelay, FunctionalInterface, OutboxDispatcher, OutboxStore, OutboxRelay

### Community 49 - "run.sh"
Cohesion: 0.22
Nodes (14): await_http(), await_log(), cleanup(), die(), log(), LOG_DIR, REPO_ROOT, require() (+6 more)

### Community 50 - "ActivityLog"
Cohesion: 0.15
Nodes (7): ActivityLog, AuditLevel, ERROR, INFO, WARN, RecordingLogPublisher, Override

### Community 51 - "ProductSalesRepository"
Cohesion: 0.26
Nodes (5): ProductId, ProductSalesRepositoryContractTest, Override, Quantity, ProductSalesRepository

### Community 52 - "FraudStreamsConfiguration.java"
Cohesion: 0.47
Nodes (4): FraudStreamsConfiguration, KafkaStreamsConfiguration, org.springframework.kafka.annotation.EnableKafkaStreams, org.springframework.kafka.config.KafkaStreamsConfiguration

### Community 53 - "Anticorruption Layer de tradução na fronteira"
Cohesion: 0.50
Nodes (5): Anticorruption Layer de tradução na fronteira, Contratos de evento duplicados por serviço, LogEntryTranslator, spring.json.add.type.headers=false, OrderPlacedTranslator

### Community 62 - "MetricsQueryService"
Cohesion: 0.21
Nodes (5): MetricsQueryPort, MetricsQueryService, AnalyticsWiring, Override, MetricsQueryFacade

### Community 64 - "OutboxRecord"
Cohesion: 0.09
Nodes (12): Override, KafkaOutboxDispatcher, OutboxRelay, FunctionalInterface, OutboxDispatcher, OutboxRecord, OutboxStore, FakeStore (+4 more)

### Community 65 - "ProcessOrderPaymentTest"
Cohesion: 0.24
Nodes (5): Override, RecordingGateway, ProcessOrderPaymentTest, RecordingGateway, GatewayResult

### Community 66 - "com.fasterxml.jackson.databind.ObjectMapper"
Cohesion: 0.21
Nodes (5): DuckDbAuditRepository, Override, com.fasterxml.jackson.databind.ObjectMapper, java.sql.ResultSet, OrderEventOutboxTranslator

### Community 67 - "DeadLetterProperties"
Cohesion: 0.16
Nodes (3): DeadLetterProperties, DefaultErrorHandler, KafkaErrorHandlingConfig

### Community 68 - "DeadLetterProperties"
Cohesion: 0.09
Nodes (4): DeadLetterProperties, Retry, DefaultErrorHandler, KafkaErrorHandlingConfig

### Community 69 - "JsonlFileAuditRepository"
Cohesion: 0.33
Nodes (4): ObjectMapper, Override, JsonlFileAuditRepository, StoredLine

### Community 70 - "org.springframework.kafka.core.KafkaTemplate"
Cohesion: 0.24
Nodes (5): KafkaActivityLogPublisher, KafkaOutboxDispatcher, ActivityLogPublisher, ActivityLogFacade, org.springframework.kafka.core.KafkaTemplate

### Community 71 - ".handle"
Cohesion: 0.28
Nodes (4): PlaceOrderCommand, Override, PlaceOrderService, PlaceOrderServiceTest

### Community 72 - "Order"
Cohesion: 0.21
Nodes (3): Override, Order, WhenValid

### Community 74 - "ApplicationName"
Cohesion: 0.30
Nodes (3): ApplicationName, Override, AuditFilterTest

### Community 76 - ".place"
Cohesion: 0.20
Nodes (6): FakeRepository, Override, CustomerId, Money, ProductId, Quantity

### Community 77 - "OutboxRecord"
Cohesion: 0.22
Nodes (4): Override, OutboxRecord, FakeStore, Override

### Community 78 - "Money"
Cohesion: 0.15
Nodes (6): OrderPlaced, Quantity, OrderPlacedTranslator, InvalidValueException, Override, Money

### Community 79 - "OrderId"
Cohesion: 0.10
Nodes (11): Override, RecordingRepository, DomainEvent, InvalidOrderTransitionException, Override, OrderId, OrderStatus, CANCELLED (+3 more)

### Community 82 - "InvalidAuditException"
Cohesion: 0.23
Nodes (3): AuditEventMessage, AuditEventTranslator, InvalidAuditException

### Community 88 - ".translate"
Cohesion: 0.29
Nodes (3): OutboxRecord, Override, OrderPlacedMessage

### Community 90 - "FraudEventConsumerConfig.java"
Cohesion: 0.50
Nodes (3): ConcurrentKafkaListenerContainerFactory, org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory, FraudEventConsumerConfig

## Knowledge Gaps
- **78 isolated node(s):** `audit-service-adapters`, `audit-service-application`, `audit-service-bootstrap`, `audit-service-domain`, `DEBUG` (+73 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **40 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `AuditEvent` connect `AuditEvent` to `OrderServiceWiring.java`, `com.fasterxml.jackson.databind.ObjectMapper`, `AuditFilter`, `JsonlFileAuditRepository`, `ApplicationName`, `InvalidAuditException`, `AuditServiceWiring.java`, `AuditRepository`?**
  _High betweenness centrality (0.041) - this node is a cross-community bridge._
- **Why does `ProductId` connect `ProductId` to `OrderServiceWiring.java`, `ProductSalesRecord`, `OrderRecord`, `OrderLedgerRepository`, `Money`, `ProductSalesRepository`, `FakeProductSales`, `MetricsQueryService`?**
  _High betweenness centrality (0.030) - this node is a cross-community bridge._
- **Why does `SqliteOrderRepository` connect `SqliteOrderRepository` to `OrderServiceWiring.java`, `org.springframework.context.annotation.Bean`, `Order`, `OrderRepository`, `OrderId`, `OutboxStore`?**
  _High betweenness centrality (0.028) - this node is a cross-community bridge._
- **What connects `audit-service-adapters`, `audit-service-application`, `audit-service-bootstrap` to the rest of the system?**
  _78 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `OrderServiceWiring.java` be split into smaller, more focused modules?**
  _Cohesion score 0.06848357791754019 - nodes in this community are weakly interconnected._
- **Should `ProductSalesRecord` be split into smaller, more focused modules?**
  _Cohesion score 0.09225589225589226 - nodes in this community are weakly interconnected._
- **Should `ProductId` be split into smaller, more focused modules?**
  _Cohesion score 0.09647058823529411 - nodes in this community are weakly interconnected._