# Graph Report - training_kafka  (2026-09-03)

## Corpus Check
- 240 files · ~45,622 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1496 nodes · 4003 edges · 100 communities (61 shown, 39 thin omitted)
- Extraction: 88% EXTRACTED · 12% INFERRED · 0% AMBIGUOUS · INFERRED: 463 edges (avg confidence: 0.81)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `1b344b12`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- AuditController.java
- ProductSalesRecord
- .request
- ProductId
- DeadLetterProperties
- FraudDetectedMessage
- metrics-consumer (App B)
- OrderLedgerRepository
- java.sql.Connection
- Violation
- Order
- Money
- FraudTopology.java
- OrderServiceWiring
- orders-load.js
- package.json
- org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
- org.springframework.boot.autoconfigure.SpringBootApplication
- AuditEvent
- StdoutAuditRepository
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
- OutboxRelayScheduler
- analytics/adapters/messaging/AuditEventMessage.java
- audit-service-application
- org.springframework.context.annotation.Bean
- Topics
- audit-service-bootstrap
- audit-service-domain
- ApplyPaymentResult
- PaymentRepository
- OutboxStore
- run.sh
- PaymentWiring.java
- ProductSalesRepository
- FraudStreamsConfiguration.java
- Anticorruption Layer de tradução na fronteira
- fraud-service
- metrics-consumer-bootstrap
- order-service-bootstrap
- fraud-service-adapters
- fraud-service-application
- fraud-service-bootstrap
- org.springframework.stereotype.Component
- fraud-service-domain
- .handle
- DeadLetterProperties
- OutboxRecord
- org.junit.jupiter.api.Test
- DuckDbAuditRepository
- DeadLetterProperties
- DeadLetterProperties
- JsonlFileAuditRepository
- KafkaActivityLogPublisher
- OrderRepository
- com.fasterxml.jackson.databind.ObjectMapper
- org.junit.jupiter.api.BeforeEach
- ApplicationName
- TestRepositories
- org.junit.jupiter.api.DisplayName
- ActivityLogPublisher
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
- OrderEventOutboxTranslator
- OutboxRecord
- FraudEventConsumerConfig.java
- OrderRecord
- ActivityLog
- Retry
- PaymentEventOutboxTranslator
- KafkaActivityLogPublisher
- PlaceOrderUseCase
- AuditLevel
- FakeProductSales
- GatewayResult

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

## Communities (100 total, 39 thin omitted)

### Community 0 - "AuditController.java"
Cohesion: 0.33
Nodes (6): AuditController, AuditEventView, OrderController, org.springframework.web.bind.annotation.GetMapping, org.springframework.web.bind.annotation.RequestMapping, org.springframework.web.bind.annotation.RestController

### Community 1 - "ProductSalesRecord"
Cohesion: 0.10
Nodes (13): Override, ProductId, InMemoryProductSalesRepository, Override, Money, Override, ProductId, Override (+5 more)

### Community 2 - ".request"
Cohesion: 0.18
Nodes (5): OutboxRecord, Override, SqlitePaymentRepository, SqlitePaymentRepositoryTest, PaymentTest

### Community 3 - "ProductId"
Cohesion: 0.09
Nodes (13): MetricsController, ProductSalesView, RevenueView, MetricsQueryPort, MetricsQueryService, Override, MetricsQueryFacade, Override (+5 more)

### Community 4 - "DeadLetterProperties"
Cohesion: 0.12
Nodes (3): DeadLetterProperties, FraudProperties, org.springframework.boot.context.properties.ConfigurationProperties

### Community 5 - "FraudDetectedMessage"
Cohesion: 0.21
Nodes (6): FraudDetectedMessage, FraudulentOrder, FraudEventListener, FraudEventPort, FraudEventFacade, Override

### Community 6 - "metrics-consumer (App B)"
Cohesion: 0.08
Nodes (38): KAFKA_AUTO_CREATE_TOPICS_ENABLE=false, Listeners PLAINTEXT (9092) e PLAINTEXT_HOST (9094), Broker Kafka (apache/kafka:4.1.2), Kafka UI (kafbat/kafka-ui:8090), KRaft: nó único broker+controller, Fatores de replicação 1 para nó único, Tópico Kafka "application-logs", Gargalo: commit por mensagem no consumidor (+30 more)

### Community 8 - "java.sql.Connection"
Cohesion: 0.15
Nodes (11): AuditPersistenceConfiguration, DuckDbPersistence, JsonlPersistence, StdoutPersistence, java.sql.Connection, DuckDbPersistence, InMemoryPersistence, PersistenceConfiguration (+3 more)

### Community 9 - "Violation"
Cohesion: 0.08
Nodes (18): AuditExceptionHandler, java.time.format.DateTimeParseException, OrderPlacedMessage, OrderPlaced, Quantity, OrderPlacedTranslator, AnalyticsExceptionHandler, InvalidValueException (+10 more)

### Community 10 - "Order"
Cohesion: 0.16
Nodes (5): Override, Order, OrderTest, WhenValid, org.junit.jupiter.api.Nested

### Community 11 - "Money"
Cohesion: 0.12
Nodes (9): OrderPlaced, CustomerId, Override, Override, Money, Override, ProductId, Override (+1 more)

### Community 12 - "FraudTopology.java"
Cohesion: 0.05
Nodes (36): AuditEventMessage, FraudDetectedMessage, FraudulentOrder, OrderPlacedMessage, Topics, CustomerFraudState, Override, OccurredAtTimestampExtractor (+28 more)

### Community 14 - "orders-load.js"
Cohesion: 0.09
Nodes (28): acceptanceRate, APPROVAL_LIMIT, awaitStatus(), CATALOG, COMPENSATION_TIMEOUT_MS, compensationsObserved, FRAUD_MAX_ORDERS, options (+20 more)

### Community 15 - "package.json"
Cohesion: 0.14
Nodes (13): esbuild, @faker-js/faker, description, devDependencies, esbuild, @faker-js/faker, name, private (+5 more)

### Community 16 - "org.springframework.boot.autoconfigure.condition.ConditionalOnProperty"
Cohesion: 0.18
Nodes (12): KafkaErrorHandlingConfig, KafkaErrorHandlingConfig, Topics, org.slf4j.Logger, org.springframework.boot.autoconfigure.condition.ConditionalOnProperty, org.springframework.boot.autoconfigure.kafka.KafkaProperties, org.springframework.boot.context.properties.EnableConfigurationProperties, org.springframework.kafka.core.KafkaOperations (+4 more)

### Community 17 - "org.springframework.boot.autoconfigure.SpringBootApplication"
Cohesion: 0.18
Nodes (7): AuditServiceBootstrap, FraudServiceBootstrap, MetricsConsumerBootstrap, OrderServiceBootstrap, org.springframework.boot.autoconfigure.SpringBootApplication, org.springframework.scheduling.annotation.EnableScheduling, PaymentServiceBootstrap

### Community 18 - "AuditEvent"
Cohesion: 0.11
Nodes (11): AuditEventListener, IngestAuditPort, AuditQueryPort, AuditQueryService, IngestAuditService, AuditServiceWiring, AuditQueryFacade, Override (+3 more)

### Community 19 - "StdoutAuditRepository"
Cohesion: 0.17
Nodes (4): Override, StdoutAuditRepository, StdoutAuditRepositoryTest, org.junit.jupiter.api.AfterEach

### Community 22 - "Payment"
Cohesion: 0.11
Nodes (8): FunctionalInterface, LogFactory, FakeRepository, FakeRepository, Override, Payment, Override, PaymentId

### Community 23 - "DomainEvent"
Cohesion: 0.10
Nodes (14): OutboxTranslator, AuditLevel, ERROR, INFO, WARN, DomainEvent, PaymentApproved, PaymentCancelled (+6 more)

### Community 24 - "OrderServiceWiring.java"
Cohesion: 0.22
Nodes (5): FindOrderPort, OrderResponse, FindOrderUseCase, FindOrderFacade, Override

### Community 25 - "AuditRepository"
Cohesion: 0.42
Nodes (5): AuditRepositoryContractTest, AuditLevel, AuditRepository, org.junit.jupiter.params.ParameterizedTest, org.junit.jupiter.params.provider.MethodSource

### Community 34 - "SqliteOrderRepository"
Cohesion: 0.27
Nodes (4): OutboxRecord, Override, SqliteOrderRepository, SqliteOrderRepositoryTest

### Community 36 - "OrderPlaced"
Cohesion: 0.08
Nodes (14): OrderListener, OrderPlacedPort, OrderPlacedHandler, OrderPlaced, ProductId, OrderPlacedHandlerTest, AnalyticsWiring, Override (+6 more)

### Community 39 - "OutboxRelayScheduler"
Cohesion: 0.28
Nodes (3): OutboxRelayScheduler, org.springframework.scheduling.annotation.Scheduled, OutboxRelayScheduler

### Community 42 - "org.springframework.context.annotation.Bean"
Cohesion: 0.23
Nodes (4): KafkaTopicsConfig, Topics, org.apache.kafka.clients.admin.NewTopic, org.springframework.context.annotation.Bean

### Community 46 - "ApplyPaymentResult"
Cohesion: 0.12
Nodes (8): PaymentEventListener, PaymentEventMessage, PaymentEventPort, UnknownPaymentEventException, ApplyPaymentResult, ApplyPaymentResultTest, Override, PaymentEventFacade

### Community 47 - "PaymentRepository"
Cohesion: 0.13
Nodes (7): DeterministicPaymentGateway, DeterministicPaymentGatewayTest, ProcessOrderPayment, Override, OrderPlacedFacade, PaymentGateway, PaymentRepository

### Community 48 - "OutboxStore"
Cohesion: 0.17
Nodes (5): OutboxRelay, FunctionalInterface, OutboxDispatcher, OutboxStore, OutboxRelay

### Community 49 - "run.sh"
Cohesion: 0.22
Nodes (14): await_http(), await_log(), cleanup(), die(), log(), LOG_DIR, REPO_ROOT, require() (+6 more)

### Community 50 - "PaymentWiring.java"
Cohesion: 0.26
Nodes (5): OutboxRelay, FunctionalInterface, OutboxDispatcher, OutboxStore, OutboxRelay

### Community 51 - "ProductSalesRepository"
Cohesion: 0.28
Nodes (5): ProductId, ProductSalesRepositoryContractTest, Override, Quantity, ProductSalesRepository

### Community 52 - "FraudStreamsConfiguration.java"
Cohesion: 0.47
Nodes (4): FraudStreamsConfiguration, KafkaStreamsConfiguration, org.springframework.kafka.annotation.EnableKafkaStreams, org.springframework.kafka.config.KafkaStreamsConfiguration

### Community 53 - "Anticorruption Layer de tradução na fronteira"
Cohesion: 0.50
Nodes (5): Anticorruption Layer de tradução na fronteira, Contratos de evento duplicados por serviço, LogEntryTranslator, spring.json.add.type.headers=false, OrderPlacedTranslator

### Community 60 - "org.springframework.stereotype.Component"
Cohesion: 0.19
Nodes (5): org.springframework.kafka.annotation.KafkaListener, org.springframework.stereotype.Component, OrderPlacedListener, OrderPlacedMessage, OrderPlacedPort

### Community 62 - ".handle"
Cohesion: 0.30
Nodes (4): FraudulentOrder, CompensateFraudulentOrdersTest, FraudulentOrder, RecordingGateway

### Community 64 - "OutboxRecord"
Cohesion: 0.16
Nodes (6): Override, OutboxRecord, FakeStore, OutboxRecord, Override, OutboxRelayTest

### Community 65 - "org.junit.jupiter.api.Test"
Cohesion: 0.17
Nodes (6): KafkaErrorHandlingConfigTest, KafkaErrorHandlingConfigTest, org.junit.jupiter.api.Test, org.springframework.boot.test.context.runner.ApplicationContextRunner, ProcessOrderPaymentTest, RecordingGateway

### Community 67 - "DeadLetterProperties"
Cohesion: 0.10
Nodes (4): DeadLetterProperties, Retry, DefaultErrorHandler, KafkaErrorHandlingConfig

### Community 68 - "DeadLetterProperties"
Cohesion: 0.16
Nodes (3): DeadLetterProperties, DefaultErrorHandler, KafkaErrorHandlingConfig

### Community 69 - "JsonlFileAuditRepository"
Cohesion: 0.33
Nodes (4): ObjectMapper, Override, JsonlFileAuditRepository, StoredLine

### Community 70 - "KafkaActivityLogPublisher"
Cohesion: 0.18
Nodes (5): AuditEventMessage, KafkaActivityLogPublisher, KafkaActivityLogPublisher, ActivityLogFacade, Override

### Community 71 - "OrderRepository"
Cohesion: 0.10
Nodes (12): PlaceOrderService, ActivityLog, ActivityLogPublisher, AuditLevel, ERROR, INFO, WARN, Override (+4 more)

### Community 72 - "com.fasterxml.jackson.databind.ObjectMapper"
Cohesion: 0.32
Nodes (6): com.fasterxml.jackson.databind.ObjectMapper, Override, KafkaOutboxDispatcher, org.springframework.kafka.core.KafkaTemplate, KafkaOutboxDispatcher, AuditEventMessageContractTest

### Community 73 - "org.junit.jupiter.api.BeforeEach"
Cohesion: 0.24
Nodes (6): InvalidOrderTransitionException, OrderStatus, CANCELLED, PAID, PENDING_PAYMENT, org.junit.jupiter.api.BeforeEach

### Community 74 - "ApplicationName"
Cohesion: 0.24
Nodes (3): ApplicationName, Override, AuditFilterTest

### Community 75 - "TestRepositories"
Cohesion: 0.19
Nodes (3): Connection, TestRepositories, org.junit.jupiter.params.provider.Arguments

### Community 76 - "org.junit.jupiter.api.DisplayName"
Cohesion: 0.14
Nodes (10): PlaceOrderCommand, Override, PlaceOrderServiceTest, CustomerId, Money, ProductId, Quantity, MoneyRules (+2 more)

### Community 77 - "ActivityLogPublisher"
Cohesion: 0.19
Nodes (3): CompensateFraudulentOrders, ActivityLogPublisher, PaymentWiring

### Community 79 - "OrderId"
Cohesion: 0.08
Nodes (11): FindOrderService, Override, OrderView, FakeRepository, Override, FakeRepository, FindOrderServiceTest, Override (+3 more)

### Community 82 - "InvalidAuditException"
Cohesion: 0.25
Nodes (3): AuditEventMessage, AuditEventTranslator, InvalidAuditException

### Community 88 - "OrderEventOutboxTranslator"
Cohesion: 0.25
Nodes (4): OutboxRecord, Override, OrderEventOutboxTranslator, OrderPlacedMessage

### Community 89 - "OutboxRecord"
Cohesion: 0.23
Nodes (5): OutboxRecord, FakeStore, OutboxRecord, Override, OutboxRelayTest

### Community 90 - "FraudEventConsumerConfig.java"
Cohesion: 0.50
Nodes (3): ConcurrentKafkaListenerContainerFactory, org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory, FraudEventConsumerConfig

### Community 91 - "OrderRecord"
Cohesion: 0.13
Nodes (12): java.sql.PreparedStatement, java.sql.ResultSet, DuckDbOrderLedgerRepository, Override, DuckDbProductSalesRepository, InMemoryOrderLedgerRepository, Override, MoneyCents (+4 more)

### Community 92 - "ActivityLog"
Cohesion: 0.19
Nodes (5): ActivityLog, Override, RecordingLogPublisher, RecordingLogPublisher, Override

### Community 94 - "PaymentEventOutboxTranslator"
Cohesion: 0.24
Nodes (5): PaymentEventMessage, OutboxRecord, Override, PaymentEventMessage, PaymentEventOutboxTranslator

### Community 95 - "KafkaActivityLogPublisher"
Cohesion: 0.20
Nodes (4): AuditEventMessage, KafkaActivityLogPublisher, KafkaActivityLogPublisher, ActivityLogFacade

### Community 96 - "PlaceOrderUseCase"
Cohesion: 0.16
Nodes (6): PlaceOrderPort, PlaceOrderRequest, PlaceOrderResponse, PlaceOrderUseCase, PlaceOrderFacade, org.springframework.web.bind.annotation.PostMapping

### Community 97 - "AuditLevel"
Cohesion: 0.24
Nodes (7): AuditLevel, DEBUG, ERROR, INFO, WARN, isAtLeast(), parse()

### Community 98 - "FakeProductSales"
Cohesion: 0.36
Nodes (3): FakeLedger, FakeProductSales, Override

## Knowledge Gaps
- **81 isolated node(s):** `audit-service-adapters`, `audit-service-application`, `audit-service-bootstrap`, `audit-service-domain`, `DEBUG` (+76 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **39 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `AuditEvent` connect `AuditEvent` to `AuditController.java`, `AuditLevel`, `DuckDbAuditRepository`, `AuditFilter`, `JsonlFileAuditRepository`, `ApplicationName`, `InvalidAuditException`, `StdoutAuditRepository`, `AuditRepository`?**
  _High betweenness centrality (0.040) - this node is a cross-community bridge._
- **Why does `Payment` connect `Payment` to `.request`, `GatewayResult`, `PaymentRepository`, `DomainEvent`, `.handle`?**
  _High betweenness centrality (0.037) - this node is a cross-community bridge._
- **Why does `SqliteOrderRepository` connect `SqliteOrderRepository` to `OrderRepository`, `java.sql.Connection`, `Order`, `Money`, `OrderServiceWiring`, `OutboxStore`, `OrderServiceWiring.java`?**
  _High betweenness centrality (0.028) - this node is a cross-community bridge._
- **What connects `audit-service-adapters`, `audit-service-application`, `audit-service-bootstrap` to the rest of the system?**
  _81 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `ProductSalesRecord` be split into smaller, more focused modules?**
  _Cohesion score 0.09954751131221719 - nodes in this community are weakly interconnected._
- **Should `ProductId` be split into smaller, more focused modules?**
  _Cohesion score 0.08928571428571429 - nodes in this community are weakly interconnected._
- **Should `DeadLetterProperties` be split into smaller, more focused modules?**
  _Cohesion score 0.11695906432748537 - nodes in this community are weakly interconnected._