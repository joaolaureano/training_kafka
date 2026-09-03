# Graph Report - training_kafka  (2026-09-03)

## Corpus Check
- 233 files · ~43,797 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1470 nodes · 3857 edges · 98 communities (56 shown, 42 thin omitted)
- Extraction: 88% EXTRACTED · 12% INFERRED · 0% AMBIGUOUS · INFERRED: 450 edges (avg confidence: 0.81)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `a267fbe3`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- MetricsController
- Units
- SqlitePaymentRepository
- ProductId
- DeadLetterProperties
- PaymentRepository
- metrics-consumer (App B)
- .orderAt
- java.sql.Connection
- Violation
- org.junit.jupiter.api.Test
- Order
- FraudTopology.java
- OrderServiceWiring.java
- orders-load.js
- package.json
- org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
- org.springframework.boot.autoconfigure.SpringBootApplication
- AuditServiceWiring.java
- org.junit.jupiter.api.BeforeEach
- SqliteRepositoryException
- dev.joaolaureano.trainingkafka:training-kafka
- Payment
- DomainEvent
- dev.joaolaureano.trainingkafka.orders.domain.port.OrderRepository
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
- org.springframework.context.annotation.Bean
- Topics
- audit-service-bootstrap
- audit-service-domain
- ApplyPaymentResult
- PaymentWiring.java
- OutboxRecord
- run.sh
- OutboxStore
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
- DuckDbAuditRepository
- DeadLetterProperties
- DeadLetterProperties
- AuditEvent
- KafkaActivityLogPublisher
- .handle
- com.fasterxml.jackson.databind.ObjectMapper
- FindOrderServiceTest.java
- ApplicationName
- TestRepositories
- .place
- FindOrderPort
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
- OrderEventOutboxTranslator.java
- FakeStore
- FraudEventConsumerConfig.java
- ProductSalesRecord
- Topics
- Retry
- PaymentEventOutboxTranslator
- .applyPaymentResult
- PlaceOrderResponse
- OutboxRelay

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
10. `AuditFilter` - 30 edges

## Surprising Connections (you probably didn't know these)
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` --conceptually_related_to--> `order-service (App A)`  [INFERRED]
  docker-compose.yml → README.md
- `training_kafka (laboratório Kafka + DDD)` --references--> `Kafka UI (kafbat/kafka-ui:8090)`  [EXTRACTED]
  README.md → docker-compose.yml
- `training_kafka (laboratório Kafka + DDD)` --references--> `Broker Kafka (apache/kafka:4.1.2)`  [EXTRACTED]
  README.md → docker-compose.yml
- `FindOrderFacade` --implements--> `FindOrderPort`  [EXTRACTED]
  order-service/order-service-bootstrap/src/main/java/dev/joaolaureano/trainingkafka/orders/bootstrap/facade/FindOrderFacade.java → order-service/order-service-adapters/src/main/java/dev/joaolaureano/trainingkafka/orders/adapters/web/FindOrderPort.java
- `FindOrderServiceTest` --references--> `FindOrderService`  [EXTRACTED]
  order-service/order-service-application/src/test/java/dev/joaolaureano/trainingkafka/orders/application/FindOrderServiceTest.java → order-service/order-service-application/src/main/java/dev/joaolaureano/trainingkafka/orders/application/FindOrderService.java

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Pipeline de eventos A → orders → B → application-logs → C** — readme_order_service, readme_orders_topic, readme_metrics_consumer, readme_application_logs_topic, readme_log_aggregator, docker_compose_kafka_broker [EXTRACTED 1.00]
- **Modelo de domínio do App B (3 raízes + VO derivado + handler)** — readme_productsalesrecord, readme_customerorderpattern, readme_orderrecord, readme_revenuewindow, readme_orderplacedhandler [EXTRACTED 1.00]

## Communities (98 total, 42 thin omitted)

### Community 0 - "MetricsController"
Cohesion: 0.17
Nodes (9): MetricsController, ProductSalesView, RevenueView, org.springframework.web.bind.annotation.GetMapping, org.springframework.web.bind.annotation.PostMapping, org.springframework.web.bind.annotation.RequestMapping, org.springframework.web.bind.annotation.RestController, PlaceOrderRequest (+1 more)

### Community 1 - "Units"
Cohesion: 0.11
Nodes (12): java.sql.PreparedStatement, DuckDbProductSalesRepository, Override, ProductId, Money, MoneyCents, Override, ProductId (+4 more)

### Community 2 - "SqlitePaymentRepository"
Cohesion: 0.21
Nodes (5): OutboxRecord, Override, SqlitePaymentRepository, SqlitePaymentRepositoryTest, PaymentTest

### Community 3 - "ProductId"
Cohesion: 0.09
Nodes (16): java.sql.ResultSet, DuckDbOrderLedgerRepository, Override, InMemoryOrderLedgerRepository, Override, Override, SqliteOrderLedgerRepository, FakeLedger (+8 more)

### Community 4 - "DeadLetterProperties"
Cohesion: 0.07
Nodes (4): DeadLetterProperties, FraudProperties, DeadLetterProperties, org.springframework.boot.context.properties.ConfigurationProperties

### Community 5 - "PaymentRepository"
Cohesion: 0.11
Nodes (11): FraudDetectedMessage, FraudulentOrder, FraudEventListener, FraudEventPort, CompensateFraudulentOrders, FraudulentOrder, CompensateFraudulentOrdersTest, FraudulentOrder (+3 more)

### Community 6 - "metrics-consumer (App B)"
Cohesion: 0.08
Nodes (38): KAFKA_AUTO_CREATE_TOPICS_ENABLE=false, Listeners PLAINTEXT (9092) e PLAINTEXT_HOST (9094), Broker Kafka (apache/kafka:4.1.2), Kafka UI (kafbat/kafka-ui:8090), KRaft: nó único broker+controller, Fatores de replicação 1 para nó único, Tópico Kafka "application-logs", Gargalo: commit por mensagem no consumidor (+30 more)

### Community 8 - "java.sql.Connection"
Cohesion: 0.13
Nodes (11): AuditPersistenceConfiguration, DuckDbPersistence, JsonlPersistence, StdoutPersistence, java.sql.Connection, DuckDbPersistence, InMemoryPersistence, PersistenceConfiguration (+3 more)

### Community 9 - "Violation"
Cohesion: 0.12
Nodes (13): AuditExceptionHandler, java.time.format.DateTimeParseException, AnalyticsExceptionHandler, ApiError, ApiExceptionHandler, InvalidOrderException, Override, Violation (+5 more)

### Community 10 - "org.junit.jupiter.api.Test"
Cohesion: 0.07
Nodes (14): KafkaErrorHandlingConfigTest, KafkaErrorHandlingConfigTest, TimeRange, RevenueWindowTest, Override, OrderView, MoneyRules, OrderTest (+6 more)

### Community 11 - "Order"
Cohesion: 0.09
Nodes (15): OrderPlaced, CustomerId, Override, Override, Money, Override, Order, OrderStatus (+7 more)

### Community 12 - "FraudTopology.java"
Cohesion: 0.05
Nodes (36): AuditEventMessage, FraudDetectedMessage, FraudulentOrder, OrderPlacedMessage, Topics, CustomerFraudState, Override, OccurredAtTimestampExtractor (+28 more)

### Community 13 - "OrderServiceWiring.java"
Cohesion: 0.13
Nodes (13): dev.joaolaureano.trainingkafka.orders.adapters.messaging.KafkaActivityLogPublisher, dev.joaolaureano.trainingkafka.orders.adapters.messaging.OutboxRelay, dev.joaolaureano.trainingkafka.orders.adapters.persistence.OutboxDispatcher, dev.joaolaureano.trainingkafka.orders.adapters.persistence.OutboxStore, dev.joaolaureano.trainingkafka.orders.adapters.persistence.OutboxTranslator, dev.joaolaureano.trainingkafka.orders.adapters.persistence.SqliteOrderRepository, dev.joaolaureano.trainingkafka.orders.adapters.web.PlaceOrderPort, dev.joaolaureano.trainingkafka.orders.application.PlaceOrderUseCase (+5 more)

### Community 14 - "orders-load.js"
Cohesion: 0.09
Nodes (28): acceptanceRate, APPROVAL_LIMIT, awaitStatus(), CATALOG, COMPENSATION_TIMEOUT_MS, compensationsObserved, FRAUD_MAX_ORDERS, options (+20 more)

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
Nodes (8): IngestAuditPort, AuditController, AuditQueryPort, AuditQueryService, IngestAuditService, AuditServiceWiring, AuditQueryFacade, IngestAuditFacade

### Community 19 - "org.junit.jupiter.api.BeforeEach"
Cohesion: 0.14
Nodes (6): ObjectMapper, Override, StdoutAuditRepository, StdoutAuditRepositoryTest, org.junit.jupiter.api.AfterEach, org.junit.jupiter.api.BeforeEach

### Community 22 - "Payment"
Cohesion: 0.14
Nodes (6): FakeRepository, Override, FakeRepository, Override, Payment, PaymentId

### Community 23 - "DomainEvent"
Cohesion: 0.09
Nodes (11): OutboxTranslator, DomainEvent, PaymentApproved, PaymentCancelled, PaymentFailed, Override, PaymentStatus, APPROVED (+3 more)

### Community 24 - "dev.joaolaureano.trainingkafka.orders.domain.port.OrderRepository"
Cohesion: 0.26
Nodes (5): dev.joaolaureano.trainingkafka.orders.domain.port.OrderRepository, FindOrderService, FindOrderUseCase, FindOrderFacade, Override

### Community 25 - "AuditRepository"
Cohesion: 0.33
Nodes (6): AuditRepositoryContractTest, AuditLevel, Connection, AuditRepository, org.junit.jupiter.params.ParameterizedTest, org.junit.jupiter.params.provider.MethodSource

### Community 34 - "SqliteOrderRepository"
Cohesion: 0.34
Nodes (4): OutboxRecord, Override, SqliteOrderRepository, SqliteOrderRepositoryTest

### Community 35 - "AuditFilter"
Cohesion: 0.11
Nodes (9): AuditFilter, AuditLevel, DEBUG, ERROR, INFO, WARN, isAtLeast(), parse() (+1 more)

### Community 36 - "OrderPlaced"
Cohesion: 0.12
Nodes (12): OrderPlacedPort, OrderPlacedHandler, OrderPlaced, ProductId, OrderPlacedHandlerTest, Override, OrderPlacedFacade, OrderPlaced (+4 more)

### Community 39 - "org.springframework.stereotype.Component"
Cohesion: 0.06
Nodes (17): AuditEventListener, AuditEventMessage, OrderListener, OrderPlacedMessage, OrderPlaced, Quantity, OrderPlacedTranslator, InvalidValueException (+9 more)

### Community 42 - "org.springframework.context.annotation.Bean"
Cohesion: 0.23
Nodes (4): KafkaTopicsConfig, Topics, org.apache.kafka.clients.admin.NewTopic, org.springframework.context.annotation.Bean

### Community 46 - "ApplyPaymentResult"
Cohesion: 0.23
Nodes (4): ApplyPaymentResult, ApplyPaymentResultTest, Override, PaymentEventFacade

### Community 47 - "PaymentWiring.java"
Cohesion: 0.10
Nodes (10): DeterministicPaymentGateway, OrderPlacedListener, OrderPlacedMessage, OrderPlacedPort, DeterministicPaymentGatewayTest, ProcessOrderPayment, PaymentWiring, Override (+2 more)

### Community 48 - "OutboxRecord"
Cohesion: 0.15
Nodes (6): Override, OutboxRelay, FunctionalInterface, OutboxDispatcher, OutboxRecord, OutboxStore

### Community 49 - "run.sh"
Cohesion: 0.22
Nodes (14): await_http(), await_log(), cleanup(), die(), log(), LOG_DIR, REPO_ROOT, require() (+6 more)

### Community 50 - "OutboxStore"
Cohesion: 0.26
Nodes (5): OutboxRelay, FunctionalInterface, OutboxDispatcher, OutboxStore, OutboxRelay

### Community 51 - "ProductSalesRepository"
Cohesion: 0.24
Nodes (6): ProductId, ProductSalesRepositoryContractTest, Override, Quantity, ProductSalesRepository, ProductSalesRecordTest

### Community 52 - "FraudStreamsConfiguration.java"
Cohesion: 0.47
Nodes (4): FraudStreamsConfiguration, KafkaStreamsConfiguration, org.springframework.kafka.annotation.EnableKafkaStreams, org.springframework.kafka.config.KafkaStreamsConfiguration

### Community 53 - "Anticorruption Layer de tradução na fronteira"
Cohesion: 0.50
Nodes (5): Anticorruption Layer de tradução na fronteira, Contratos de evento duplicados por serviço, LogEntryTranslator, spring.json.add.type.headers=false, OrderPlacedTranslator

### Community 62 - "MetricsQueryService"
Cohesion: 0.18
Nodes (5): MetricsQueryPort, MetricsQueryService, AnalyticsWiring, Override, MetricsQueryFacade

### Community 64 - "OutboxRecord"
Cohesion: 0.17
Nodes (6): Override, OutboxRecord, FakeStore, OutboxRecord, Override, OutboxRelayTest

### Community 65 - "ProcessOrderPaymentTest"
Cohesion: 0.24
Nodes (5): Override, RecordingGateway, ProcessOrderPaymentTest, RecordingGateway, GatewayResult

### Community 67 - "DeadLetterProperties"
Cohesion: 0.10
Nodes (4): DeadLetterProperties, Retry, DefaultErrorHandler, KafkaErrorHandlingConfig

### Community 68 - "DeadLetterProperties"
Cohesion: 0.16
Nodes (3): DeadLetterProperties, DefaultErrorHandler, KafkaErrorHandlingConfig

### Community 69 - "AuditEvent"
Cohesion: 0.18
Nodes (7): Override, JsonlFileAuditRepository, StoredLine, AuditEventView, Override, Override, AuditEvent

### Community 70 - "KafkaActivityLogPublisher"
Cohesion: 0.21
Nodes (4): AuditEventMessage, KafkaActivityLogPublisher, ActivityLogFacade, Override

### Community 71 - ".handle"
Cohesion: 0.13
Nodes (11): PlaceOrderCommand, Override, PlaceOrderService, ActivityLog, ActivityLogPublisher, AuditLevel, ERROR, INFO (+3 more)

### Community 72 - "com.fasterxml.jackson.databind.ObjectMapper"
Cohesion: 0.44
Nodes (4): com.fasterxml.jackson.databind.ObjectMapper, KafkaOutboxDispatcher, org.springframework.kafka.core.KafkaTemplate, KafkaOutboxDispatcher

### Community 73 - "FindOrderServiceTest.java"
Cohesion: 0.40
Nodes (6): dev.joaolaureano.trainingkafka.orders.domain.event.DomainEvent, dev.joaolaureano.trainingkafka.orders.domain.model.Order, dev.joaolaureano.trainingkafka.orders.domain.model.OrderId, FakeRepository, FindOrderServiceTest, Override

### Community 74 - "ApplicationName"
Cohesion: 0.30
Nodes (3): ApplicationName, Override, AuditFilterTest

### Community 76 - ".place"
Cohesion: 0.29
Nodes (4): CustomerId, Money, ProductId, Quantity

### Community 77 - "FindOrderPort"
Cohesion: 0.31
Nodes (4): FindOrderPort, PlaceOrderPort, OrderController, OrderResponse

### Community 79 - "OrderId"
Cohesion: 0.08
Nodes (14): PlaceOrderPort, PlaceOrderRequest, PlaceOrderUseCase, FakeRepository, Override, Override, RecordingRepository, Override (+6 more)

### Community 88 - "OrderEventOutboxTranslator.java"
Cohesion: 0.20
Nodes (5): OutboxRecord, Override, OrderEventOutboxTranslator, OrderPlacedMessage, OutboxTranslator

### Community 89 - "FakeStore"
Cohesion: 0.31
Nodes (4): FakeStore, OutboxRecord, Override, OutboxRelayTest

### Community 90 - "FraudEventConsumerConfig.java"
Cohesion: 0.50
Nodes (3): ConcurrentKafkaListenerContainerFactory, org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory, FraudEventConsumerConfig

### Community 91 - "ProductSalesRecord"
Cohesion: 0.19
Nodes (4): InMemoryProductSalesRepository, Override, Override, ProductSalesRecord

### Community 94 - "PaymentEventOutboxTranslator"
Cohesion: 0.24
Nodes (5): PaymentEventMessage, OutboxRecord, Override, PaymentEventMessage, PaymentEventOutboxTranslator

### Community 95 - ".applyPaymentResult"
Cohesion: 0.40
Nodes (3): ApplyPaymentResult, dev.joaolaureano.trainingkafka.orders.adapters.messaging.PaymentEventPort, dev.joaolaureano.trainingkafka.orders.application.ApplyPaymentResult

## Knowledge Gaps
- **78 isolated node(s):** `name`, `version`, `private`, `description`, `build` (+73 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **42 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `DeadLetterProperties` connect `DeadLetterProperties` to `org.springframework.context.annotation.Bean`, `DeadLetterProperties`, `Retry`?**
  _High betweenness centrality (0.030) - this node is a cross-community bridge._
- **Why does `Payment` connect `Payment` to `ProcessOrderPaymentTest`, `SqlitePaymentRepository`, `PaymentRepository`, `org.junit.jupiter.api.Test`, `PaymentWiring.java`, `DomainEvent`?**
  _High betweenness centrality (0.024) - this node is a cross-community bridge._
- **Why does `AuditEvent` connect `AuditEvent` to `DuckDbAuditRepository`, `AuditFilter`, `org.springframework.stereotype.Component`, `ApplicationName`, `InvalidAuditException`, `AuditServiceWiring.java`, `org.junit.jupiter.api.BeforeEach`, `AuditRepository`?**
  _High betweenness centrality (0.024) - this node is a cross-community bridge._
- **What connects `name`, `version`, `private` to the rest of the system?**
  _78 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Units` be split into smaller, more focused modules?**
  _Cohesion score 0.10793650793650794 - nodes in this community are weakly interconnected._
- **Should `ProductId` be split into smaller, more focused modules?**
  _Cohesion score 0.09288824383164006 - nodes in this community are weakly interconnected._
- **Should `DeadLetterProperties` be split into smaller, more focused modules?**
  _Cohesion score 0.07096774193548387 - nodes in this community are weakly interconnected._