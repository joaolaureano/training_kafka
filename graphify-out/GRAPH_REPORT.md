# Graph Report - training_kafka  (2026-09-03)

## Corpus Check
- 240 files · ~45,622 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1514 nodes · 3993 edges · 102 communities (58 shown, 44 thin omitted)
- Extraction: 88% EXTRACTED · 12% INFERRED · 0% AMBIGUOUS · INFERRED: 463 edges (avg confidence: 0.81)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `1f21a858`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- AuditController.java
- Units
- SqlitePaymentRepository
- ProductId
- DeadLetterProperties
- FraudDetectedMessage
- metrics-consumer (App B)
- MetricsQueryService
- org.springframework.context.annotation.Bean
- Violation
- Order
- Money
- FraudTopology.java
- DomainEvent
- orders-load.js
- package.json
- org.springframework.context.annotation.Configuration
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
- ApplyPaymentResult
- .charge
- OutboxRecord
- run.sh
- OutboxRelay
- ProductSalesRepository
- .request
- Anticorruption Layer de tradução na fronteira
- fraud-service
- metrics-consumer-bootstrap
- order-service-bootstrap
- fraud-service-adapters
- fraud-service-application
- fraud-service-bootstrap
- OrderPlacedPort
- fraud-service-domain
- .handle
- DeadLetterProperties
- OutboxRecord
- dev.joaolaureano.trainingkafka.payment.domain.model.Payment
- DuckDbAuditRepository
- DeadLetterProperties
- DeadLetterProperties
- JsonlFileAuditRepository
- KafkaActivityLogPublisher
- PlaceOrderServiceTest.java
- ActivityLogPublisher
- org.junit.jupiter.api.BeforeEach
- PaymentEventPort
- org.slf4j.Logger
- org.junit.jupiter.api.Test
- PaymentWiring.java
- Money
- Retry
- Retry
- UnknownOrderException
- Retry
- payment-service
- payment-service-adapters
- payment-service-application
- payment-service-bootstrap
- payment-service-domain
- .audit
- AuditLevel
- Topics
- java.sql.Connection
- ActivityLog
- Retry
- PaymentEventOutboxTranslator
- com.fasterxml.jackson.databind.ObjectMapper
- OrderId
- Topics
- ProductSalesRecord
- FunctionalInterface
- FraudulentOrder
- KafkaActivityLogPublisher

## God Nodes (most connected - your core abstractions)
1. `Order` - 55 edges
2. `ProductSalesRecord` - 49 edges
3. `ProductId` - 45 edges
4. `AuditEvent` - 43 edges
5. `Payment` - 39 edges
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
- `CompensateFraudulentOrders` --references--> `ActivityLogPublisher`  [EXTRACTED]
  payment-service/payment-service-application/src/main/java/dev/joaolaureano/trainingkafka/payment/application/CompensateFraudulentOrders.java → payment-service/payment-service-application/src/main/java/dev/joaolaureano/trainingkafka/payment/application/port/ActivityLogPublisher.java
- `CompensateFraudulentOrdersTest` --references--> `CompensateFraudulentOrders`  [EXTRACTED]
  payment-service/payment-service-application/src/test/java/dev/joaolaureano/trainingkafka/payment/application/CompensateFraudulentOrdersTest.java → payment-service/payment-service-application/src/main/java/dev/joaolaureano/trainingkafka/payment/application/CompensateFraudulentOrders.java

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Pipeline de eventos A → orders → B → application-logs → C** — readme_order_service, readme_orders_topic, readme_metrics_consumer, readme_application_logs_topic, readme_log_aggregator, docker_compose_kafka_broker [EXTRACTED 1.00]
- **Modelo de domínio do App B (3 raízes + VO derivado + handler)** — readme_productsalesrecord, readme_customerorderpattern, readme_orderrecord, readme_revenuewindow, readme_orderplacedhandler [EXTRACTED 1.00]

## Communities (102 total, 44 thin omitted)

### Community 0 - "AuditController.java"
Cohesion: 0.15
Nodes (11): AuditController, AuditEventView, MetricsController, ProductSalesView, RevenueView, OrderController, PlaceOrderResponse, org.springframework.web.bind.annotation.GetMapping (+3 more)

### Community 1 - "Units"
Cohesion: 0.14
Nodes (8): Override, ProductId, Money, Override, ProductId, TopSellerPolicy, Override, Units

### Community 2 - "SqlitePaymentRepository"
Cohesion: 0.25
Nodes (4): OutboxRecord, Override, SqlitePaymentRepository, SqlitePaymentRepositoryTest

### Community 3 - "ProductId"
Cohesion: 0.12
Nodes (10): InMemoryOrderLedgerRepository, Override, OrderLedgerRepositoryContractTest, OrderRecord, Override, ProductId, RevenueWindow, TimeRange (+2 more)

### Community 4 - "DeadLetterProperties"
Cohesion: 0.11
Nodes (4): DeadLetterProperties, KafkaErrorHandlingConfig, FraudProperties, org.springframework.boot.context.properties.ConfigurationProperties

### Community 5 - "FraudDetectedMessage"
Cohesion: 0.21
Nodes (5): FraudDetectedMessage, FraudulentOrder, FraudEventListener, FraudEventPort, Override

### Community 6 - "metrics-consumer (App B)"
Cohesion: 0.08
Nodes (38): KAFKA_AUTO_CREATE_TOPICS_ENABLE=false, Listeners PLAINTEXT (9092) e PLAINTEXT_HOST (9094), Broker Kafka (apache/kafka:4.1.2), Kafka UI (kafbat/kafka-ui:8090), KRaft: nó único broker+controller, Fatores de replicação 1 para nó único, Tópico Kafka "application-logs", Gargalo: commit por mensagem no consumidor (+30 more)

### Community 7 - "MetricsQueryService"
Cohesion: 0.23
Nodes (5): MetricsQueryPort, MetricsQueryService, AnalyticsWiring, Override, MetricsQueryFacade

### Community 8 - "org.springframework.context.annotation.Bean"
Cohesion: 0.14
Nodes (7): DuckDbPersistence, InMemoryPersistence, PersistenceConfiguration, SqlitePersistence, KafkaTopicsConfig, org.apache.kafka.clients.admin.NewTopic, org.springframework.context.annotation.Bean

### Community 9 - "Violation"
Cohesion: 0.08
Nodes (16): AuditEventMessage, AuditEventTranslator, AuditExceptionHandler, InvalidAuditException, java.time.format.DateTimeParseException, AnalyticsExceptionHandler, ApiError, ApiExceptionHandler (+8 more)

### Community 10 - "Order"
Cohesion: 0.17
Nodes (7): FakeRepository, Override, FakeRepository, FindOrderServiceTest, Override, Override, Order

### Community 11 - "Money"
Cohesion: 0.11
Nodes (10): OrderPlacedMessage, OrderPlaced, CustomerId, Override, Override, Money, Override, ProductId (+2 more)

### Community 12 - "FraudTopology.java"
Cohesion: 0.05
Nodes (37): AuditEventMessage, FraudDetectedMessage, FraudulentOrder, OrderPlacedMessage, Topics, CustomerFraudState, Override, OccurredAtTimestampExtractor (+29 more)

### Community 13 - "DomainEvent"
Cohesion: 0.13
Nodes (4): OutboxRecord, Override, OutboxTranslator, DomainEvent

### Community 14 - "orders-load.js"
Cohesion: 0.09
Nodes (28): acceptanceRate, APPROVAL_LIMIT, awaitStatus(), CATALOG, COMPENSATION_TIMEOUT_MS, compensationsObserved, FRAUD_MAX_ORDERS, options (+20 more)

### Community 15 - "package.json"
Cohesion: 0.14
Nodes (13): esbuild, @faker-js/faker, description, devDependencies, esbuild, @faker-js/faker, name, private (+5 more)

### Community 16 - "org.springframework.context.annotation.Configuration"
Cohesion: 0.23
Nodes (11): FraudStreamsConfiguration, KafkaStreamsConfiguration, org.springframework.boot.autoconfigure.condition.ConditionalOnProperty, org.springframework.boot.autoconfigure.kafka.KafkaProperties, org.springframework.boot.context.properties.EnableConfigurationProperties, org.springframework.context.annotation.Configuration, org.springframework.kafka.annotation.EnableKafkaStreams, org.springframework.kafka.config.KafkaStreamsConfiguration (+3 more)

### Community 17 - "org.springframework.boot.autoconfigure.SpringBootApplication"
Cohesion: 0.18
Nodes (7): AuditServiceBootstrap, FraudServiceBootstrap, MetricsConsumerBootstrap, OrderServiceBootstrap, org.springframework.boot.autoconfigure.SpringBootApplication, org.springframework.scheduling.annotation.EnableScheduling, PaymentServiceBootstrap

### Community 18 - "AuditServiceWiring.java"
Cohesion: 0.11
Nodes (10): AuditEventListener, IngestAuditPort, AuditQueryPort, AuditQueryService, IngestAuditService, AuditServiceWiring, AuditQueryFacade, Override (+2 more)

### Community 19 - "AuditEvent"
Cohesion: 0.21
Nodes (4): Override, StdoutAuditRepository, StdoutAuditRepositoryTest, AuditEvent

### Community 22 - "Payment"
Cohesion: 0.18
Nodes (8): Payment, PaymentId, PaymentStatus, APPROVED, CANCELLED, FAILED, PENDING, PaymentRepository

### Community 23 - "DomainEvent"
Cohesion: 0.21
Nodes (6): OutboxTranslator, DomainEvent, PaymentApproved, PaymentCancelled, PaymentFailed, Override

### Community 24 - "OrderServiceWiring.java"
Cohesion: 0.11
Nodes (11): FindOrderPort, OrderResponse, FindOrderService, FindOrderUseCase, OrderView, PlaceOrderService, ActivityLogPublisher, OrderServiceWiring (+3 more)

### Community 25 - "AuditRepository"
Cohesion: 0.33
Nodes (6): AuditRepositoryContractTest, AuditLevel, Connection, AuditRepository, org.junit.jupiter.params.ParameterizedTest, org.junit.jupiter.params.provider.MethodSource

### Community 34 - "SqliteOrderRepository"
Cohesion: 0.23
Nodes (5): ObjectMapper, OutboxRecord, Override, SqliteOrderRepository, SqliteOrderRepositoryTest

### Community 35 - "AuditFilter"
Cohesion: 0.12
Nodes (9): AuditFilter, AuditLevel, DEBUG, ERROR, INFO, WARN, isAtLeast(), parse() (+1 more)

### Community 36 - "OrderPlaced"
Cohesion: 0.13
Nodes (9): OrderPlacedPort, OrderPlacedHandler, FakeLedger, OrderPlaced, ProductId, OrderPlacedHandlerTest, Override, OrderPlacedFacade (+1 more)

### Community 39 - "org.springframework.stereotype.Component"
Cohesion: 0.17
Nodes (7): OrderListener, OrderPlacedMessage, OutboxRelayScheduler, org.springframework.kafka.annotation.KafkaListener, org.springframework.scheduling.annotation.Scheduled, org.springframework.stereotype.Component, OutboxRelayScheduler

### Community 46 - "ApplyPaymentResult"
Cohesion: 0.22
Nodes (4): ApplyPaymentResult, ApplyPaymentResultTest, Override, PaymentEventFacade

### Community 47 - ".charge"
Cohesion: 0.21
Nodes (5): DeterministicPaymentGateway, Override, DeterministicPaymentGatewayTest, GatewayResult, PaymentGateway

### Community 48 - "OutboxRecord"
Cohesion: 0.10
Nodes (11): Override, OutboxRelay, FunctionalInterface, OutboxDispatcher, OutboxRecord, OutboxStore, FakeStore, OutboxRecord (+3 more)

### Community 49 - "run.sh"
Cohesion: 0.22
Nodes (14): await_http(), await_log(), cleanup(), die(), log(), LOG_DIR, REPO_ROOT, require() (+6 more)

### Community 51 - "ProductSalesRepository"
Cohesion: 0.18
Nodes (6): ProductId, ProductSalesRepositoryContractTest, Override, Quantity, ProductSalesRepository, ProductSalesRecordTest

### Community 53 - "Anticorruption Layer de tradução na fronteira"
Cohesion: 0.50
Nodes (5): Anticorruption Layer de tradução na fronteira, Contratos de evento duplicados por serviço, LogEntryTranslator, spring.json.add.type.headers=false, OrderPlacedTranslator

### Community 60 - "OrderPlacedPort"
Cohesion: 0.22
Nodes (5): OrderPlacedListener, OrderPlacedMessage, OrderPlacedPort, Override, OrderPlacedFacade

### Community 63 - "DeadLetterProperties"
Cohesion: 0.13
Nodes (6): ConcurrentKafkaListenerContainerFactory, DeadLetterProperties, KafkaErrorHandlingConfig, org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory, org.springframework.kafka.listener.CommonErrorHandler, FraudEventConsumerConfig

### Community 64 - "OutboxRecord"
Cohesion: 0.11
Nodes (10): Override, OutboxRelay, FunctionalInterface, OutboxDispatcher, OutboxRecord, OutboxStore, FakeStore, OutboxRecord (+2 more)

### Community 65 - "dev.joaolaureano.trainingkafka.payment.domain.model.Payment"
Cohesion: 0.23
Nodes (9): dev.joaolaureano.trainingkafka.payment.domain.event.DomainEvent, dev.joaolaureano.trainingkafka.payment.domain.model.Payment, FakeRepository, GatewayResult, Override, FakeRepository, GatewayResult, Override (+1 more)

### Community 67 - "DeadLetterProperties"
Cohesion: 0.16
Nodes (3): DeadLetterProperties, DefaultErrorHandler, KafkaErrorHandlingConfig

### Community 68 - "DeadLetterProperties"
Cohesion: 0.16
Nodes (3): DeadLetterProperties, DefaultErrorHandler, KafkaErrorHandlingConfig

### Community 69 - "JsonlFileAuditRepository"
Cohesion: 0.48
Nodes (3): Override, JsonlFileAuditRepository, StoredLine

### Community 70 - "KafkaActivityLogPublisher"
Cohesion: 0.18
Nodes (5): AuditEventMessage, KafkaActivityLogPublisher, KafkaActivityLogPublisher, ActivityLogFacade, Override

### Community 71 - "PlaceOrderServiceTest.java"
Cohesion: 0.13
Nodes (8): ActivityLog, AuditLevel, ERROR, INFO, WARN, Override, RecordingLogPublisher, RecordingRepository

### Community 72 - "ActivityLogPublisher"
Cohesion: 0.26
Nodes (5): dev.joaolaureano.trainingkafka.payment.domain.model.PaymentStatus, dev.joaolaureano.trainingkafka.payment.domain.port.PaymentGateway, dev.joaolaureano.trainingkafka.payment.domain.port.PaymentRepository, ActivityLogPublisher, ProcessOrderPayment

### Community 73 - "org.junit.jupiter.api.BeforeEach"
Cohesion: 0.27
Nodes (6): InvalidOrderTransitionException, OrderStatus, CANCELLED, PAID, PENDING_PAYMENT, org.junit.jupiter.api.BeforeEach

### Community 74 - "PaymentEventPort"
Cohesion: 0.20
Nodes (4): PaymentEventListener, PaymentEventMessage, PaymentEventPort, UnknownPaymentEventException

### Community 75 - "org.slf4j.Logger"
Cohesion: 0.25
Nodes (6): AuditPersistenceConfiguration, DuckDbPersistence, JsonlPersistence, StdoutPersistence, org.slf4j.Logger, org.springframework.context.annotation.Profile

### Community 76 - "org.junit.jupiter.api.Test"
Cohesion: 0.06
Nodes (24): KafkaErrorHandlingConfigTest, ApplicationName, Override, AuditFilterTest, KafkaErrorHandlingConfigTest, TimeRange, Override, PlaceOrderCommand (+16 more)

### Community 77 - "PaymentWiring.java"
Cohesion: 0.12
Nodes (12): dev.joaolaureano.trainingkafka.payment.adapters.messaging.FraudEventPort, dev.joaolaureano.trainingkafka.payment.adapters.messaging.OrderPlacedPort, dev.joaolaureano.trainingkafka.payment.adapters.messaging.OutboxRelay, dev.joaolaureano.trainingkafka.payment.adapters.persistence.OutboxDispatcher, dev.joaolaureano.trainingkafka.payment.adapters.persistence.OutboxStore, dev.joaolaureano.trainingkafka.payment.adapters.persistence.OutboxTranslator, dev.joaolaureano.trainingkafka.payment.adapters.persistence.SqlitePaymentRepository, OutboxRelay (+4 more)

### Community 78 - "Money"
Cohesion: 0.09
Nodes (10): OrderPlaced, Quantity, OrderPlacedTranslator, CustomerId, Override, InvalidValueException, Override, Money (+2 more)

### Community 88 - ".audit"
Cohesion: 0.32
Nodes (3): FunctionalInterface, LogFactory, GatewayResult

### Community 89 - "AuditLevel"
Cohesion: 0.40
Nodes (4): AuditLevel, ERROR, INFO, WARN

### Community 91 - "java.sql.Connection"
Cohesion: 0.11
Nodes (12): java.sql.Connection, java.sql.PreparedStatement, java.sql.ResultSet, DuckDbOrderLedgerRepository, Override, DuckDbProductSalesRepository, MoneyCents, Override (+4 more)

### Community 92 - "ActivityLog"
Cohesion: 0.19
Nodes (4): ActivityLog, RecordingLogPublisher, RecordingLogPublisher, Override

### Community 94 - "PaymentEventOutboxTranslator"
Cohesion: 0.24
Nodes (5): PaymentEventMessage, OutboxRecord, Override, PaymentEventMessage, PaymentEventOutboxTranslator

### Community 95 - "com.fasterxml.jackson.databind.ObjectMapper"
Cohesion: 0.15
Nodes (9): com.fasterxml.jackson.databind.ObjectMapper, KafkaOutboxDispatcher, OrderEventOutboxTranslator, org.springframework.kafka.core.KafkaTemplate, AuditEventMessage, KafkaActivityLogPublisher, KafkaOutboxDispatcher, AuditEventMessageContractTest (+1 more)

### Community 96 - "OrderId"
Cohesion: 0.12
Nodes (7): PlaceOrderPort, PlaceOrderRequest, PlaceOrderUseCase, Override, PlaceOrderFacade, Override, OrderId

### Community 98 - "ProductSalesRecord"
Cohesion: 0.20
Nodes (6): InMemoryProductSalesRepository, Override, FakeProductSales, Override, Override, ProductSalesRecord

## Knowledge Gaps
- **81 isolated node(s):** `INFO`, `WARN`, `ERROR`, `AuditEventMessage`, `FraudulentOrder` (+76 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **44 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `DeadLetterProperties` connect `DeadLetterProperties` to `org.springframework.context.annotation.Bean`, `DeadLetterProperties`, `Retry`?**
  _High betweenness centrality (0.039) - this node is a cross-community bridge._
- **Why does `AuditEvent` connect `AuditEvent` to `AuditController.java`, `DuckDbAuditRepository`, `AuditFilter`, `JsonlFileAuditRepository`, `Violation`, `org.junit.jupiter.api.Test`, `AuditServiceWiring.java`, `AuditRepository`, `java.sql.Connection`?**
  _High betweenness centrality (0.032) - this node is a cross-community bridge._
- **Why does `Retry` connect `Retry` to `DeadLetterProperties`?**
  _High betweenness centrality (0.031) - this node is a cross-community bridge._
- **What connects `INFO`, `WARN`, `ERROR` to the rest of the system?**
  _81 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Units` be split into smaller, more focused modules?**
  _Cohesion score 0.14492753623188406 - nodes in this community are weakly interconnected._
- **Should `ProductId` be split into smaller, more focused modules?**
  _Cohesion score 0.12210915818686402 - nodes in this community are weakly interconnected._
- **Should `DeadLetterProperties` be split into smaller, more focused modules?**
  _Cohesion score 0.11462450592885376 - nodes in this community are weakly interconnected._