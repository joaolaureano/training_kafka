# Graph Report - training_kafka  (2026-09-04)

## Corpus Check
- 312 files · ~67,309 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1960 nodes · 5394 edges · 123 communities (71 shown, 52 thin omitted)
- Extraction: 88% EXTRACTED · 12% INFERRED · 0% AMBIGUOUS · INFERRED: 645 edges (avg confidence: 0.81)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `4f61c541`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- MetricsQueryService
- ProductSalesRecord
- .request
- TimeRange
- DeadLetterProperties
- org.junit.jupiter.api.Test
- metrics-consumer (App B)
- Order
- org.springframework.context.annotation.Bean
- .place
- OrderServiceWiring.java
- Money
- CustomerFraudPattern
- InventoryWiring.java
- orders-load.js
- package.json
- org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
- org.springframework.boot.autoconfigure.SpringBootApplication
- AuditServiceWiring.java
- StdoutAuditRepository
- SqliteRepositoryException
- dev.joaolaureano.trainingkafka:training-kafka
- Product
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
- Money
- audit-service
- audit-service-adapters
- org.springframework.scheduling.annotation.Scheduled
- analytics/adapters/messaging/AuditEventMessage.java
- audit-service-application
- Topics
- Topics
- audit-service-bootstrap
- audit-service-domain
- ApplyPaymentResult
- AuditLevel
- OutboxRecord
- run.sh
- WhenValid
- ProductSalesRepository
- .handle
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
- OutboxRelayTest
- ProcessOrderPaymentTest
- SqliteInventoryRepository
- DeadLetterProperties
- DeadLetterProperties
- AuditEvent
- KafkaActivityLogPublisher
- Quantity
- .handle
- DeadLetterProperties
- Sku
- InvalidAuditException
- AuditFilterTest
- ProductResponse
- Retry
- OutboxRelayTest
- Retry
- UnknownOrderException
- Retry
- payment-service
- payment-service-adapters
- payment-service-application
- payment-service-bootstrap
- payment-service-domain
- PaymentEventOutboxTranslator
- KafkaActivityLogPublisher
- Topics
- java.sql.Connection
- ActivityLog
- org.junit.jupiter.api.DisplayName
- FraudTopologyTest
- com.fasterxml.jackson.databind.ObjectMapper
- Topics
- FraudTransformer
- org.springframework.context.annotation.Configuration
- OutboxRecord
- Reservation
- FraudTopology.java
- org.springframework.stereotype.Component
- KafkaActivityLogPublisher
- OrderPlacedPort
- InventoryEventPort
- InventoryWiring
- ReserveStockForOrderTest
- ReservationTest
- InventoryEventOutboxTranslator
- PaymentEventPort
- ApplyStockResult
- OccurredAtTimestampExtractor.java
- AuditLevel
- FraudDetectedMessage
- .charge
- Topics
- inventory-service
- inventory-service-adapters
- inventory-service-application
- inventory-service-bootstrap
- inventory-service-domain

## God Nodes (most connected - your core abstractions)
1. `Order` - 65 edges
2. `Payment` - 53 edges
3. `ProductSalesRecord` - 49 edges
4. `Quantity` - 47 edges
5. `Product` - 46 edges
6. `ProductId` - 45 edges
7. `AuditEvent` - 43 edges
8. `Reservation` - 43 edges
9. `OrderId` - 41 edges
10. `Sku` - 37 edges

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

## Communities (123 total, 52 thin omitted)

### Community 0 - "MetricsQueryService"
Cohesion: 0.13
Nodes (8): MetricsController, ProductSalesView, RevenueView, MetricsQueryPort, MetricsQueryService, AnalyticsWiring, Override, MetricsQueryFacade

### Community 1 - "ProductSalesRecord"
Cohesion: 0.09
Nodes (16): Override, ProductId, InMemoryProductSalesRepository, Override, Money, Override, ProductId, FakeProductSales (+8 more)

### Community 2 - ".request"
Cohesion: 0.18
Nodes (5): OutboxRecord, Override, SqlitePaymentRepository, SqlitePaymentRepositoryTest, PaymentTest

### Community 3 - "TimeRange"
Cohesion: 0.12
Nodes (8): DuckDbOrderLedgerRepository, Override, InMemoryOrderLedgerRepository, Override, OrderLedgerRepositoryContractTest, RevenueWindow, TimeRange, OrderLedgerRepository

### Community 4 - "DeadLetterProperties"
Cohesion: 0.11
Nodes (3): DeadLetterProperties, FraudProperties, org.springframework.boot.context.properties.ConfigurationProperties

### Community 5 - "org.junit.jupiter.api.Test"
Cohesion: 0.13
Nodes (6): KafkaErrorHandlingConfigTest, KafkaErrorHandlingConfigTest, Override, org.junit.jupiter.api.Test, org.springframework.boot.test.context.runner.ApplicationContextRunner, AuditEventMessageContractTest

### Community 6 - "metrics-consumer (App B)"
Cohesion: 0.08
Nodes (38): KAFKA_AUTO_CREATE_TOPICS_ENABLE=false, Listeners PLAINTEXT (9092) e PLAINTEXT_HOST (9094), Broker Kafka (apache/kafka:4.1.2), Kafka UI (kafbat/kafka-ui:8090), KRaft: nó único broker+controller, Fatores de replicação 1 para nó único, Tópico Kafka "application-logs", Gargalo: commit por mensagem no consumidor (+30 more)

### Community 7 - "Order"
Cohesion: 0.08
Nodes (21): FindOrderService, FakeRepository, Override, FakeRepository, FindOrderServiceTest, Override, Override, RecordingRepository (+13 more)

### Community 8 - "org.springframework.context.annotation.Bean"
Cohesion: 0.10
Nodes (12): AuditPersistenceConfiguration, DuckDbPersistence, JsonlPersistence, StdoutPersistence, DuckDbPersistence, InMemoryPersistence, PersistenceConfiguration, SqlitePersistence (+4 more)

### Community 9 - ".place"
Cohesion: 0.06
Nodes (25): AuditExceptionHandler, ApiError, ApiExceptionHandler, InvalidProductException, java.time.format.DateTimeParseException, OrderPlacedMessage, OrderPlaced, Quantity (+17 more)

### Community 10 - "OrderServiceWiring.java"
Cohesion: 0.08
Nodes (14): FindOrderPort, OrderController, OrderResponse, PlaceOrderPort, PlaceOrderRequest, PlaceOrderResponse, FindOrderUseCase, OrderView (+6 more)

### Community 11 - "Money"
Cohesion: 0.09
Nodes (12): OutboxRecord, Override, OrderPlacedMessage, OrderPlaced, CustomerId, Override, Override, Money (+4 more)

### Community 12 - "CustomerFraudPattern"
Cohesion: 0.17
Nodes (5): FraudDetected, CustomerFraudPattern, FraudOrder, FraudPolicy, CustomerFraudPatternTest

### Community 13 - "InventoryWiring.java"
Cohesion: 0.10
Nodes (10): ManageCatalog, OptimisticRetry, ActivityLog, ActivityLogPublisher, ReleaseStockForOrder, ReserveStockForOrder, RecordingLogPublisher, Override (+2 more)

### Community 14 - "orders-load.js"
Cohesion: 0.06
Nodes (44): acceptanceRate, APPROVAL_LIMIT, awaitStatus(), CATALOG, COMPENSATION_METRICS, COMPENSATION_TIMEOUT_MS, compensationDuration, compensationSettled (+36 more)

### Community 15 - "package.json"
Cohesion: 0.14
Nodes (13): esbuild, @faker-js/faker, description, devDependencies, esbuild, @faker-js/faker, name, private (+5 more)

### Community 16 - "org.springframework.boot.autoconfigure.condition.ConditionalOnProperty"
Cohesion: 0.26
Nodes (10): KafkaErrorHandlingConfig, KafkaErrorHandlingConfig, org.slf4j.Logger, org.springframework.boot.autoconfigure.condition.ConditionalOnProperty, org.springframework.boot.autoconfigure.kafka.KafkaProperties, org.springframework.boot.context.properties.EnableConfigurationProperties, org.springframework.kafka.core.KafkaOperations, org.springframework.kafka.listener.CommonErrorHandler (+2 more)

### Community 17 - "org.springframework.boot.autoconfigure.SpringBootApplication"
Cohesion: 0.16
Nodes (8): AuditServiceBootstrap, FraudServiceBootstrap, InventoryServiceBootstrap, MetricsConsumerBootstrap, OrderServiceBootstrap, org.springframework.boot.autoconfigure.SpringBootApplication, org.springframework.scheduling.annotation.EnableScheduling, PaymentServiceBootstrap

### Community 18 - "AuditServiceWiring.java"
Cohesion: 0.10
Nodes (10): AuditEventListener, IngestAuditPort, AuditQueryPort, AuditQueryService, IngestAuditService, AuditServiceWiring, AuditQueryFacade, Override (+2 more)

### Community 19 - "StdoutAuditRepository"
Cohesion: 0.33
Nodes (3): Override, StdoutAuditRepository, StdoutAuditRepositoryTest

### Community 22 - "Product"
Cohesion: 0.13
Nodes (5): FakeInventory, Override, Override, Product, Override

### Community 23 - "Payment"
Cohesion: 0.09
Nodes (11): FakeRepository, Override, FakeRepository, Override, DomainEvent, PaymentApproved, PaymentCancelled, PaymentFailed (+3 more)

### Community 24 - "OutboxRecord"
Cohesion: 0.12
Nodes (9): Override, OutboxRelay, FunctionalInterface, OutboxDispatcher, OutboxRecord, OutboxStore, FakeStore, Override (+1 more)

### Community 25 - "AuditRepository"
Cohesion: 0.30
Nodes (4): AuditRepositoryContractTest, AuditLevel, Connection, AuditRepository

### Community 34 - "SqliteOrderRepository"
Cohesion: 0.25
Nodes (4): OutboxRecord, Override, SqliteOrderRepository, SqliteOrderRepositoryTest

### Community 35 - "AuditFilter"
Cohesion: 0.14
Nodes (8): AuditController, AuditEventView, AuditFilter, TimeRange, org.springframework.web.bind.annotation.GetMapping, org.springframework.web.bind.annotation.PutMapping, org.springframework.web.bind.annotation.RequestMapping, org.springframework.web.bind.annotation.RestController

### Community 36 - "Money"
Cohesion: 0.08
Nodes (16): Override, OrderPlacedHandler, FakeLedger, OrderPlaced, ProductId, OrderPlacedHandlerTest, Override, OrderPlacedFacade (+8 more)

### Community 39 - "org.springframework.scheduling.annotation.Scheduled"
Cohesion: 0.19
Nodes (4): OutboxRelayScheduler, OutboxRelayScheduler, org.springframework.scheduling.annotation.Scheduled, OutboxRelayScheduler

### Community 46 - "ApplyPaymentResult"
Cohesion: 0.11
Nodes (8): PaymentEventListener, PaymentEventMessage, PaymentEventPort, UnknownPaymentEventException, ApplyPaymentResult, ApplyPaymentResultTest, Override, PaymentEventFacade

### Community 47 - "AuditLevel"
Cohesion: 0.28
Nodes (7): AuditLevel, DEBUG, ERROR, INFO, WARN, isAtLeast(), parse()

### Community 48 - "OutboxRecord"
Cohesion: 0.13
Nodes (8): OutboxRelay, FunctionalInterface, OutboxDispatcher, OutboxRecord, OutboxStore, FakeStore, Override, OutboxRelay

### Community 49 - "run.sh"
Cohesion: 0.21
Nodes (15): await_http(), await_log(), cleanup(), die(), log(), LOG_DIR, REPO_ROOT, require() (+7 more)

### Community 51 - "ProductSalesRepository"
Cohesion: 0.21
Nodes (8): ProductId, ProductSalesRepositoryContractTest, Override, Quantity, ProductSalesRepository, ProductSalesRecordTest, org.junit.jupiter.params.ParameterizedTest, org.junit.jupiter.params.provider.MethodSource

### Community 52 - ".handle"
Cohesion: 0.20
Nodes (6): FraudulentOrder, FunctionalInterface, LogFactory, CompensateFraudulentOrdersTest, FraudulentOrder, Override

### Community 53 - "Anticorruption Layer de tradução na fronteira"
Cohesion: 0.50
Nodes (5): Anticorruption Layer de tradução na fronteira, Contratos de evento duplicados por serviço, LogEntryTranslator, spring.json.add.type.headers=false, OrderPlacedTranslator

### Community 60 - "PaymentWiring.java"
Cohesion: 0.10
Nodes (12): DeterministicPaymentGateway, FraudEventPort, InventoryEventMessage, StockReservedPort, OutboxTranslator, CompensateFraudulentOrders, ProcessOrderPayment, PaymentWiring (+4 more)

### Community 62 - "OrderServiceWiring"
Cohesion: 0.18
Nodes (3): OutboxTranslator, KafkaActivityLogPublisher, OrderServiceWiring

### Community 65 - "ProcessOrderPaymentTest"
Cohesion: 0.22
Nodes (5): Override, RecordingGateway, ProcessOrderPaymentTest, RecordingGateway, GatewayResult

### Community 66 - "SqliteInventoryRepository"
Cohesion: 0.24
Nodes (5): OutboxRecord, Override, SqliteInventoryRepository, Quantity, SqliteInventoryRepositoryTest

### Community 67 - "DeadLetterProperties"
Cohesion: 0.16
Nodes (3): DeadLetterProperties, DefaultErrorHandler, KafkaErrorHandlingConfig

### Community 68 - "DeadLetterProperties"
Cohesion: 0.15
Nodes (3): DeadLetterProperties, DefaultErrorHandler, KafkaErrorHandlingConfig

### Community 69 - "AuditEvent"
Cohesion: 0.12
Nodes (10): DuckDbAuditRepository, Override, ObjectMapper, Override, JsonlFileAuditRepository, StoredLine, ApplicationName, Override (+2 more)

### Community 70 - "KafkaActivityLogPublisher"
Cohesion: 0.09
Nodes (12): AuditEventMessage, KafkaActivityLogPublisher, PlaceOrderService, ActivityLog, ActivityLogPublisher, AuditLevel, ERROR, INFO (+4 more)

### Community 71 - "Quantity"
Cohesion: 0.09
Nodes (8): InsufficientStockException, Override, Quantity, ReservationStatus, HELD, REJECTED, RELEASED, VOIDED

### Community 72 - ".handle"
Cohesion: 0.42
Nodes (3): PlaceOrderCommand, Override, PlaceOrderServiceTest

### Community 73 - "DeadLetterProperties"
Cohesion: 0.10
Nodes (4): DeadLetterProperties, Retry, DefaultErrorHandler, KafkaErrorHandlingConfig

### Community 74 - "Sku"
Cohesion: 0.23
Nodes (9): DomainEvent, StockRejected, StockReleased, StockReserved, RejectionReason, OUT_OF_STOCK, UNKNOWN_PRODUCT, Sku (+1 more)

### Community 75 - "InvalidAuditException"
Cohesion: 0.24
Nodes (3): AuditEventMessage, AuditEventTranslator, InvalidAuditException

### Community 77 - "ProductResponse"
Cohesion: 0.17
Nodes (7): FindProductPort, ProductController, ProductResponse, UpsertProductPort, UpsertProductRequest, CatalogFacade, Override

### Community 88 - "PaymentEventOutboxTranslator"
Cohesion: 0.24
Nodes (5): PaymentEventMessage, OutboxRecord, Override, PaymentEventMessage, PaymentEventOutboxTranslator

### Community 89 - "KafkaActivityLogPublisher"
Cohesion: 0.16
Nodes (5): AuditEventMessage, KafkaActivityLogPublisher, KafkaActivityLogPublisher, ActivityLogFacade, Override

### Community 91 - "java.sql.Connection"
Cohesion: 0.15
Nodes (8): java.sql.Connection, java.sql.PreparedStatement, DuckDbProductSalesRepository, MoneyCents, SqliteOrderLedgerRepository, SqliteProductSalesRepository, TestRepositories, org.junit.jupiter.params.provider.Arguments

### Community 92 - "ActivityLog"
Cohesion: 0.10
Nodes (14): ActivityLog, ActivityLogPublisher, AuditLevel, ERROR, INFO, WARN, RecordingLogPublisher, RecordingLogPublisher (+6 more)

### Community 93 - "org.junit.jupiter.api.DisplayName"
Cohesion: 0.10
Nodes (10): ProductTest, WhenDefining, WhenReleasing, WhenReserving, TimeRange, RevenueWindowTest, MoneyRules, WhenInvalid (+2 more)

### Community 94 - "FraudTopologyTest"
Cohesion: 0.15
Nodes (8): OrderPlacedMessage, FraudTopologyTest, OrderPlacedMessage, org.apache.kafka.streams.state.KeyValueStore, org.apache.kafka.streams.TestInputTopic, org.apache.kafka.streams.TestOutputTopic, org.apache.kafka.streams.TopologyTestDriver, org.junit.jupiter.api.AfterEach

### Community 95 - "com.fasterxml.jackson.databind.ObjectMapper"
Cohesion: 0.26
Nodes (7): com.fasterxml.jackson.databind.ObjectMapper, KafkaOutboxDispatcher, Override, KafkaOutboxDispatcher, OrderEventOutboxTranslator, org.springframework.kafka.core.KafkaTemplate, KafkaOutboxDispatcher

### Community 97 - "FraudTransformer"
Cohesion: 0.17
Nodes (10): FraudDetectedMessage, FraudulentOrder, CustomerFraudState, FraudTransformer, FraudDetectedMessage, Override, org.apache.kafka.streams.KeyValue, org.apache.kafka.streams.kstream.Transformer (+2 more)

### Community 98 - "org.springframework.context.annotation.Configuration"
Cohesion: 0.16
Nodes (12): FraudStreamsConfiguration, ConcurrentKafkaListenerContainerFactory, PaymentEventConsumerConfig, KafkaStreamsConfiguration, InventoryEventConsumerConfig, ConcurrentKafkaListenerContainerFactory, org.springframework.context.annotation.Configuration, org.springframework.kafka.annotation.EnableKafkaStreams (+4 more)

### Community 99 - "OutboxRecord"
Cohesion: 0.15
Nodes (7): Override, OutboxRelay, FunctionalInterface, OutboxDispatcher, OutboxRecord, OutboxStore, OutboxRelay

### Community 101 - "FraudTopology.java"
Cohesion: 0.18
Nodes (10): AuditEventMessage, Topics, FraudDetectionService, FraudTopology, FraudTransformerSupplier, AuditEventMessage, org.apache.kafka.streams.kstream.KStream, org.apache.kafka.streams.kstream.TransformerSupplier (+2 more)

### Community 102 - "org.springframework.stereotype.Component"
Cohesion: 0.19
Nodes (6): OrderListener, OrderPlacedPort, org.springframework.kafka.annotation.KafkaListener, org.springframework.stereotype.Component, FraudEventListener, StockReservedListener

### Community 103 - "KafkaActivityLogPublisher"
Cohesion: 0.19
Nodes (4): AuditEventMessage, KafkaActivityLogPublisher, ActivityLogFacade, Override

### Community 104 - "OrderPlacedPort"
Cohesion: 0.20
Nodes (5): OrderPlacedListener, OrderPlacedMessage, OrderPlacedPort, Override, OrderPlacedFacade

### Community 105 - "InventoryEventPort"
Cohesion: 0.22
Nodes (4): InventoryEventListener, InventoryEventMessage, InventoryEventPort, UnknownInventoryEventException

### Community 106 - "InventoryWiring"
Cohesion: 0.18
Nodes (3): OutboxTranslator, InventoryWiring, KafkaActivityLogPublisher

### Community 109 - "InventoryEventOutboxTranslator"
Cohesion: 0.24
Nodes (5): InventoryEventMessage, InventoryEventOutboxTranslator, InventoryEventMessage, OutboxRecord, Override

### Community 110 - "PaymentEventPort"
Cohesion: 0.24
Nodes (3): PaymentEventListener, PaymentEventMessage, PaymentEventPort

### Community 111 - "ApplyStockResult"
Cohesion: 0.31
Nodes (3): ApplyStockResult, InventoryEventFacade, Override

### Community 113 - "OccurredAtTimestampExtractor.java"
Cohesion: 0.47
Nodes (4): Override, OccurredAtTimestampExtractor, org.apache.kafka.clients.consumer.ConsumerRecord, org.apache.kafka.streams.processor.TimestampExtractor

### Community 114 - "AuditLevel"
Cohesion: 0.40
Nodes (4): AuditLevel, ERROR, INFO, WARN

## Knowledge Gaps
- **108 isolated node(s):** `audit-service-adapters`, `audit-service-application`, `audit-service-bootstrap`, `audit-service-domain`, `DEBUG` (+103 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **52 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `AuditEvent` connect `AuditEvent` to `AuditFilter`, `InvalidAuditException`, `AuditFilterTest`, `AuditLevel`, `AuditServiceWiring.java`, `StdoutAuditRepository`, `AuditRepository`?**
  _High betweenness centrality (0.038) - this node is a cross-community bridge._
- **Why does `OrderId` connect `Order` to `SqliteOrderRepository`, `AuditFilter`, `KafkaActivityLogPublisher`, `.handle`, `OrderServiceWiring.java`, `Money`, `ApplyPaymentResult`?**
  _High betweenness centrality (0.033) - this node is a cross-community bridge._
- **Why does `DeadLetterProperties` connect `DeadLetterProperties` to `org.springframework.boot.autoconfigure.condition.ConditionalOnProperty`, `org.springframework.context.annotation.Bean`, `DeadLetterProperties`?**
  _High betweenness centrality (0.028) - this node is a cross-community bridge._
- **What connects `audit-service-adapters`, `audit-service-application`, `audit-service-bootstrap` to the rest of the system?**
  _108 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `MetricsQueryService` be split into smaller, more focused modules?**
  _Cohesion score 0.13 - nodes in this community are weakly interconnected._
- **Should `ProductSalesRecord` be split into smaller, more focused modules?**
  _Cohesion score 0.0935374149659864 - nodes in this community are weakly interconnected._
- **Should `TimeRange` be split into smaller, more focused modules?**
  _Cohesion score 0.1184939091915836 - nodes in this community are weakly interconnected._