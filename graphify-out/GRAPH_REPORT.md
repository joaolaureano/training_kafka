# Graph Report - training_kafka  (2026-09-04)

## Corpus Check
- 312 files · ~67,356 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 2006 nodes · 5355 edges · 139 communities (78 shown, 61 thin omitted)
- Extraction: 88% EXTRACTED · 12% INFERRED · 0% AMBIGUOUS · INFERRED: 637 edges (avg confidence: 0.81)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `005ec09d`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- MetricsQueryService
- ProductSalesRecord
- .request
- ProductId
- DeadLetterProperties
- .place
- metrics-consumer (App B)
- Order
- org.springframework.context.annotation.Bean
- org.springframework.http.ResponseEntity
- OrderController
- OrderPlaced
- CustomerFraudPattern
- InventoryWiring.java
- orders-load.js
- package.json
- org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
- org.springframework.boot.autoconfigure.SpringBootApplication
- AuditServiceWiring.java
- PaymentWiring
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
- OrderRecord
- audit-service
- audit-service-adapters
- org.springframework.stereotype.Component
- analytics/adapters/messaging/AuditEventMessage.java
- audit-service-application
- OrderId
- Topics
- audit-service-bootstrap
- audit-service-domain
- OrderRepository
- AuditLevel
- OutboxRelay
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
- OrderServiceWiring.java
- DeadLetterProperties
- DuckDbAuditRepository
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
- AuditController.java
- ProductResponse
- org.junit.jupiter.api.BeforeEach
- OutboxRelayTest
- Money
- UnknownOrderException
- OrderEventOutboxTranslator.java
- payment-service
- payment-service-adapters
- payment-service-application
- payment-service-bootstrap
- payment-service-domain
- DomainEvent
- KafkaActivityLogPublisher
- Topics
- java.sql.Connection
- ActivityLog
- org.junit.jupiter.api.Test
- FraudTopologyTest
- com.fasterxml.jackson.databind.ObjectMapper
- Topics
- FraudTransformer
- org.springframework.context.annotation.Configuration
- OutboxRecord
- Reservation
- FraudTopology.java
- org.springframework.kafka.annotation.KafkaListener
- KafkaActivityLogPublisher
- OrderPlacedPort
- InventoryEventPort
- PaymentEventPort
- Retry
- Quantity
- InventoryEventOutboxTranslator
- OutboxRecord
- TestRepositories
- .place
- OccurredAtTimestampExtractor.java
- AuditLevel
- FraudDetectedMessage
- GatewayResult
- Topics
- inventory-service
- inventory-service-adapters
- inventory-service-application
- inventory-service-bootstrap
- inventory-service-domain
- Money
- .decide
- FraudStreamsConfiguration.java
- orders/adapters/messaging/AuditEventMessage.java
- OutboxRecord
- FunctionalInterface
- OutboxRecord
- Quantity
- OutboxRelay
- OutboxRecord
- OutboxRelay
- CustomerId
- Money
- ProductId
- Quantity
- OutboxRelay

## God Nodes (most connected - your core abstractions)
1. `Order` - 65 edges
2. `Quantity` - 64 edges
3. `Payment` - 53 edges
4. `ProductSalesRecord` - 49 edges
5. `Product` - 46 edges
6. `ProductId` - 45 edges
7. `Reservation` - 43 edges
8. `AuditEvent` - 39 edges
9. `Sku` - 37 edges
10. `ProductSalesRepository` - 34 edges

## Surprising Connections (you probably didn't know these)
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` --conceptually_related_to--> `order-service (App A)`  [INFERRED]
  docker-compose.yml → README.md
- `KafkaErrorHandlingConfig` --references--> `DeadLetterProperties`  [EXTRACTED]
  payment-service/payment-service-adapters/src/main/java/dev/joaolaureano/trainingkafka/payment/adapters/config/KafkaErrorHandlingConfig.java → inventory-service/inventory-service-adapters/src/main/java/dev/joaolaureano/trainingkafka/inventory/adapters/config/DeadLetterProperties.java
- `SqliteOrderRepository` --implements--> `OutboxStore`  [EXTRACTED]
  order-service/order-service-adapters/src/main/java/dev/joaolaureano/trainingkafka/orders/adapters/persistence/SqliteOrderRepository.java → inventory-service/inventory-service-adapters/src/main/java/dev/joaolaureano/trainingkafka/inventory/adapters/persistence/OutboxStore.java
- `SqliteOrderRepository` --references--> `OutboxTranslator`  [EXTRACTED]
  order-service/order-service-adapters/src/main/java/dev/joaolaureano/trainingkafka/orders/adapters/persistence/SqliteOrderRepository.java → inventory-service/inventory-service-adapters/src/main/java/dev/joaolaureano/trainingkafka/inventory/adapters/persistence/OutboxTranslator.java
- `Order` --references--> `Quantity`  [EXTRACTED]
  order-service/order-service-domain/src/main/java/dev/joaolaureano/trainingkafka/orders/domain/model/Order.java → inventory-service/inventory-service-domain/src/main/java/dev/joaolaureano/trainingkafka/inventory/domain/model/Quantity.java

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Pipeline de eventos A → orders → B → application-logs → C** — readme_order_service, readme_orders_topic, readme_metrics_consumer, readme_application_logs_topic, readme_log_aggregator, docker_compose_kafka_broker [EXTRACTED 1.00]
- **Modelo de domínio do App B (3 raízes + VO derivado + handler)** — readme_productsalesrecord, readme_customerorderpattern, readme_orderrecord, readme_revenuewindow, readme_orderplacedhandler [EXTRACTED 1.00]

## Communities (139 total, 61 thin omitted)

### Community 0 - "MetricsQueryService"
Cohesion: 0.23
Nodes (5): MetricsQueryPort, MetricsQueryService, AnalyticsWiring, Override, MetricsQueryFacade

### Community 1 - "ProductSalesRecord"
Cohesion: 0.09
Nodes (15): Override, ProductId, InMemoryProductSalesRepository, Override, Money, Override, ProductId, FakeProductSales (+7 more)

### Community 2 - ".request"
Cohesion: 0.18
Nodes (5): OutboxRecord, Override, SqlitePaymentRepository, SqlitePaymentRepositoryTest, PaymentTest

### Community 3 - "ProductId"
Cohesion: 0.14
Nodes (7): OrderLedgerRepositoryContractTest, Override, ProductId, RevenueWindow, TimeRange, OrderLedgerRepository, RevenueWindowTest

### Community 4 - "DeadLetterProperties"
Cohesion: 0.10
Nodes (3): DeadLetterProperties, Retry, KafkaErrorHandlingConfig

### Community 5 - ".place"
Cohesion: 0.15
Nodes (5): FindOrderService, Override, OrderView, FindOrderServiceTest, Violations

### Community 6 - "metrics-consumer (App B)"
Cohesion: 0.08
Nodes (38): KAFKA_AUTO_CREATE_TOPICS_ENABLE=false, Listeners PLAINTEXT (9092) e PLAINTEXT_HOST (9094), Broker Kafka (apache/kafka:4.1.2), Kafka UI (kafbat/kafka-ui:8090), KRaft: nó único broker+controller, Fatores de replicação 1 para nó único, Tópico Kafka "application-logs", Gargalo: commit por mensagem no consumidor (+30 more)

### Community 7 - "Order"
Cohesion: 0.13
Nodes (14): CustomerId, dev.joaolaureano.trainingkafka.orders.domain.event.DomainEvent, dev.joaolaureano.trainingkafka.orders.domain.model.OrderId, Money, FakeRepository, Override, FakeRepository, Override (+6 more)

### Community 8 - "org.springframework.context.annotation.Bean"
Cohesion: 0.19
Nodes (4): KafkaTopicsConfig, Topics, org.apache.kafka.clients.admin.NewTopic, org.springframework.context.annotation.Bean

### Community 9 - "org.springframework.http.ResponseEntity"
Cohesion: 0.06
Nodes (25): AuditExceptionHandler, ApiError, ApiExceptionHandler, InvalidProductException, java.time.format.DateTimeParseException, AnalyticsExceptionHandler, InvalidValueException, ApiError (+17 more)

### Community 10 - "OrderController"
Cohesion: 0.19
Nodes (7): FindOrderPort, OrderController, OrderResponse, PlaceOrderPort, FindOrderUseCase, FindOrderFacade, Override

### Community 11 - "OrderPlaced"
Cohesion: 0.14
Nodes (8): OrderPlacedMessage, OrderPlaced, CustomerId, Override, Override, ProductId, Override, Quantity

### Community 12 - "CustomerFraudPattern"
Cohesion: 0.17
Nodes (5): FraudDetected, CustomerFraudPattern, FraudOrder, FraudPolicy, CustomerFraudPatternTest

### Community 13 - "InventoryWiring.java"
Cohesion: 0.08
Nodes (14): PaymentEventListener, PaymentEventMessage, PaymentEventPort, OutboxTranslator, ManageCatalog, ActivityLog, ActivityLogPublisher, ReleaseStockForOrder (+6 more)

### Community 14 - "orders-load.js"
Cohesion: 0.06
Nodes (44): acceptanceRate, APPROVAL_LIMIT, awaitStatus(), CATALOG, COMPENSATION_METRICS, COMPENSATION_TIMEOUT_MS, compensationDuration, compensationSettled (+36 more)

### Community 15 - "package.json"
Cohesion: 0.14
Nodes (13): esbuild, @faker-js/faker, description, devDependencies, esbuild, @faker-js/faker, name, private (+5 more)

### Community 16 - "org.springframework.boot.autoconfigure.condition.ConditionalOnProperty"
Cohesion: 0.24
Nodes (11): KafkaErrorHandlingConfig, org.slf4j.Logger, org.springframework.boot.autoconfigure.condition.ConditionalOnProperty, org.springframework.boot.autoconfigure.kafka.KafkaProperties, org.springframework.boot.context.properties.EnableConfigurationProperties, org.springframework.kafka.core.KafkaOperations, org.springframework.kafka.listener.CommonErrorHandler, org.springframework.kafka.listener.DefaultErrorHandler (+3 more)

### Community 17 - "org.springframework.boot.autoconfigure.SpringBootApplication"
Cohesion: 0.16
Nodes (8): AuditServiceBootstrap, FraudServiceBootstrap, InventoryServiceBootstrap, MetricsConsumerBootstrap, OrderServiceBootstrap, org.springframework.boot.autoconfigure.SpringBootApplication, org.springframework.scheduling.annotation.EnableScheduling, PaymentServiceBootstrap

### Community 18 - "AuditServiceWiring.java"
Cohesion: 0.16
Nodes (6): AuditEventListener, IngestAuditPort, IngestAuditService, AuditServiceWiring, IngestAuditFacade, Override

### Community 19 - "PaymentWiring"
Cohesion: 0.13
Nodes (11): CompensateFraudulentOrders, dev.joaolaureano.trainingkafka.payment.adapters.messaging.FraudEventPort, dev.joaolaureano.trainingkafka.payment.adapters.persistence.OutboxTranslator, dev.joaolaureano.trainingkafka.payment.adapters.persistence.SqlitePaymentRepository, dev.joaolaureano.trainingkafka.payment.application.CompensateFraudulentOrders, dev.joaolaureano.trainingkafka.payment.application.port.ActivityLogPublisher, dev.joaolaureano.trainingkafka.payment.domain.port.PaymentGateway, dev.joaolaureano.trainingkafka.payment.domain.port.PaymentRepository (+3 more)

### Community 22 - "Product"
Cohesion: 0.17
Nodes (4): FakeInventory, Override, Product, WhenDefining

### Community 23 - "Payment"
Cohesion: 0.15
Nodes (6): FakeRepository, Override, FakeRepository, Override, Payment, PaymentId

### Community 24 - "OutboxRecord"
Cohesion: 0.11
Nodes (10): Override, OutboxRelay, FunctionalInterface, OutboxDispatcher, OutboxRecord, OutboxStore, FakeStore, OutboxRecord (+2 more)

### Community 25 - "AuditRepository"
Cohesion: 0.31
Nodes (7): AuditRepositoryContractTest, AuditLevel, Connection, AuditRepository, org.junit.jupiter.params.ParameterizedTest, org.junit.jupiter.params.provider.Arguments, org.junit.jupiter.params.provider.MethodSource

### Community 34 - "SqliteOrderRepository"
Cohesion: 0.27
Nodes (3): Override, SqliteOrderRepository, SqliteOrderRepositoryTest

### Community 35 - "AuditFilter"
Cohesion: 0.11
Nodes (7): AuditEventView, AuditQueryPort, AuditQueryService, AuditQueryFacade, Override, AuditFilter, TimeRange

### Community 36 - "OrderRecord"
Cohesion: 0.08
Nodes (17): OrderPlacedPort, Override, Override, Override, OrderPlacedHandler, FakeLedger, OrderPlaced, ProductId (+9 more)

### Community 39 - "org.springframework.stereotype.Component"
Cohesion: 0.20
Nodes (6): OutboxRelayScheduler, OrderListener, OutboxRelayScheduler, org.springframework.scheduling.annotation.Scheduled, org.springframework.stereotype.Component, OutboxRelayScheduler

### Community 42 - "OrderId"
Cohesion: 0.14
Nodes (6): PlaceOrderUseCase, Override, PlaceOrderFacade, InvalidOrderTransitionException, Override, OrderId

### Community 46 - "OrderRepository"
Cohesion: 0.11
Nodes (8): ApplyPaymentResult, ApplyStockResult, FindOrderService, ApplyPaymentResultTest, Override, Override, PaymentEventFacade, OrderRepository

### Community 47 - "AuditLevel"
Cohesion: 0.32
Nodes (7): AuditLevel, DEBUG, ERROR, INFO, WARN, isAtLeast(), parse()

### Community 48 - "OutboxRelay"
Cohesion: 0.24
Nodes (4): OutboxRelay, FunctionalInterface, OutboxDispatcher, OutboxStore

### Community 49 - "run.sh"
Cohesion: 0.21
Nodes (15): await_http(), await_log(), cleanup(), die(), log(), LOG_DIR, REPO_ROOT, require() (+7 more)

### Community 51 - "ProductSalesRepository"
Cohesion: 0.28
Nodes (5): ProductId, ProductSalesRepositoryContractTest, Override, Quantity, ProductSalesRepository

### Community 52 - ".handle"
Cohesion: 0.31
Nodes (3): FraudulentOrder, CompensateFraudulentOrdersTest, FraudulentOrder

### Community 53 - "Anticorruption Layer de tradução na fronteira"
Cohesion: 0.50
Nodes (5): Anticorruption Layer de tradução na fronteira, Contratos de evento duplicados por serviço, LogEntryTranslator, spring.json.add.type.headers=false, OrderPlacedTranslator

### Community 60 - "PaymentWiring.java"
Cohesion: 0.15
Nodes (9): dev.joaolaureano.trainingkafka.payment.adapters.messaging.OutboxRelay, dev.joaolaureano.trainingkafka.payment.adapters.persistence.OutboxDispatcher, dev.joaolaureano.trainingkafka.payment.adapters.persistence.OutboxStore, dev.joaolaureano.trainingkafka.payment.application.ProcessOrderPayment, InventoryEventMessage, StockReservedListener, StockReservedPort, Override (+1 more)

### Community 62 - "OrderServiceWiring.java"
Cohesion: 0.12
Nodes (13): dev.joaolaureano.trainingkafka.orders.adapters.messaging.OutboxRelay, dev.joaolaureano.trainingkafka.orders.adapters.messaging.PaymentEventPort, dev.joaolaureano.trainingkafka.orders.adapters.persistence.OutboxDispatcher, dev.joaolaureano.trainingkafka.orders.adapters.persistence.OutboxStore, dev.joaolaureano.trainingkafka.orders.adapters.persistence.OutboxTranslator, dev.joaolaureano.trainingkafka.orders.adapters.web.FindOrderPort, dev.joaolaureano.trainingkafka.orders.adapters.web.PlaceOrderPort, dev.joaolaureano.trainingkafka.orders.application.FindOrderService (+5 more)

### Community 63 - "DeadLetterProperties"
Cohesion: 0.08
Nodes (4): FraudProperties, DeadLetterProperties, Retry, org.springframework.boot.context.properties.ConfigurationProperties

### Community 64 - "DuckDbAuditRepository"
Cohesion: 0.19
Nodes (8): DuckDbAuditRepository, Override, AuditEvent, dev.joaolaureano.trainingkafka.audit.domain.model.AuditEvent, dev.joaolaureano.trainingkafka.audit.domain.model.AuditFilter, dev.joaolaureano.trainingkafka.audit.domain.model.AuditLevel, dev.joaolaureano.trainingkafka.audit.domain.port.AuditRepository, java.sql.ResultSet

### Community 65 - "ProcessOrderPaymentTest"
Cohesion: 0.33
Nodes (3): RecordingGateway, ProcessOrderPaymentTest, RecordingGateway

### Community 66 - "SqliteInventoryRepository"
Cohesion: 0.25
Nodes (3): Override, SqliteInventoryRepository, SqliteInventoryRepositoryTest

### Community 69 - "AuditEvent"
Cohesion: 0.14
Nodes (10): ObjectMapper, Override, JsonlFileAuditRepository, StoredLine, Override, StdoutAuditRepository, StdoutAuditRepositoryTest, ApplicationName (+2 more)

### Community 70 - "KafkaActivityLogPublisher"
Cohesion: 0.24
Nodes (3): AuditEventMessage, KafkaActivityLogPublisher, KafkaActivityLogPublisher

### Community 71 - "Quantity"
Cohesion: 0.12
Nodes (5): InsufficientStockException, Override, Quantity, ProductTest, WhenReserving

### Community 72 - ".handle"
Cohesion: 0.22
Nodes (7): dev.joaolaureano.trainingkafka.orders.application.port.ActivityLog, dev.joaolaureano.trainingkafka.orders.application.port.AuditLevel, PlaceOrderCommand, Override, PlaceOrderServiceTest, RecordingLogPublisher, PlaceOrderService

### Community 73 - "DeadLetterProperties"
Cohesion: 0.14
Nodes (5): DeadLetterProperties, DefaultErrorHandler, KafkaErrorHandlingConfig, DefaultErrorHandler, KafkaErrorHandlingConfig

### Community 74 - "Sku"
Cohesion: 0.15
Nodes (11): DomainEvent, StockRejected, StockReleased, StockReserved, Override, RejectionReason, OUT_OF_STOCK, UNKNOWN_PRODUCT (+3 more)

### Community 75 - "InvalidAuditException"
Cohesion: 0.24
Nodes (3): AuditEventMessage, AuditEventTranslator, InvalidAuditException

### Community 76 - "AuditController.java"
Cohesion: 0.23
Nodes (7): AuditController, MetricsController, ProductSalesView, RevenueView, org.springframework.web.bind.annotation.GetMapping, org.springframework.web.bind.annotation.RequestMapping, org.springframework.web.bind.annotation.RestController

### Community 77 - "ProductResponse"
Cohesion: 0.16
Nodes (8): FindProductPort, ProductController, ProductResponse, UpsertProductPort, UpsertProductRequest, CatalogFacade, Override, org.springframework.web.bind.annotation.PutMapping

### Community 78 - "org.junit.jupiter.api.BeforeEach"
Cohesion: 0.15
Nodes (9): dev.joaolaureano.trainingkafka.orders.domain.model.CustomerId, dev.joaolaureano.trainingkafka.orders.domain.model.Money, dev.joaolaureano.trainingkafka.orders.domain.model.ProductId, OrderStatus, CANCELLED, PAID, PENDING_PAYMENT, PENDING_STOCK (+1 more)

### Community 80 - "Money"
Cohesion: 0.16
Nodes (5): OrderPlaced, Quantity, OrderPlacedTranslator, Override, Money

### Community 82 - "OrderEventOutboxTranslator.java"
Cohesion: 0.22
Nodes (5): OutboxRecord, Override, OrderEventOutboxTranslator, OutboxTranslator, DomainEvent

### Community 88 - "DomainEvent"
Cohesion: 0.09
Nodes (11): PaymentEventMessage, OutboxRecord, Override, PaymentEventMessage, PaymentEventOutboxTranslator, OutboxTranslator, DomainEvent, PaymentApproved (+3 more)

### Community 89 - "KafkaActivityLogPublisher"
Cohesion: 0.21
Nodes (4): KafkaActivityLogPublisher, KafkaActivityLogPublisher, ActivityLogFacade, Override

### Community 91 - "java.sql.Connection"
Cohesion: 0.13
Nodes (11): java.sql.Connection, java.sql.PreparedStatement, DuckDbOrderLedgerRepository, DuckDbProductSalesRepository, InMemoryOrderLedgerRepository, MoneyCents, SqliteOrderLedgerRepository, SqliteProductSalesRepository (+3 more)

### Community 92 - "ActivityLog"
Cohesion: 0.10
Nodes (19): CompensateFraudulentOrders, FunctionalInterface, LogFactory, ActivityLog, ActivityLogPublisher, AuditLevel, ERROR, INFO (+11 more)

### Community 93 - "org.junit.jupiter.api.Test"
Cohesion: 0.08
Nodes (14): KafkaErrorHandlingConfigTest, AuditFilterTest, ReserveStockForOrderTest, WhenReleasing, KafkaErrorHandlingConfigTest, TimeRange, MoneyRules, WhenInvalid (+6 more)

### Community 94 - "FraudTopologyTest"
Cohesion: 0.15
Nodes (8): OrderPlacedMessage, FraudTopologyTest, OrderPlacedMessage, org.apache.kafka.streams.state.KeyValueStore, org.apache.kafka.streams.TestInputTopic, org.apache.kafka.streams.TestOutputTopic, org.apache.kafka.streams.TopologyTestDriver, org.junit.jupiter.api.AfterEach

### Community 95 - "com.fasterxml.jackson.databind.ObjectMapper"
Cohesion: 0.44
Nodes (5): com.fasterxml.jackson.databind.ObjectMapper, KafkaOutboxDispatcher, KafkaOutboxDispatcher, org.springframework.kafka.core.KafkaTemplate, KafkaOutboxDispatcher

### Community 97 - "FraudTransformer"
Cohesion: 0.17
Nodes (10): FraudDetectedMessage, FraudulentOrder, CustomerFraudState, FraudTransformer, FraudDetectedMessage, Override, org.apache.kafka.streams.KeyValue, org.apache.kafka.streams.kstream.Transformer (+2 more)

### Community 98 - "org.springframework.context.annotation.Configuration"
Cohesion: 0.12
Nodes (15): AuditPersistenceConfiguration, DuckDbPersistence, JsonlPersistence, StdoutPersistence, dev.joaolaureano.trainingkafka.payment.adapters.messaging.FraudDetectedMessage, ConcurrentKafkaListenerContainerFactory, PaymentEventConsumerConfig, InMemoryPersistence (+7 more)

### Community 99 - "OutboxRecord"
Cohesion: 0.15
Nodes (6): FunctionalInterface, Override, OutboxRelay, OutboxDispatcher, OutboxRecord, OutboxStore

### Community 100 - "Reservation"
Cohesion: 0.11
Nodes (8): Override, Reservation, ReservationStatus, HELD, REJECTED, RELEASED, VOIDED, ReservationTest

### Community 101 - "FraudTopology.java"
Cohesion: 0.18
Nodes (10): AuditEventMessage, Topics, FraudDetectionService, FraudTopology, FraudTransformerSupplier, AuditEventMessage, org.apache.kafka.streams.kstream.KStream, org.apache.kafka.streams.kstream.TransformerSupplier (+2 more)

### Community 103 - "KafkaActivityLogPublisher"
Cohesion: 0.19
Nodes (4): KafkaActivityLogPublisher, KafkaActivityLogPublisher, ActivityLogFacade, Override

### Community 104 - "OrderPlacedPort"
Cohesion: 0.20
Nodes (5): OrderPlacedListener, OrderPlacedMessage, OrderPlacedPort, Override, OrderPlacedFacade

### Community 105 - "InventoryEventPort"
Cohesion: 0.17
Nodes (5): InventoryEventListener, InventoryEventMessage, InventoryEventPort, UnknownInventoryEventException, InventoryEventFacade

### Community 106 - "PaymentEventPort"
Cohesion: 0.21
Nodes (4): PaymentEventListener, PaymentEventMessage, PaymentEventPort, UnknownPaymentEventException

### Community 109 - "InventoryEventOutboxTranslator"
Cohesion: 0.28
Nodes (4): InventoryEventMessage, InventoryEventOutboxTranslator, InventoryEventMessage, Override

### Community 110 - "OutboxRecord"
Cohesion: 0.24
Nodes (4): Override, OutboxRecord, FakeStore, Override

### Community 112 - ".place"
Cohesion: 0.32
Nodes (3): PlaceOrderRequest, PlaceOrderResponse, org.springframework.web.bind.annotation.PostMapping

### Community 113 - "OccurredAtTimestampExtractor.java"
Cohesion: 0.47
Nodes (4): Override, OccurredAtTimestampExtractor, org.apache.kafka.clients.consumer.ConsumerRecord, org.apache.kafka.streams.processor.TimestampExtractor

### Community 114 - "AuditLevel"
Cohesion: 0.40
Nodes (4): AuditLevel, ERROR, INFO, WARN

### Community 115 - "FraudDetectedMessage"
Cohesion: 0.19
Nodes (6): FraudDetectedMessage, FraudulentOrder, FraudEventListener, FraudEventPort, FraudEventFacade, Override

### Community 116 - "GatewayResult"
Cohesion: 0.21
Nodes (4): DeterministicPaymentGateway, Override, DeterministicPaymentGatewayTest, GatewayResult

### Community 125 - "FraudStreamsConfiguration.java"
Cohesion: 0.47
Nodes (4): FraudStreamsConfiguration, KafkaStreamsConfiguration, org.springframework.kafka.annotation.EnableKafkaStreams, org.springframework.kafka.config.KafkaStreamsConfiguration

## Knowledge Gaps
- **109 isolated node(s):** `inventory-service-adapters`, `inventory-service-application`, `INFO`, `WARN`, `ERROR` (+104 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **61 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `DeadLetterProperties` connect `DeadLetterProperties` to `org.springframework.context.annotation.Bean`, `DeadLetterProperties`?**
  _High betweenness centrality (0.042) - this node is a cross-community bridge._
- **Why does `DeadLetterProperties` connect `DeadLetterProperties` to `org.springframework.context.annotation.Bean`, `org.springframework.boot.autoconfigure.condition.ConditionalOnProperty`, `Retry`, `DeadLetterProperties`?**
  _High betweenness centrality (0.038) - this node is a cross-community bridge._
- **Why does `Quantity` connect `Quantity` to `SqliteInventoryRepository`, `SqliteOrderRepository`, `Reservation`, `.place`, `Order`, `Sku`, `Product`, `.decide`, `org.junit.jupiter.api.Test`?**
  _High betweenness centrality (0.035) - this node is a cross-community bridge._
- **Are the 12 inferred relationships involving `Quantity` (e.g. with `.findReservation()` and `.readProduct()`) actually correct?**
  _`Quantity` has 12 INFERRED edges - model-reasoned connections that need verification._
- **What connects `inventory-service-adapters`, `inventory-service-application`, `INFO` to the rest of the system?**
  _109 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `ProductSalesRecord` be split into smaller, more focused modules?**
  _Cohesion score 0.08771929824561403 - nodes in this community are weakly interconnected._
- **Should `ProductId` be split into smaller, more focused modules?**
  _Cohesion score 0.13588850174216027 - nodes in this community are weakly interconnected._