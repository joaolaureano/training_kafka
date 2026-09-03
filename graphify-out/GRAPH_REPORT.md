# Graph Report - training_kafka  (2026-09-03)

## Corpus Check
- 240 files · ~48,081 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1538 nodes · 4002 edges · 93 communities (56 shown, 37 thin omitted)
- Extraction: 88% EXTRACTED · 12% INFERRED · 0% AMBIGUOUS · INFERRED: 463 edges (avg confidence: 0.81)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `706a7151`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- AuditController.java
- ProductSalesRecord
- .request
- ProductId
- DeadLetterProperties
- org.junit.jupiter.api.Test
- metrics-consumer (App B)
- MetricsQueryService
- org.springframework.context.annotation.Configuration
- Violation
- OrderId
- Order
- FraudTopology.java
- org.springframework.context.annotation.Bean
- orders-load.js
- package.json
- org.slf4j.Logger
- org.springframework.boot.autoconfigure.SpringBootApplication
- AuditServiceWiring.java
- AuditEvent
- SqliteRepositoryException
- dev.joaolaureano.trainingkafka:training-kafka
- Payment
- DomainEvent
- OutboxRelay
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
- AuditLevel
- OutboxRelay
- run.sh
- .validOrder
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
- OutboxRelayTest
- ProcessOrderPaymentTest
- DuckDbAuditRepository
- DeadLetterProperties
- DeadLetterProperties
- JsonlFileAuditRepository
- ActivityLog
- .place
- .handle
- DomainEvent
- OutboxStore
- InvalidAuditException
- ApplicationName
- Money
- OutboxRelayTest
- Retry
- UnknownOrderException
- payment-service
- payment-service-adapters
- payment-service-application
- payment-service-bootstrap
- payment-service-domain
- FraudStreamsConfiguration.java
- KafkaActivityLogPublisher
- Topics
- java.sql.Connection
- ActivityLog
- FraudEventConsumerConfig.java
- com.fasterxml.jackson.databind.ObjectMapper

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
- `KafkaOutboxDispatcher` --implements--> `OutboxDispatcher`  [EXTRACTED]
  order-service/order-service-adapters/src/main/java/dev/joaolaureano/trainingkafka/orders/adapters/messaging/KafkaOutboxDispatcher.java → order-service/order-service-adapters/src/main/java/dev/joaolaureano/trainingkafka/orders/adapters/persistence/OutboxDispatcher.java
- `OutboxRelayScheduler` --references--> `OutboxRelay`  [EXTRACTED]
  order-service/order-service-adapters/src/main/java/dev/joaolaureano/trainingkafka/orders/adapters/messaging/OutboxRelayScheduler.java → order-service/order-service-adapters/src/main/java/dev/joaolaureano/trainingkafka/orders/adapters/messaging/OutboxRelay.java

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Pipeline de eventos A → orders → B → application-logs → C** — readme_order_service, readme_orders_topic, readme_metrics_consumer, readme_application_logs_topic, readme_log_aggregator, docker_compose_kafka_broker [EXTRACTED 1.00]
- **Modelo de domínio do App B (3 raízes + VO derivado + handler)** — readme_productsalesrecord, readme_customerorderpattern, readme_orderrecord, readme_revenuewindow, readme_orderplacedhandler [EXTRACTED 1.00]

## Communities (93 total, 37 thin omitted)

### Community 0 - "AuditController.java"
Cohesion: 0.08
Nodes (19): AuditController, AuditEventView, MetricsController, ProductSalesView, RevenueView, MetricsQueryPort, FindOrderPort, OrderController (+11 more)

### Community 1 - "ProductSalesRecord"
Cohesion: 0.09
Nodes (15): Override, ProductId, InMemoryProductSalesRepository, Override, Money, Override, ProductId, FakeProductSales (+7 more)

### Community 2 - ".request"
Cohesion: 0.15
Nodes (5): OutboxRecord, Override, SqlitePaymentRepository, SqlitePaymentRepositoryTest, PaymentTest

### Community 3 - "ProductId"
Cohesion: 0.11
Nodes (11): Override, InMemoryOrderLedgerRepository, Override, OrderLedgerRepositoryContractTest, OrderRecord, Override, ProductId, RevenueWindow (+3 more)

### Community 4 - "DeadLetterProperties"
Cohesion: 0.12
Nodes (3): DeadLetterProperties, FraudProperties, org.springframework.boot.context.properties.ConfigurationProperties

### Community 5 - "org.junit.jupiter.api.Test"
Cohesion: 0.09
Nodes (12): KafkaErrorHandlingConfigTest, KafkaErrorHandlingConfigTest, TimeRange, Override, FindOrderServiceTest, MoneyRules, OrderTest, WhenInvalid (+4 more)

### Community 6 - "metrics-consumer (App B)"
Cohesion: 0.08
Nodes (38): KAFKA_AUTO_CREATE_TOPICS_ENABLE=false, Listeners PLAINTEXT (9092) e PLAINTEXT_HOST (9094), Broker Kafka (apache/kafka:4.1.2), Kafka UI (kafbat/kafka-ui:8090), KRaft: nó único broker+controller, Fatores de replicação 1 para nó único, Tópico Kafka "application-logs", Gargalo: commit por mensagem no consumidor (+30 more)

### Community 7 - "MetricsQueryService"
Cohesion: 0.22
Nodes (4): MetricsQueryService, AnalyticsWiring, Override, MetricsQueryFacade

### Community 8 - "org.springframework.context.annotation.Configuration"
Cohesion: 0.14
Nodes (10): AuditPersistenceConfiguration, DuckDbPersistence, JsonlPersistence, StdoutPersistence, DuckDbPersistence, InMemoryPersistence, PersistenceConfiguration, SqlitePersistence (+2 more)

### Community 9 - "Violation"
Cohesion: 0.08
Nodes (18): AuditExceptionHandler, java.time.format.DateTimeParseException, OrderPlacedMessage, OrderPlaced, Quantity, OrderPlacedTranslator, AnalyticsExceptionHandler, InvalidValueException (+10 more)

### Community 10 - "OrderId"
Cohesion: 0.09
Nodes (11): FindOrderService, PlaceOrderUseCase, FakeRepository, Override, FakeRepository, Override, Override, PlaceOrderFacade (+3 more)

### Community 11 - "Order"
Cohesion: 0.10
Nodes (11): OrderView, CustomerId, Override, Override, Money, Override, Order, Override (+3 more)

### Community 12 - "FraudTopology.java"
Cohesion: 0.05
Nodes (37): AuditEventMessage, FraudDetectedMessage, FraudulentOrder, OrderPlacedMessage, Topics, CustomerFraudState, Override, OccurredAtTimestampExtractor (+29 more)

### Community 13 - "org.springframework.context.annotation.Bean"
Cohesion: 0.26
Nodes (4): KafkaTopicsConfig, org.apache.kafka.clients.admin.NewTopic, org.springframework.boot.autoconfigure.condition.ConditionalOnProperty, org.springframework.context.annotation.Bean

### Community 14 - "orders-load.js"
Cohesion: 0.07
Nodes (36): acceptanceRate, APPROVAL_LIMIT, awaitStatus(), CATALOG, COMPENSATION_METRICS, COMPENSATION_TIMEOUT_MS, compensationDuration, compensationSettled (+28 more)

### Community 15 - "package.json"
Cohesion: 0.14
Nodes (13): esbuild, @faker-js/faker, description, devDependencies, esbuild, @faker-js/faker, name, private (+5 more)

### Community 16 - "org.slf4j.Logger"
Cohesion: 0.26
Nodes (9): KafkaErrorHandlingConfig, org.slf4j.Logger, org.springframework.boot.autoconfigure.kafka.KafkaProperties, org.springframework.boot.context.properties.EnableConfigurationProperties, org.springframework.kafka.core.KafkaOperations, org.springframework.kafka.listener.CommonErrorHandler, org.springframework.kafka.listener.DefaultErrorHandler, org.springframework.util.backoff.BackOff (+1 more)

### Community 17 - "org.springframework.boot.autoconfigure.SpringBootApplication"
Cohesion: 0.18
Nodes (7): AuditServiceBootstrap, FraudServiceBootstrap, MetricsConsumerBootstrap, OrderServiceBootstrap, org.springframework.boot.autoconfigure.SpringBootApplication, org.springframework.scheduling.annotation.EnableScheduling, PaymentServiceBootstrap

### Community 18 - "AuditServiceWiring.java"
Cohesion: 0.16
Nodes (7): IngestAuditPort, AuditQueryPort, AuditQueryService, IngestAuditService, AuditServiceWiring, AuditQueryFacade, IngestAuditFacade

### Community 19 - "AuditEvent"
Cohesion: 0.19
Nodes (5): Override, StdoutAuditRepository, StdoutAuditRepositoryTest, Override, AuditEvent

### Community 22 - "Payment"
Cohesion: 0.12
Nodes (10): Override, FunctionalInterface, LogFactory, FakeRepository, Override, FakeRepository, Override, Payment (+2 more)

### Community 23 - "DomainEvent"
Cohesion: 0.09
Nodes (12): PaymentEventMessage, OutboxRecord, Override, PaymentEventMessage, PaymentEventOutboxTranslator, OutboxRecord, OutboxTranslator, DomainEvent (+4 more)

### Community 24 - "OutboxRelay"
Cohesion: 0.24
Nodes (6): dev.joaolaureano.trainingkafka.payment.adapters.persistence.OutboxStore, OutboxRelay, FunctionalInterface, OutboxRecord, OutboxDispatcher, OutboxRelay

### Community 25 - "AuditRepository"
Cohesion: 0.33
Nodes (6): AuditRepositoryContractTest, AuditLevel, Connection, AuditRepository, org.junit.jupiter.params.ParameterizedTest, org.junit.jupiter.params.provider.MethodSource

### Community 34 - "SqliteOrderRepository"
Cohesion: 0.20
Nodes (5): OutboxStore, OutboxRecord, Override, SqliteOrderRepository, SqliteOrderRepositoryTest

### Community 35 - "AuditFilter"
Cohesion: 0.17
Nodes (3): Override, AuditFilter, TimeRange

### Community 36 - "OrderPlaced"
Cohesion: 0.10
Nodes (14): OrderListener, OrderPlacedPort, OrderPlacedHandler, FakeLedger, OrderPlaced, ProductId, OrderPlacedHandlerTest, Override (+6 more)

### Community 39 - "org.springframework.stereotype.Component"
Cohesion: 0.06
Nodes (21): AuditEventListener, OutboxRelayScheduler, PaymentEventListener, PaymentEventMessage, PaymentEventPort, UnknownPaymentEventException, org.springframework.kafka.annotation.KafkaListener, org.springframework.scheduling.annotation.Scheduled (+13 more)

### Community 46 - "ApplyPaymentResult"
Cohesion: 0.23
Nodes (4): ApplyPaymentResult, ApplyPaymentResultTest, Override, PaymentEventFacade

### Community 47 - "AuditLevel"
Cohesion: 0.32
Nodes (7): AuditLevel, DEBUG, ERROR, INFO, WARN, isAtLeast(), parse()

### Community 48 - "OutboxRelay"
Cohesion: 0.24
Nodes (6): dev.joaolaureano.trainingkafka.orders.adapters.persistence.OutboxStore, OutboxRelay, FunctionalInterface, OutboxRecord, OutboxDispatcher, OutboxRelay

### Community 49 - "run.sh"
Cohesion: 0.21
Nodes (15): await_http(), await_log(), cleanup(), die(), log(), LOG_DIR, REPO_ROOT, require() (+7 more)

### Community 51 - "ProductSalesRepository"
Cohesion: 0.25
Nodes (5): ProductId, ProductSalesRepositoryContractTest, Override, Quantity, ProductSalesRepository

### Community 52 - ".handle"
Cohesion: 0.32
Nodes (3): FraudulentOrder, CompensateFraudulentOrdersTest, FraudulentOrder

### Community 53 - "Anticorruption Layer de tradução na fronteira"
Cohesion: 0.50
Nodes (5): Anticorruption Layer de tradução na fronteira, Contratos de evento duplicados por serviço, LogEntryTranslator, spring.json.add.type.headers=false, OrderPlacedTranslator

### Community 60 - "PaymentWiring.java"
Cohesion: 0.12
Nodes (15): CompensateFraudulentOrders, dev.joaolaureano.trainingkafka.payment.adapters.messaging.FraudEventPort, dev.joaolaureano.trainingkafka.payment.adapters.messaging.KafkaActivityLogPublisher, dev.joaolaureano.trainingkafka.payment.adapters.messaging.OrderPlacedPort, dev.joaolaureano.trainingkafka.payment.adapters.persistence.OutboxTranslator, dev.joaolaureano.trainingkafka.payment.adapters.persistence.SqlitePaymentRepository, dev.joaolaureano.trainingkafka.payment.application.CompensateFraudulentOrders, dev.joaolaureano.trainingkafka.payment.application.port.ActivityLogPublisher (+7 more)

### Community 62 - "OrderServiceWiring.java"
Cohesion: 0.12
Nodes (15): ApplyPaymentResult, dev.joaolaureano.trainingkafka.orders.adapters.messaging.KafkaActivityLogPublisher, dev.joaolaureano.trainingkafka.orders.adapters.messaging.PaymentEventPort, dev.joaolaureano.trainingkafka.orders.adapters.persistence.OutboxTranslator, dev.joaolaureano.trainingkafka.orders.adapters.persistence.SqliteOrderRepository, dev.joaolaureano.trainingkafka.orders.adapters.web.FindOrderPort, dev.joaolaureano.trainingkafka.orders.adapters.web.PlaceOrderPort, dev.joaolaureano.trainingkafka.orders.application.ApplyPaymentResult (+7 more)

### Community 63 - "DeadLetterProperties"
Cohesion: 0.10
Nodes (3): DeadLetterProperties, Retry, KafkaErrorHandlingConfig

### Community 64 - "OutboxRelayTest"
Cohesion: 0.25
Nodes (6): dev.joaolaureano.trainingkafka.payment.adapters.persistence.OutboxRecord, Override, FakeStore, OutboxRecord, Override, OutboxRelayTest

### Community 65 - "ProcessOrderPaymentTest"
Cohesion: 0.16
Nodes (6): DeterministicPaymentGateway, DeterministicPaymentGatewayTest, RecordingGateway, ProcessOrderPaymentTest, RecordingGateway, PaymentGateway

### Community 67 - "DeadLetterProperties"
Cohesion: 0.10
Nodes (4): DeadLetterProperties, Retry, DefaultErrorHandler, KafkaErrorHandlingConfig

### Community 68 - "DeadLetterProperties"
Cohesion: 0.10
Nodes (4): DeadLetterProperties, Retry, DefaultErrorHandler, KafkaErrorHandlingConfig

### Community 69 - "JsonlFileAuditRepository"
Cohesion: 0.33
Nodes (4): ObjectMapper, Override, JsonlFileAuditRepository, StoredLine

### Community 70 - "ActivityLog"
Cohesion: 0.10
Nodes (9): AuditEventMessage, KafkaActivityLogPublisher, ActivityLog, AuditLevel, ERROR, INFO, WARN, ActivityLogFacade (+1 more)

### Community 71 - ".place"
Cohesion: 0.40
Nodes (4): CustomerId, Money, ProductId, Quantity

### Community 72 - ".handle"
Cohesion: 0.18
Nodes (8): PlaceOrderCommand, Override, PlaceOrderService, ActivityLogPublisher, Override, PlaceOrderServiceTest, RecordingLogPublisher, RecordingRepository

### Community 73 - "DomainEvent"
Cohesion: 0.09
Nodes (14): OutboxRecord, Override, OrderEventOutboxTranslator, OrderPlacedMessage, OutboxRecord, OutboxTranslator, DomainEvent, OrderPlaced (+6 more)

### Community 75 - "InvalidAuditException"
Cohesion: 0.23
Nodes (3): AuditEventMessage, AuditEventTranslator, InvalidAuditException

### Community 76 - "ApplicationName"
Cohesion: 0.25
Nodes (3): ApplicationName, Override, AuditFilterTest

### Community 79 - "OutboxRelayTest"
Cohesion: 0.25
Nodes (6): dev.joaolaureano.trainingkafka.orders.adapters.persistence.OutboxRecord, Override, FakeStore, OutboxRecord, Override, OutboxRelayTest

### Community 88 - "FraudStreamsConfiguration.java"
Cohesion: 0.47
Nodes (4): FraudStreamsConfiguration, KafkaStreamsConfiguration, org.springframework.kafka.annotation.EnableKafkaStreams, org.springframework.kafka.config.KafkaStreamsConfiguration

### Community 89 - "KafkaActivityLogPublisher"
Cohesion: 0.22
Nodes (4): AuditEventMessage, KafkaActivityLogPublisher, AuditEventMessageContractTest, ActivityLogFacade

### Community 91 - "java.sql.Connection"
Cohesion: 0.12
Nodes (11): java.sql.Connection, java.sql.PreparedStatement, java.sql.ResultSet, DuckDbOrderLedgerRepository, DuckDbProductSalesRepository, MoneyCents, Override, SqliteOrderLedgerRepository (+3 more)

### Community 92 - "ActivityLog"
Cohesion: 0.09
Nodes (17): CompensateFraudulentOrders, ActivityLog, ActivityLogPublisher, AuditLevel, ERROR, INFO, WARN, ProcessOrderPayment (+9 more)

### Community 93 - "FraudEventConsumerConfig.java"
Cohesion: 0.47
Nodes (4): ConcurrentKafkaListenerContainerFactory, dev.joaolaureano.trainingkafka.payment.adapters.messaging.FraudDetectedMessage, org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory, FraudEventConsumerConfig

### Community 95 - "com.fasterxml.jackson.databind.ObjectMapper"
Cohesion: 0.47
Nodes (4): com.fasterxml.jackson.databind.ObjectMapper, KafkaOutboxDispatcher, org.springframework.kafka.core.KafkaTemplate, KafkaOutboxDispatcher

## Knowledge Gaps
- **88 isolated node(s):** `SCRIPT_DIR`, `REPO_ROOT`, `LOG_DIR`, `VERSION`, `SERVICES` (+83 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **37 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `ProductSalesRecord` connect `ProductSalesRecord` to `AuditController.java`, `ProductId`, `MetricsQueryService`, `Money`, `ProductSalesRepository`, `java.sql.Connection`?**
  _High betweenness centrality (0.050) - this node is a cross-community bridge._
- **Why does `DeadLetterProperties` connect `DeadLetterProperties` to `DeadLetterProperties`, `org.springframework.context.annotation.Bean`?**
  _High betweenness centrality (0.039) - this node is a cross-community bridge._
- **Why does `DeadLetterProperties` connect `DeadLetterProperties` to `DeadLetterProperties`, `org.springframework.context.annotation.Bean`?**
  _High betweenness centrality (0.039) - this node is a cross-community bridge._
- **What connects `SCRIPT_DIR`, `REPO_ROOT`, `LOG_DIR` to the rest of the system?**
  _88 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `AuditController.java` be split into smaller, more focused modules?**
  _Cohesion score 0.07928118393234672 - nodes in this community are weakly interconnected._
- **Should `ProductSalesRecord` be split into smaller, more focused modules?**
  _Cohesion score 0.09433962264150944 - nodes in this community are weakly interconnected._
- **Should `ProductId` be split into smaller, more focused modules?**
  _Cohesion score 0.10857142857142857 - nodes in this community are weakly interconnected._