# Graph Report - training_kafka  (2026-09-03)

## Corpus Check
- 155 files · ~27,672 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 920 nodes · 2343 edges · 62 communities (33 shown, 29 thin omitted)
- Extraction: 89% EXTRACTED · 11% INFERRED · 0% AMBIGUOUS · INFERRED: 259 edges (avg confidence: 0.82)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `79988594`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- OrderId
- ProductSalesRecord
- OrderPlaced
- TestRepositories
- DeadLetterProperties
- ApplicationName
- metrics-consumer (App B)
- TimeRange
- org.springframework.context.annotation.Bean
- Violation
- org.junit.jupiter.api.Test
- OrderPlaced
- FraudTopology.java
- DeadLetterProperties
- orders-load.js
- package.json
- audit/adapters/config/KafkaErrorHandlingConfig.java
- org.springframework.boot.autoconfigure.SpringBootApplication
- AuditServiceWiring.java
- AuditEvent
- SqliteRepositoryException
- dev.joaolaureano.trainingkafka:training-kafka
- AuditFilter
- Quantity
- MetricsController
- AuditRepository
- metrics-consumer
- metrics-consumer-adapters
- metrics-consumer-application
- metrics-consumer-domain
- order-service
- order-service-adapters
- order-service-application
- order-service-domain
- ProductId
- KafkaErrorHandlingConfigTest
- OrderPlacedHandler
- audit-service
- audit-service-adapters
- .toDomainEvent
- analytics/adapters/messaging/AuditEventMessage.java
- audit-service-application
- Topics
- OrderLedgerRepository
- audit-service-bootstrap
- audit-service-domain
- Retry
- PlaceOrderServiceTest.java
- .place
- Order
- .handle
- OrderRecord
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

## God Nodes (most connected - your core abstractions)
1. `ProductSalesRecord` - 49 edges
2. `ProductId` - 45 edges
3. `AuditEvent` - 43 edges
4. `ProductSalesRepository` - 34 edges
5. `AuditRepository` - 33 edges
6. `TimeRange` - 32 edges
7. `OrderLedgerRepository` - 31 edges
8. `AuditFilter` - 30 edges
9. `ApplicationName` - 27 edges
10. `RevenueWindow` - 27 edges

## Surprising Connections (you probably didn't know these)
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` --conceptually_related_to--> `order-service (App A)`  [INFERRED]
  docker-compose.yml → README.md
- `training_kafka (laboratório Kafka + DDD)` --references--> `Kafka UI (kafbat/kafka-ui:8090)`  [EXTRACTED]
  README.md → docker-compose.yml
- `training_kafka (laboratório Kafka + DDD)` --references--> `Broker Kafka (apache/kafka:4.1.2)`  [EXTRACTED]
  README.md → docker-compose.yml
- `KafkaErrorHandlingConfig` --references--> `DeadLetterProperties`  [EXTRACTED]
  audit-service/audit-service-adapters/src/main/java/dev/joaolaureano/trainingkafka/audit/adapters/config/KafkaErrorHandlingConfig.java → audit-service/audit-service-adapters/src/main/java/dev/joaolaureano/trainingkafka/audit/adapters/config/DeadLetterProperties.java
- `AuditEventListener` --references--> `IngestAuditPort`  [EXTRACTED]
  audit-service/audit-service-adapters/src/main/java/dev/joaolaureano/trainingkafka/audit/adapters/messaging/AuditEventListener.java → audit-service/audit-service-adapters/src/main/java/dev/joaolaureano/trainingkafka/audit/adapters/messaging/IngestAuditPort.java

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Pipeline de eventos A → orders → B → application-logs → C** — readme_order_service, readme_orders_topic, readme_metrics_consumer, readme_application_logs_topic, readme_log_aggregator, docker_compose_kafka_broker [EXTRACTED 1.00]
- **Modelo de domínio do App B (3 raízes + VO derivado + handler)** — readme_productsalesrecord, readme_customerorderpattern, readme_orderrecord, readme_revenuewindow, readme_orderplacedhandler [EXTRACTED 1.00]

## Communities (62 total, 29 thin omitted)

### Community 0 - "OrderId"
Cohesion: 0.06
Nodes (22): AuditEventMessage, KafkaActivityLogPublisher, Override, KafkaOrderEventPublisher, OrderController, PlaceOrderPort, PlaceOrderRequest, PlaceOrderResponse (+14 more)

### Community 1 - "ProductSalesRecord"
Cohesion: 0.06
Nodes (22): java.sql.ResultSet, DuckDbProductSalesRepository, Override, ProductId, InMemoryProductSalesRepository, Override, Money, MoneyCents (+14 more)

### Community 2 - "OrderPlaced"
Cohesion: 0.14
Nodes (9): OrderPlaced, ProductId, OrderPlacedHandlerTest, Override, OrderPlaced, CustomerId, Override, Override (+1 more)

### Community 3 - "TestRepositories"
Cohesion: 0.19
Nodes (3): Connection, TestRepositories, org.junit.jupiter.params.provider.Arguments

### Community 4 - "DeadLetterProperties"
Cohesion: 0.13
Nodes (3): DeadLetterProperties, FraudProperties, org.springframework.boot.context.properties.ConfigurationProperties

### Community 5 - "ApplicationName"
Cohesion: 0.15
Nodes (9): DuckDbAuditRepository, Override, ObjectMapper, Override, JsonlFileAuditRepository, StoredLine, ApplicationName, Override (+1 more)

### Community 6 - "metrics-consumer (App B)"
Cohesion: 0.08
Nodes (38): KAFKA_AUTO_CREATE_TOPICS_ENABLE=false, Listeners PLAINTEXT (9092) e PLAINTEXT_HOST (9094), Broker Kafka (apache/kafka:4.1.2), Kafka UI (kafbat/kafka-ui:8090), KRaft: nó único broker+controller, Fatores de replicação 1 para nó único, Tópico Kafka "application-logs", Gargalo: commit por mensagem no consumidor (+30 more)

### Community 7 - "TimeRange"
Cohesion: 0.14
Nodes (9): java.sql.PreparedStatement, DuckDbOrderLedgerRepository, Override, InMemoryOrderLedgerRepository, Override, Override, SqliteOrderLedgerRepository, RevenueWindow (+1 more)

### Community 8 - "org.springframework.context.annotation.Bean"
Cohesion: 0.12
Nodes (14): AuditPersistenceConfiguration, DuckDbPersistence, JsonlPersistence, StdoutPersistence, java.sql.Connection, DuckDbPersistence, InMemoryPersistence, PersistenceConfiguration (+6 more)

### Community 9 - "Violation"
Cohesion: 0.11
Nodes (13): AuditExceptionHandler, java.time.format.DateTimeParseException, AnalyticsExceptionHandler, ApiError, ApiExceptionHandler, InvalidOrderException, Override, Violation (+5 more)

### Community 10 - "org.junit.jupiter.api.Test"
Cohesion: 0.19
Nodes (6): AuditFilterTest, TimeRange, RevenueWindowTest, MoneyRules, org.junit.jupiter.api.DisplayName, org.junit.jupiter.api.Test

### Community 11 - "OrderPlaced"
Cohesion: 0.09
Nodes (10): OrderPlacedMessage, OrderPlaced, CustomerId, Override, Override, Money, Override, ProductId (+2 more)

### Community 12 - "FraudTopology.java"
Cohesion: 0.06
Nodes (34): AuditEventMessage, OrderPlacedMessage, Topics, CustomerFraudState, Override, OccurredAtTimestampExtractor, FraudDetectionService, FraudTopology (+26 more)

### Community 14 - "orders-load.js"
Cohesion: 0.17
Nodes (13): acceptanceRate, CATALOG, options, orderLatency, ordersAccepted, ordersRejected, placeNormalOrder(), placeOrder() (+5 more)

### Community 15 - "package.json"
Cohesion: 0.14
Nodes (13): esbuild, @faker-js/faker, description, devDependencies, esbuild, @faker-js/faker, name, private (+5 more)

### Community 16 - "audit/adapters/config/KafkaErrorHandlingConfig.java"
Cohesion: 0.23
Nodes (9): KafkaErrorHandlingConfig, Topics, KafkaErrorHandlingConfig, org.slf4j.Logger, org.springframework.boot.autoconfigure.condition.ConditionalOnProperty, org.springframework.boot.autoconfigure.kafka.KafkaProperties, org.springframework.kafka.core.KafkaOperations, org.springframework.kafka.listener.CommonErrorHandler (+1 more)

### Community 17 - "org.springframework.boot.autoconfigure.SpringBootApplication"
Cohesion: 0.21
Nodes (5): AuditServiceBootstrap, FraudServiceBootstrap, MetricsConsumerBootstrap, OrderServiceBootstrap, org.springframework.boot.autoconfigure.SpringBootApplication

### Community 18 - "AuditServiceWiring.java"
Cohesion: 0.15
Nodes (7): IngestAuditPort, AuditQueryService, IngestAuditService, AuditServiceWiring, AuditQueryFacade, IngestAuditFacade, Override

### Community 19 - "AuditEvent"
Cohesion: 0.20
Nodes (5): Override, StdoutAuditRepository, StdoutAuditRepositoryTest, Override, AuditEvent

### Community 22 - "AuditFilter"
Cohesion: 0.10
Nodes (12): AuditController, AuditEventView, AuditQueryPort, AuditFilter, AuditLevel, DEBUG, ERROR, INFO (+4 more)

### Community 23 - "Quantity"
Cohesion: 0.27
Nodes (4): ProductId, ProductSalesRepositoryContractTest, Override, Quantity

### Community 24 - "MetricsController"
Cohesion: 0.26
Nodes (5): MetricsController, ProductSalesView, RevenueView, MetricsQueryPort, org.springframework.web.bind.annotation.GetMapping

### Community 25 - "AuditRepository"
Cohesion: 0.44
Nodes (5): AuditRepositoryContractTest, AuditLevel, AuditRepository, org.junit.jupiter.params.ParameterizedTest, org.junit.jupiter.params.provider.MethodSource

### Community 34 - "ProductId"
Cohesion: 0.21
Nodes (5): MetricsQueryService, Override, MetricsQueryFacade, Override, ProductId

### Community 35 - "KafkaErrorHandlingConfigTest"
Cohesion: 0.18
Nodes (3): KafkaErrorHandlingConfigTest, KafkaErrorHandlingConfigTest, org.springframework.boot.test.context.runner.ApplicationContextRunner

### Community 36 - "OrderPlacedHandler"
Cohesion: 0.27
Nodes (4): OrderPlacedPort, OrderPlacedHandler, AnalyticsWiring, OrderPlacedFacade

### Community 39 - ".toDomainEvent"
Cohesion: 0.09
Nodes (12): AuditEventListener, AuditEventMessage, AuditEventTranslator, InvalidAuditException, OrderListener, OrderPlacedMessage, OrderPlaced, Quantity (+4 more)

### Community 47 - "PlaceOrderServiceTest.java"
Cohesion: 0.25
Nodes (5): AuditLevel, ERROR, INFO, WARN, org.junit.jupiter.api.BeforeEach

### Community 48 - ".place"
Cohesion: 0.24
Nodes (5): CustomerId, Money, ProductId, Quantity, WhenInvalid

### Community 49 - "Order"
Cohesion: 0.15
Nodes (6): DomainEvent, Override, Order, OrderTest, WhenValid, org.junit.jupiter.api.Nested

### Community 50 - ".handle"
Cohesion: 0.16
Nodes (8): PlaceOrderCommand, Override, ActivityLog, Override, PlaceOrderServiceTest, RecordingEventPublisher, RecordingLogPublisher, Override

### Community 52 - "FraudStreamsConfiguration.java"
Cohesion: 0.43
Nodes (5): FraudStreamsConfiguration, KafkaStreamsConfiguration, org.springframework.boot.context.properties.EnableConfigurationProperties, org.springframework.kafka.annotation.EnableKafkaStreams, org.springframework.kafka.config.KafkaStreamsConfiguration

### Community 53 - "Anticorruption Layer de tradução na fronteira"
Cohesion: 0.50
Nodes (5): Anticorruption Layer de tradução na fronteira, Contratos de evento duplicados por serviço, LogEntryTranslator, spring.json.add.type.headers=false, OrderPlacedTranslator

## Knowledge Gaps
- **48 isolated node(s):** `audit-service-adapters`, `audit-service-application`, `audit-service-bootstrap`, `audit-service-domain`, `DEBUG` (+43 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **29 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `AuditEvent` connect `AuditEvent` to `ApplicationName`, `.toDomainEvent`, `org.junit.jupiter.api.Test`, `AuditServiceWiring.java`, `AuditFilter`, `AuditRepository`?**
  _High betweenness centrality (0.053) - this node is a cross-community bridge._
- **Why does `DeadLetterProperties` connect `DeadLetterProperties` to `audit/adapters/config/KafkaErrorHandlingConfig.java`, `org.springframework.context.annotation.Bean`, `DeadLetterProperties`?**
  _High betweenness centrality (0.042) - this node is a cross-community bridge._
- **Why does `DeadLetterProperties` connect `DeadLetterProperties` to `audit/adapters/config/KafkaErrorHandlingConfig.java`, `org.springframework.context.annotation.Bean`, `Retry`?**
  _High betweenness centrality (0.042) - this node is a cross-community bridge._
- **Are the 4 inferred relationships involving `ProductId` (e.g. with `.toDomainEvent()` and `.revenue()`) actually correct?**
  _`ProductId` has 4 INFERRED edges - model-reasoned connections that need verification._
- **What connects `audit-service-adapters`, `audit-service-application`, `audit-service-bootstrap` to the rest of the system?**
  _48 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `OrderId` be split into smaller, more focused modules?**
  _Cohesion score 0.06384180790960452 - nodes in this community are weakly interconnected._
- **Should `ProductSalesRecord` be split into smaller, more focused modules?**
  _Cohesion score 0.06406112253893623 - nodes in this community are weakly interconnected._