# Graph Report - training_kafka  (2026-09-02)

## Corpus Check
- 156 files · ~30,612 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 998 nodes · 2701 edges · 61 communities (38 shown, 23 thin omitted)
- Extraction: 89% EXTRACTED · 11% INFERRED · 0% AMBIGUOUS · INFERRED: 286 edges (avg confidence: 0.82)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `6cc62ccd`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- OrderId
- ProductSalesRecord
- CustomerOrderPattern
- Violation
- DeadLetterProperties
- DuckDbAuditRepository.java
- metrics-consumer (App B)
- ProductId
- org.springframework.context.annotation.Bean
- .place
- org.junit.jupiter.api.DisplayName
- Order
- OrderPlacedHandlerTest
- DeadLetterProperties
- orders-load.js
- package.json
- org.slf4j.Logger
- org.springframework.boot.autoconfigure.SpringBootApplication
- AuditServiceWiring.java
- .toDomainEvent
- SqliteRepositoryException
- dev.joaolaureano.trainingkafka:training-kafka
- AuditFilter
- org.junit.jupiter.api.Test
- AuditController.java
- AuditEvent
- metrics-consumer
- metrics-consumer-adapters
- metrics-consumer-application
- metrics-consumer-domain
- order-service
- order-service-adapters
- order-service-application
- order-service-domain
- org.junit.jupiter.params.ParameterizedTest
- KafkaActivityLogPublisher
- Topics
- audit-service
- audit-service-adapters
- DomainEvent
- OrderServiceWiring.java
- audit-service-application
- org.apache.kafka.clients.admin.NewTopic
- ApplicationName
- audit-service-bootstrap
- audit-service-domain
- CustomerOrderPatternTest
- ActivityLog
- StdoutAuditRepository
- DomainEvent
- .handle
- AnalyticsWiring.java
- AuditFilterTest
- Anticorruption Layer de tradução na fronteira
- AuditLevel
- metrics-consumer-bootstrap
- order-service-bootstrap
- Money
- AuditEventListener
- AnalyticsWiring
- Topics

## God Nodes (most connected - your core abstractions)
1. `ProductSalesRecord` - 48 edges
2. `AuditEvent` - 43 edges
3. `ProductId` - 41 edges
4. `CustomerOrderPattern` - 40 edges
5. `ProductSalesRepository` - 34 edges
6. `AuditFilter` - 33 edges
7. `AuditRepository` - 33 edges
8. `ApplicationName` - 32 edges
9. `OrderLedgerRepository` - 31 edges
10. `TimeRange` - 30 edges

## Surprising Connections (you probably didn't know these)
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` --conceptually_related_to--> `order-service (App A)`  [INFERRED]
  docker-compose.yml → README.md
- `AuditEvent` --references--> `ApplicationName`  [EXTRACTED]
  log-aggregator/log-aggregator-domain/src/main/java/dev/joaolaureano/trainingkafka/audit/domain/model/AuditEvent.java → audit-service/audit-service-domain/src/main/java/dev/joaolaureano/trainingkafka/audit/domain/model/ApplicationName.java
- `AuditEvent` --references--> `AuditLevel`  [EXTRACTED]
  log-aggregator/log-aggregator-domain/src/main/java/dev/joaolaureano/trainingkafka/audit/domain/model/AuditEvent.java → audit-service/audit-service-domain/src/main/java/dev/joaolaureano/trainingkafka/audit/domain/model/AuditLevel.java
- `training_kafka (laboratório Kafka + DDD)` --references--> `Kafka UI (kafbat/kafka-ui:8090)`  [EXTRACTED]
  README.md → docker-compose.yml
- `AuditFilter` --references--> `ApplicationName`  [EXTRACTED]
  log-aggregator/log-aggregator-domain/src/main/java/dev/joaolaureano/trainingkafka/audit/domain/model/LogFilter.java → audit-service/audit-service-domain/src/main/java/dev/joaolaureano/trainingkafka/audit/domain/model/ApplicationName.java

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Pipeline de eventos A → orders → B → application-logs → C** — readme_order_service, readme_orders_topic, readme_metrics_consumer, readme_application_logs_topic, readme_log_aggregator, docker_compose_kafka_broker [EXTRACTED 1.00]
- **Modelo de domínio do App B (3 raízes + VO derivado + handler)** — readme_productsalesrecord, readme_customerorderpattern, readme_orderrecord, readme_revenuewindow, readme_orderplacedhandler [EXTRACTED 1.00]

## Communities (61 total, 23 thin omitted)

### Community 0 - "OrderId"
Cohesion: 0.16
Nodes (6): PlaceOrderPort, PlaceOrderUseCase, Override, PlaceOrderFacade, Override, OrderId

### Community 1 - "ProductSalesRecord"
Cohesion: 0.07
Nodes (20): Override, ProductId, InMemoryProductSalesRepository, Override, Money, Override, ProductId, ProductId (+12 more)

### Community 2 - "CustomerOrderPattern"
Cohesion: 0.07
Nodes (20): DuckDbCustomerPatternRepository, Override, InMemoryCustomerPatternRepository, Override, Override, SqliteCustomerPatternRepository, CustomerPatternRepositoryContractTest, CustomerId (+12 more)

### Community 3 - "Violation"
Cohesion: 0.09
Nodes (17): AuditEventMessage, AuditEventTranslator, AuditEvent, AuditExceptionHandler, InvalidAuditException, java.time.format.DateTimeParseException, InvalidAuditException, AnalyticsExceptionHandler (+9 more)

### Community 4 - "DeadLetterProperties"
Cohesion: 0.08
Nodes (3): DeadLetterProperties, Retry, org.springframework.boot.context.properties.ConfigurationProperties

### Community 5 - "DuckDbAuditRepository.java"
Cohesion: 0.14
Nodes (9): DuckDbAuditRepository, AuditEvent, Override, AuditEvent, ObjectMapper, Override, JsonlFileAuditRepository, StoredLine (+1 more)

### Community 6 - "metrics-consumer (App B)"
Cohesion: 0.08
Nodes (38): KAFKA_AUTO_CREATE_TOPICS_ENABLE=false, Listeners PLAINTEXT (9092) e PLAINTEXT_HOST (9094), Broker Kafka (apache/kafka:4.1.2), Kafka UI (kafbat/kafka-ui:8090), KRaft: nó único broker+controller, Fatores de replicação 1 para nó único, Tópico Kafka "application-logs", Gargalo: commit por mensagem no consumidor (+30 more)

### Community 7 - "ProductId"
Cohesion: 0.06
Nodes (27): java.sql.Connection, java.sql.PreparedStatement, java.sql.ResultSet, DuckDbOrderLedgerRepository, Override, DuckDbProductSalesRepository, InMemoryOrderLedgerRepository, Override (+19 more)

### Community 8 - "org.springframework.context.annotation.Bean"
Cohesion: 0.16
Nodes (11): AuditPersistenceConfiguration, DuckDbPersistence, JsonlPersistence, StdoutPersistence, DuckDbPersistence, InMemoryPersistence, PersistenceConfiguration, SqlitePersistence (+3 more)

### Community 9 - ".place"
Cohesion: 0.32
Nodes (3): PlaceOrderRequest, PlaceOrderResponse, org.springframework.web.bind.annotation.PostMapping

### Community 10 - "org.junit.jupiter.api.DisplayName"
Cohesion: 0.15
Nodes (7): TimeRange, RevenueWindowTest, MoneyRules, OrderTest, WhenInvalid, WhenValid, org.junit.jupiter.api.DisplayName

### Community 11 - "Order"
Cohesion: 0.06
Nodes (17): OrderPlacedMessage, OrderPlaced, CustomerId, Override, Override, Money, CustomerId, Money (+9 more)

### Community 12 - "OrderPlacedHandlerTest"
Cohesion: 0.20
Nodes (6): FakeLedger, OrderPlaced, ProductId, TimeRange, OrderPlacedHandlerTest, org.junit.jupiter.api.BeforeEach

### Community 14 - "orders-load.js"
Cohesion: 0.17
Nodes (13): acceptanceRate, CATALOG, options, orderLatency, ordersAccepted, ordersRejected, placeNormalOrder(), placeOrder() (+5 more)

### Community 15 - "package.json"
Cohesion: 0.14
Nodes (13): esbuild, @faker-js/faker, description, devDependencies, esbuild, @faker-js/faker, name, private (+5 more)

### Community 16 - "org.slf4j.Logger"
Cohesion: 0.27
Nodes (9): KafkaErrorHandlingConfig, KafkaErrorHandlingConfig, org.slf4j.Logger, org.springframework.boot.autoconfigure.condition.ConditionalOnProperty, org.springframework.boot.autoconfigure.kafka.KafkaProperties, org.springframework.boot.context.properties.EnableConfigurationProperties, org.springframework.kafka.core.KafkaOperations, org.springframework.kafka.listener.CommonErrorHandler (+1 more)

### Community 17 - "org.springframework.boot.autoconfigure.SpringBootApplication"
Cohesion: 0.27
Nodes (4): AuditServiceBootstrap, MetricsConsumerBootstrap, OrderServiceBootstrap, org.springframework.boot.autoconfigure.SpringBootApplication

### Community 18 - "AuditServiceWiring.java"
Cohesion: 0.16
Nodes (8): IngestAuditPort, AuditQueryPort, AuditQueryService, IngestAuditService, AuditServiceWiring, AuditQueryFacade, Override, IngestAuditFacade

### Community 19 - ".toDomainEvent"
Cohesion: 0.21
Nodes (5): OrderPlacedMessage, OrderPlaced, Quantity, OrderPlacedTranslator, InvalidValueException

### Community 22 - "AuditFilter"
Cohesion: 0.08
Nodes (12): ApplicationName, Override, AuditFilter, AuditLevel, DEBUG, ERROR, INFO, WARN (+4 more)

### Community 23 - "org.junit.jupiter.api.Test"
Cohesion: 0.16
Nodes (5): KafkaErrorHandlingConfigTest, KafkaErrorHandlingConfigTest, ProductSalesRecordTest, org.junit.jupiter.api.Test, org.springframework.boot.test.context.runner.ApplicationContextRunner

### Community 24 - "AuditController.java"
Cohesion: 0.20
Nodes (7): AuditController, AuditEventView, TimeRange, OrderController, org.springframework.web.bind.annotation.GetMapping, org.springframework.web.bind.annotation.RequestMapping, org.springframework.web.bind.annotation.RestController

### Community 25 - "AuditEvent"
Cohesion: 0.18
Nodes (4): Override, AuditEvent, AuditEvent, AuditRepository

### Community 34 - "org.junit.jupiter.params.ParameterizedTest"
Cohesion: 0.29
Nodes (7): AuditRepositoryContractTest, AuditEvent, AuditLevel, Connection, AuditRepository, org.junit.jupiter.params.ParameterizedTest, org.junit.jupiter.params.provider.MethodSource

### Community 35 - "KafkaActivityLogPublisher"
Cohesion: 0.19
Nodes (4): AuditEventMessage, KafkaActivityLogPublisher, ActivityLogFacade, Override

### Community 39 - "DomainEvent"
Cohesion: 0.16
Nodes (7): AuditEventMessage, Override, KafkaDomainEventPublisher, RecordingPublisher, DomainEvent, DomainEventPublisher, org.springframework.kafka.core.KafkaTemplate

### Community 40 - "OrderServiceWiring.java"
Cohesion: 0.27
Nodes (5): KafkaOrderEventPublisher, PlaceOrderService, ActivityLogPublisher, OrderServiceWiring, OrderEventPublisher

### Community 42 - "org.apache.kafka.clients.admin.NewTopic"
Cohesion: 0.28
Nodes (3): KafkaTopicsConfig, Topics, org.apache.kafka.clients.admin.NewTopic

### Community 43 - "ApplicationName"
Cohesion: 0.22
Nodes (4): ApplicationName, Override, AuditFilterTest, AuditEvent

### Community 46 - "CustomerOrderPatternTest"
Cohesion: 0.24
Nodes (6): BurstBehaviour, CustomerOrderPatternTest, Reconstitution, SuspicionDecision, WindowPruning, org.junit.jupiter.api.Nested

### Community 47 - "ActivityLog"
Cohesion: 0.20
Nodes (5): ActivityLog, AuditLevel, ERROR, INFO, WARN

### Community 48 - "StdoutAuditRepository"
Cohesion: 0.23
Nodes (4): Override, StdoutAuditRepository, StdoutAuditRepositoryTest, org.junit.jupiter.api.AfterEach

### Community 49 - "DomainEvent"
Cohesion: 0.16
Nodes (5): Override, Override, RecordingEventPublisher, RecordingLogPublisher, DomainEvent

### Community 50 - ".handle"
Cohesion: 0.44
Nodes (3): PlaceOrderCommand, Override, PlaceOrderServiceTest

### Community 51 - "AnalyticsWiring.java"
Cohesion: 0.29
Nodes (5): OrderPlacedPort, OrderPlacedHandler, Override, OrderPlacedFacade, OrderPlaced

### Community 53 - "Anticorruption Layer de tradução na fronteira"
Cohesion: 0.50
Nodes (5): Anticorruption Layer de tradução na fronteira, Contratos de evento duplicados por serviço, LogEntryTranslator, spring.json.add.type.headers=false, OrderPlacedTranslator

### Community 54 - "AuditLevel"
Cohesion: 0.28
Nodes (7): AuditLevel, DEBUG, ERROR, INFO, WARN, isAtLeast(), parse()

### Community 58 - "AuditEventListener"
Cohesion: 0.36
Nodes (4): AuditEventListener, OrderListener, org.springframework.kafka.annotation.KafkaListener, org.springframework.stereotype.Component

## Knowledge Gaps
- **46 isolated node(s):** `audit-service-adapters`, `audit-service-application`, `audit-service-bootstrap`, `audit-service-domain`, `DEBUG` (+41 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **23 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `AuditEvent` connect `AuditEvent` to `Violation`, `DuckDbAuditRepository.java`, `ApplicationName`, `StdoutAuditRepository`, `AuditServiceWiring.java`, `AuditFilterTest`, `AuditLevel`, `AuditFilter`, `AuditController.java`?**
  _High betweenness centrality (0.047) - this node is a cross-community bridge._
- **Why does `ApplicationName` connect `AuditFilter` to `org.junit.jupiter.params.ParameterizedTest`, `Violation`, `DuckDbAuditRepository.java`, `StdoutAuditRepository`, `AuditFilterTest`, `AuditLevel`, `AuditController.java`, `AuditEvent`?**
  _High betweenness centrality (0.044) - this node is a cross-community bridge._
- **Why does `DeadLetterProperties` connect `DeadLetterProperties` to `org.slf4j.Logger`?**
  _High betweenness centrality (0.039) - this node is a cross-community bridge._
- **Are the 3 inferred relationships involving `AuditEvent` (e.g. with `.omitsEmptyContext()` and `.printsTheEssentials()`) actually correct?**
  _`AuditEvent` has 3 INFERRED edges - model-reasoned connections that need verification._
- **What connects `audit-service-adapters`, `audit-service-application`, `audit-service-bootstrap` to the rest of the system?**
  _46 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `ProductSalesRecord` be split into smaller, more focused modules?**
  _Cohesion score 0.07256571640133284 - nodes in this community are weakly interconnected._
- **Should `CustomerOrderPattern` be split into smaller, more focused modules?**
  _Cohesion score 0.0695970695970696 - nodes in this community are weakly interconnected._