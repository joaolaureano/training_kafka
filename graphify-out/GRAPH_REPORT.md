# Graph Report - training_kafka  (2026-09-02)

## Corpus Check
- 148 files · ~29,662 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 944 nodes · 2592 edges · 52 communities (28 shown, 24 thin omitted)
- Extraction: 88% EXTRACTED · 12% INFERRED · 0% AMBIGUOUS · INFERRED: 308 edges (avg confidence: 0.82)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `928e5ee9`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- OrderId
- ProductSalesRecord
- CustomerOrderPattern
- .place
- DeadLetterProperties
- AuditEvent
- metrics-consumer (App B)
- org.junit.jupiter.params.ParameterizedTest
- org.springframework.context.annotation.Bean
- MetricsController
- org.junit.jupiter.api.Test
- Order
- SuspicionProperties
- DeadLetterProperties
- orders-load.js
- package.json
- audit/adapters/config/KafkaErrorHandlingConfig.java
- org.springframework.boot.autoconfigure.SpringBootApplication
- AuditServiceWiring.java
- Topics
- SqliteRepositoryException
- dev.joaolaureano.trainingkafka:training-kafka
- AuditFilter
- InvalidAuditException
- .query
- AuditQueryService
- metrics-consumer
- metrics-consumer-adapters
- metrics-consumer-application
- metrics-consumer-domain
- order-service
- order-service-adapters
- order-service-application
- order-service-domain
- AuditLevel
- ActivityLogPublisher
- Topics
- audit-service
- audit-service-adapters
- AnalyticsWiring.java
- org.slf4j.Logger
- audit-service-application
- org.apache.kafka.clients.admin.NewTopic
- DomainEvent
- audit-service-bootstrap
- audit-service-domain
- ActivityLog
- org.junit.jupiter.params.provider.Arguments
- .handle
- Anticorruption Layer de tradução na fronteira
- metrics-consumer-bootstrap
- order-service-bootstrap

## God Nodes (most connected - your core abstractions)
1. `ProductSalesRecord` - 48 edges
2. `AuditEvent` - 43 edges
3. `ProductId` - 41 edges
4. `CustomerOrderPattern` - 40 edges
5. `ProductSalesRepository` - 34 edges
6. `AuditRepository` - 33 edges
7. `OrderLedgerRepository` - 31 edges
8. `AuditFilter` - 30 edges
9. `TimeRange` - 30 edges
10. `SuspicionPolicy` - 28 edges

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

## Communities (52 total, 24 thin omitted)

### Community 0 - "OrderId"
Cohesion: 0.16
Nodes (7): PlaceOrderPort, PlaceOrderRequest, PlaceOrderUseCase, Override, PlaceOrderFacade, Override, OrderId

### Community 1 - "ProductSalesRecord"
Cohesion: 0.06
Nodes (25): java.sql.Connection, DuckDbProductSalesRepository, Override, ProductId, InMemoryProductSalesRepository, Override, Money, Override (+17 more)

### Community 2 - "CustomerOrderPattern"
Cohesion: 0.07
Nodes (20): DuckDbCustomerPatternRepository, Override, InMemoryCustomerPatternRepository, Override, MoneyCents, Override, SqliteCustomerPatternRepository, CustomerPatternRepositoryContractTest (+12 more)

### Community 3 - ".place"
Cohesion: 0.10
Nodes (17): AuditExceptionHandler, java.time.format.DateTimeParseException, AnalyticsExceptionHandler, ApiError, ApiExceptionHandler, InvalidOrderException, CustomerId, Money (+9 more)

### Community 5 - "AuditEvent"
Cohesion: 0.15
Nodes (9): DuckDbAuditRepository, Override, ObjectMapper, Override, JsonlFileAuditRepository, StoredLine, AuditEvent, com.fasterxml.jackson.databind.ObjectMapper (+1 more)

### Community 6 - "metrics-consumer (App B)"
Cohesion: 0.08
Nodes (38): KAFKA_AUTO_CREATE_TOPICS_ENABLE=false, Listeners PLAINTEXT (9092) e PLAINTEXT_HOST (9094), Broker Kafka (apache/kafka:4.1.2), Kafka UI (kafbat/kafka-ui:8090), KRaft: nó único broker+controller, Fatores de replicação 1 para nó único, Tópico Kafka "application-logs", Gargalo: commit por mensagem no consumidor (+30 more)

### Community 7 - "org.junit.jupiter.params.ParameterizedTest"
Cohesion: 0.07
Nodes (23): AuditRepositoryContractTest, AuditLevel, Connection, AuditRepository, java.sql.PreparedStatement, DuckDbOrderLedgerRepository, Override, InMemoryOrderLedgerRepository (+15 more)

### Community 8 - "org.springframework.context.annotation.Bean"
Cohesion: 0.10
Nodes (13): AuditPersistenceConfiguration, DuckDbPersistence, JsonlPersistence, StdoutPersistence, AnalyticsWiring, DuckDbPersistence, InMemoryPersistence, PersistenceConfiguration (+5 more)

### Community 9 - "MetricsController"
Cohesion: 0.15
Nodes (10): MetricsController, ProductSalesView, RevenueView, MetricsQueryPort, OrderController, PlaceOrderResponse, org.springframework.web.bind.annotation.GetMapping, org.springframework.web.bind.annotation.PostMapping (+2 more)

### Community 10 - "org.junit.jupiter.api.Test"
Cohesion: 0.06
Nodes (24): Override, StdoutAuditRepository, KafkaErrorHandlingConfigTest, StdoutAuditRepositoryTest, ApplicationName, Override, AuditFilterTest, KafkaErrorHandlingConfigTest (+16 more)

### Community 11 - "Order"
Cohesion: 0.09
Nodes (10): CustomerId, Override, Override, Money, Override, Order, Override, ProductId (+2 more)

### Community 14 - "orders-load.js"
Cohesion: 0.17
Nodes (13): acceptanceRate, CATALOG, options, orderLatency, ordersAccepted, ordersRejected, placeNormalOrder(), placeOrder() (+5 more)

### Community 15 - "package.json"
Cohesion: 0.14
Nodes (13): esbuild, @faker-js/faker, description, devDependencies, esbuild, @faker-js/faker, name, private (+5 more)

### Community 16 - "audit/adapters/config/KafkaErrorHandlingConfig.java"
Cohesion: 0.28
Nodes (8): KafkaErrorHandlingConfig, KafkaErrorHandlingConfig, org.springframework.boot.autoconfigure.condition.ConditionalOnProperty, org.springframework.boot.autoconfigure.kafka.KafkaProperties, org.springframework.boot.context.properties.EnableConfigurationProperties, org.springframework.kafka.core.KafkaOperations, org.springframework.kafka.listener.CommonErrorHandler, org.springframework.util.backoff.BackOff

### Community 17 - "org.springframework.boot.autoconfigure.SpringBootApplication"
Cohesion: 0.27
Nodes (4): AuditServiceBootstrap, MetricsConsumerBootstrap, OrderServiceBootstrap, org.springframework.boot.autoconfigure.SpringBootApplication

### Community 18 - "AuditServiceWiring.java"
Cohesion: 0.17
Nodes (6): AuditEventListener, IngestAuditPort, IngestAuditService, AuditServiceWiring, IngestAuditFacade, Override

### Community 23 - "InvalidAuditException"
Cohesion: 0.24
Nodes (3): AuditEventMessage, AuditEventTranslator, InvalidAuditException

### Community 24 - ".query"
Cohesion: 0.27
Nodes (3): AuditController, AuditEventView, AuditQueryPort

### Community 25 - "AuditQueryService"
Cohesion: 0.31
Nodes (3): AuditQueryService, AuditQueryFacade, Override

### Community 34 - "AuditLevel"
Cohesion: 0.32
Nodes (7): AuditLevel, DEBUG, ERROR, INFO, WARN, isAtLeast(), parse()

### Community 35 - "ActivityLogPublisher"
Cohesion: 0.52
Nodes (3): PlaceOrderService, ActivityLogPublisher, OrderEventPublisher

### Community 39 - "AnalyticsWiring.java"
Cohesion: 0.06
Nodes (23): OrderListener, OrderPlacedMessage, OrderPlacedPort, OrderPlaced, Quantity, OrderPlacedTranslator, OrderPlacedHandler, FakeProductSales (+15 more)

### Community 40 - "org.slf4j.Logger"
Cohesion: 0.19
Nodes (8): AuditEventMessage, Override, KafkaDomainEventPublisher, KafkaActivityLogPublisher, KafkaOrderEventPublisher, ActivityLogFacade, org.slf4j.Logger, org.springframework.kafka.core.KafkaTemplate

### Community 42 - "org.apache.kafka.clients.admin.NewTopic"
Cohesion: 0.28
Nodes (3): KafkaTopicsConfig, Topics, org.apache.kafka.clients.admin.NewTopic

### Community 43 - "DomainEvent"
Cohesion: 0.18
Nodes (4): Override, OrderPlacedMessage, DomainEvent, OrderPlaced

### Community 47 - "ActivityLog"
Cohesion: 0.13
Nodes (7): AuditEventMessage, ActivityLog, AuditLevel, ERROR, INFO, WARN, Override

### Community 50 - ".handle"
Cohesion: 0.24
Nodes (6): PlaceOrderCommand, Override, Override, PlaceOrderServiceTest, RecordingEventPublisher, RecordingLogPublisher

### Community 53 - "Anticorruption Layer de tradução na fronteira"
Cohesion: 0.50
Nodes (5): Anticorruption Layer de tradução na fronteira, Contratos de evento duplicados por serviço, LogEntryTranslator, spring.json.add.type.headers=false, OrderPlacedTranslator

## Knowledge Gaps
- **42 isolated node(s):** `audit-service-adapters`, `audit-service-application`, `audit-service-bootstrap`, `audit-service-domain`, `DEBUG` (+37 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **24 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `AuditEvent` connect `AuditEvent` to `AuditLevel`, `org.junit.jupiter.params.ParameterizedTest`, `org.junit.jupiter.api.Test`, `AuditServiceWiring.java`, `AuditFilter`, `InvalidAuditException`, `.query`, `AuditQueryService`?**
  _High betweenness centrality (0.053) - this node is a cross-community bridge._
- **Why does `DeadLetterProperties` connect `DeadLetterProperties` to `audit/adapters/config/KafkaErrorHandlingConfig.java`, `org.apache.kafka.clients.admin.NewTopic`, `SuspicionProperties`?**
  _High betweenness centrality (0.041) - this node is a cross-community bridge._
- **Why does `DeadLetterProperties` connect `DeadLetterProperties` to `audit/adapters/config/KafkaErrorHandlingConfig.java`, `SuspicionProperties`?**
  _High betweenness centrality (0.041) - this node is a cross-community bridge._
- **Are the 3 inferred relationships involving `AuditEvent` (e.g. with `.omitsEmptyContext()` and `.printsTheEssentials()`) actually correct?**
  _`AuditEvent` has 3 INFERRED edges - model-reasoned connections that need verification._
- **What connects `audit-service-adapters`, `audit-service-application`, `audit-service-bootstrap` to the rest of the system?**
  _42 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `ProductSalesRecord` be split into smaller, more focused modules?**
  _Cohesion score 0.062066063538817585 - nodes in this community are weakly interconnected._
- **Should `CustomerOrderPattern` be split into smaller, more focused modules?**
  _Cohesion score 0.07098765432098765 - nodes in this community are weakly interconnected._