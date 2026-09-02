# Graph Report - training_kafka  (2026-09-02)

## Corpus Check
- 156 files · ~30,612 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 999 nodes · 2698 edges · 54 communities (32 shown, 22 thin omitted)
- Extraction: 89% EXTRACTED · 11% INFERRED · 0% AMBIGUOUS · INFERRED: 286 edges (avg confidence: 0.82)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `3ba66ee3`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- OrderId
- ProductSalesRecord
- CustomerOrderPattern
- org.springframework.http.ResponseEntity
- DeadLetterProperties
- DuckDbAuditRepository.java
- metrics-consumer (App B)
- java.sql.Connection
- org.springframework.context.annotation.Bean
- .place
- org.junit.jupiter.api.Test
- OrderPlaced
- AuditEvent
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
- AuditController.java
- AuditFilter
- metrics-consumer
- metrics-consumer-adapters
- metrics-consumer-application
- metrics-consumer-domain
- order-service
- order-service-adapters
- order-service-application
- order-service-domain
- org.junit.jupiter.params.ParameterizedTest
- Topics
- audit-service
- audit-service-adapters
- AnalyticsWiring.java
- OrderServiceWiring.java
- audit-service-application
- org.apache.kafka.clients.admin.NewTopic
- Override
- audit-service-bootstrap
- audit-service-domain
- ActivityLog
- AuditEvent
- Order
- .handle
- Anticorruption Layer de tradução na fronteira
- AuditEventTranslator.java
- metrics-consumer-bootstrap
- order-service-bootstrap
- AuditEventListener
- Topics

## God Nodes (most connected - your core abstractions)
1. `ProductSalesRecord` - 48 edges
2. `ProductId` - 41 edges
3. `CustomerOrderPattern` - 40 edges
4. `AuditEvent` - 39 edges
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
- `AuditEvent` --references--> `ApplicationName`  [EXTRACTED]
  log-aggregator/log-aggregator-domain/src/main/java/dev/joaolaureano/trainingkafka/audit/domain/model/AuditEvent.java → log-aggregator/log-aggregator-domain/src/main/java/dev/joaolaureano/trainingkafka/audit/domain/model/ApplicationName.java
- `AuditFilter` --references--> `ApplicationName`  [EXTRACTED]
  log-aggregator/log-aggregator-domain/src/main/java/dev/joaolaureano/trainingkafka/audit/domain/model/LogFilter.java → log-aggregator/log-aggregator-domain/src/main/java/dev/joaolaureano/trainingkafka/audit/domain/model/ApplicationName.java

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Pipeline de eventos A → orders → B → application-logs → C** — readme_order_service, readme_orders_topic, readme_metrics_consumer, readme_application_logs_topic, readme_log_aggregator, docker_compose_kafka_broker [EXTRACTED 1.00]
- **Modelo de domínio do App B (3 raízes + VO derivado + handler)** — readme_productsalesrecord, readme_customerorderpattern, readme_orderrecord, readme_revenuewindow, readme_orderplacedhandler [EXTRACTED 1.00]

## Communities (54 total, 22 thin omitted)

### Community 0 - "OrderId"
Cohesion: 0.14
Nodes (7): PlaceOrderPort, PlaceOrderRequest, PlaceOrderUseCase, Override, PlaceOrderFacade, Override, OrderId

### Community 1 - "ProductSalesRecord"
Cohesion: 0.05
Nodes (29): DuckDbProductSalesRepository, Override, ProductId, InMemoryProductSalesRepository, Override, Money, Override, ProductId (+21 more)

### Community 2 - "CustomerOrderPattern"
Cohesion: 0.06
Nodes (26): DuckDbCustomerPatternRepository, Override, InMemoryCustomerPatternRepository, Override, Override, SqliteCustomerPatternRepository, CustomerPatternRepositoryContractTest, CustomerId (+18 more)

### Community 3 - "org.springframework.http.ResponseEntity"
Cohesion: 0.24
Nodes (9): AuditExceptionHandler, java.time.format.DateTimeParseException, AnalyticsExceptionHandler, ApiError, ApiExceptionHandler, org.springframework.http.converter.HttpMessageNotReadableException, org.springframework.http.ResponseEntity, org.springframework.web.bind.annotation.ExceptionHandler (+1 more)

### Community 4 - "DeadLetterProperties"
Cohesion: 0.08
Nodes (3): DeadLetterProperties, Retry, org.springframework.boot.context.properties.ConfigurationProperties

### Community 5 - "DuckDbAuditRepository.java"
Cohesion: 0.14
Nodes (10): DuckDbAuditRepository, AuditEvent, Override, AuditEvent, ObjectMapper, Override, JsonlFileAuditRepository, StoredLine (+2 more)

### Community 6 - "metrics-consumer (App B)"
Cohesion: 0.08
Nodes (38): KAFKA_AUTO_CREATE_TOPICS_ENABLE=false, Listeners PLAINTEXT (9092) e PLAINTEXT_HOST (9094), Broker Kafka (apache/kafka:4.1.2), Kafka UI (kafbat/kafka-ui:8090), KRaft: nó único broker+controller, Fatores de replicação 1 para nó único, Tópico Kafka "application-logs", Gargalo: commit por mensagem no consumidor (+30 more)

### Community 7 - "java.sql.Connection"
Cohesion: 0.07
Nodes (20): java.sql.Connection, java.sql.PreparedStatement, java.sql.ResultSet, DuckDbOrderLedgerRepository, Override, InMemoryOrderLedgerRepository, Override, MoneyCents (+12 more)

### Community 8 - "org.springframework.context.annotation.Bean"
Cohesion: 0.16
Nodes (11): AuditPersistenceConfiguration, DuckDbPersistence, JsonlPersistence, StdoutPersistence, DuckDbPersistence, InMemoryPersistence, PersistenceConfiguration, SqlitePersistence (+3 more)

### Community 9 - ".place"
Cohesion: 0.14
Nodes (8): InvalidOrderException, CustomerId, Money, ProductId, Quantity, Override, Violation, Violations

### Community 10 - "org.junit.jupiter.api.Test"
Cohesion: 0.05
Nodes (24): KafkaErrorHandlingConfigTest, StdoutAuditRepositoryTest, ApplicationName, Override, AuditFilterTest, AuditEvent, ApplicationName, AuditFilterTest (+16 more)

### Community 11 - "OrderPlaced"
Cohesion: 0.09
Nodes (10): OrderPlacedMessage, OrderPlaced, CustomerId, Override, Override, Money, Override, ProductId (+2 more)

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
Cohesion: 0.17
Nodes (7): IngestAuditPort, AuditQueryPort, AuditQueryService, IngestAuditService, AuditServiceWiring, AuditQueryFacade, IngestAuditFacade

### Community 19 - ".toDomainEvent"
Cohesion: 0.23
Nodes (5): OrderPlacedMessage, OrderPlaced, Quantity, OrderPlacedTranslator, InvalidValueException

### Community 22 - "AuditFilter"
Cohesion: 0.10
Nodes (10): Override, AuditFilter, AuditLevel, DEBUG, ERROR, INFO, WARN, isAtLeast() (+2 more)

### Community 24 - "AuditController.java"
Cohesion: 0.15
Nodes (12): AuditController, AuditEventView, MetricsController, ProductSalesView, RevenueView, MetricsQueryPort, OrderController, PlaceOrderResponse (+4 more)

### Community 25 - "AuditFilter"
Cohesion: 0.08
Nodes (10): AuditEvent, AuditFilter, AuditLevel, DEBUG, ERROR, INFO, WARN, isAtLeast() (+2 more)

### Community 34 - "org.junit.jupiter.params.ParameterizedTest"
Cohesion: 0.28
Nodes (7): AuditRepositoryContractTest, AuditEvent, AuditLevel, Connection, AuditRepository, org.junit.jupiter.params.ParameterizedTest, org.junit.jupiter.params.provider.MethodSource

### Community 39 - "AnalyticsWiring.java"
Cohesion: 0.09
Nodes (12): SuspicionProperties, AuditEventMessage, Override, KafkaDomainEventPublisher, OrderPlacedPort, OrderPlacedHandler, RecordingPublisher, AnalyticsWiring (+4 more)

### Community 40 - "OrderServiceWiring.java"
Cohesion: 0.20
Nodes (8): KafkaActivityLogPublisher, KafkaOrderEventPublisher, PlaceOrderService, ActivityLogPublisher, OrderServiceWiring, ActivityLogFacade, OrderEventPublisher, org.springframework.kafka.core.KafkaTemplate

### Community 42 - "org.apache.kafka.clients.admin.NewTopic"
Cohesion: 0.28
Nodes (3): KafkaTopicsConfig, Topics, org.apache.kafka.clients.admin.NewTopic

### Community 47 - "ActivityLog"
Cohesion: 0.11
Nodes (9): AuditEventMessage, ActivityLog, AuditLevel, ERROR, INFO, WARN, Override, RecordingLogPublisher (+1 more)

### Community 48 - "AuditEvent"
Cohesion: 0.20
Nodes (4): Override, StdoutAuditRepository, Override, AuditEvent

### Community 49 - "Order"
Cohesion: 0.17
Nodes (4): Override, DomainEvent, Override, Order

### Community 50 - ".handle"
Cohesion: 0.35
Nodes (4): PlaceOrderCommand, Override, PlaceOrderServiceTest, RecordingEventPublisher

### Community 53 - "Anticorruption Layer de tradução na fronteira"
Cohesion: 0.50
Nodes (5): Anticorruption Layer de tradução na fronteira, Contratos de evento duplicados por serviço, LogEntryTranslator, spring.json.add.type.headers=false, OrderPlacedTranslator

### Community 54 - "AuditEventTranslator.java"
Cohesion: 0.18
Nodes (5): AuditEventMessage, AuditEventTranslator, AuditEvent, InvalidAuditException, InvalidAuditException

### Community 58 - "AuditEventListener"
Cohesion: 0.24
Nodes (4): AuditEventListener, OrderListener, org.springframework.kafka.annotation.KafkaListener, org.springframework.stereotype.Component

## Knowledge Gaps
- **46 isolated node(s):** `DEBUG`, `INFO`, `WARN`, `ERROR`, `acceptanceRate` (+41 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **22 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `AuditEvent` connect `AuditEvent` to `DuckDbAuditRepository.java`, `org.junit.jupiter.api.Test`, `AuditServiceWiring.java`, `AuditEventTranslator.java`, `AuditFilter`, `AuditController.java`, `AuditFilter`, `AuditEventListener`?**
  _High betweenness centrality (0.041) - this node is a cross-community bridge._
- **Why does `DeadLetterProperties` connect `DeadLetterProperties` to `org.slf4j.Logger`?**
  _High betweenness centrality (0.039) - this node is a cross-community bridge._
- **Why does `DeadLetterProperties` connect `DeadLetterProperties` to `org.slf4j.Logger`, `org.apache.kafka.clients.admin.NewTopic`, `DeadLetterProperties`?**
  _High betweenness centrality (0.039) - this node is a cross-community bridge._
- **Are the 4 inferred relationships involving `ProductId` (e.g. with `.toDomainEvent()` and `.revenue()`) actually correct?**
  _`ProductId` has 4 INFERRED edges - model-reasoned connections that need verification._
- **What connects `DEBUG`, `INFO`, `WARN` to the rest of the system?**
  _46 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `OrderId` be split into smaller, more focused modules?**
  _Cohesion score 0.14285714285714285 - nodes in this community are weakly interconnected._
- **Should `ProductSalesRecord` be split into smaller, more focused modules?**
  _Cohesion score 0.05431140892258861 - nodes in this community are weakly interconnected._