# Graph Report - training_kafka  (2026-08-31)

## Corpus Check
- 134 files · ~26,270 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 901 nodes · 2475 edges · 49 communities (29 shown, 20 thin omitted)
- Extraction: 87% EXTRACTED · 13% INFERRED · 0% AMBIGUOUS · INFERRED: 316 edges (avg confidence: 0.82)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `5d1020ec`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- org.junit.jupiter.params.ParameterizedTest
- ProductSalesRecord
- CustomerOrderPattern
- org.springframework.http.ResponseEntity
- ActivityLog
- DomainEvent
- metrics-consumer (App B)
- java.sql.Connection
- org.springframework.context.annotation.Bean
- LogController.java
- org.junit.jupiter.api.Test
- Order
- DeadLetterProperties
- DeadLetterProperties
- orders-load.js
- package.json
- LogPersistenceConfiguration.java
- org.springframework.boot.autoconfigure.SpringBootApplication
- LogFilter
- Topics
- SqliteRepositoryException
- dev.joaolaureano.trainingkafka:training-kafka
- log-aggregator
- log-aggregator-adapters
- log-aggregator-application
- log-aggregator-domain
- metrics-consumer
- metrics-consumer-adapters
- metrics-consumer-application
- metrics-consumer-domain
- order-service
- order-service-adapters
- order-service-application
- order-service-domain
- logs/adapters/config/KafkaErrorHandlingConfig.java
- LogEntry
- org.springframework.kafka.core.KafkaTemplate
- InvalidLogException
- OrderPlacedHandler
- .toDomainEvent
- .place
- Violation
- IngestLogService
- DomainEvent
- org.apache.kafka.clients.admin.NewTopic
- .publish
- SuspicionProperties
- LogLevel
- Topics

## God Nodes (most connected - your core abstractions)
1. `ProductSalesRecord` - 44 edges
2. `CustomerOrderPattern` - 40 edges
3. `ProductId` - 37 edges
4. `LogEntry` - 35 edges
5. `ProductSalesRepository` - 34 edges
6. `LogRepository` - 33 edges
7. `OrderLedgerRepository` - 31 edges
8. `SuspicionPolicy` - 28 edges
9. `CustomerPatternRepository` - 28 edges
10. `ApplicationName` - 27 edges

## Surprising Connections (you probably didn't know these)
- `ApplicationLogMessage (default type do consumidor C)` --semantically_similar_to--> `OrderPlacedMessage (default type do consumidor B)`  [INFERRED] [semantically similar]
  log-aggregator/log-aggregator-adapters/src/main/resources/application.yml → metrics-consumer/metrics-consumer-adapters/src/main/resources/application.yml
- `ErrorHandlingDeserializer + JsonDeserializer sem type headers (C)` --semantically_similar_to--> `ErrorHandlingDeserializer + JsonDeserializer sem type headers (B)`  [INFERRED] [semantically similar]
  log-aggregator/log-aggregator-adapters/src/main/resources/application.yml → metrics-consumer/metrics-consumer-adapters/src/main/resources/application.yml
- `analytics.suspicion (max-orders 5 / janela 10s)` --conceptually_related_to--> `CustomerOrderPattern (raiz de agregado)`  [INFERRED]
  metrics-consumer/metrics-consumer-adapters/src/main/resources/application.yml → README.md
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` --conceptually_related_to--> `order-service (App A)`  [INFERRED]
  docker-compose.yml → README.md
- `training_kafka (laboratório Kafka + DDD)` --references--> `Kafka UI (kafbat/kafka-ui:8090)`  [EXTRACTED]
  README.md → docker-compose.yml

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Pipeline de eventos A → orders → B → application-logs → C** — readme_order_service, readme_orders_topic, readme_metrics_consumer, readme_application_logs_topic, readme_log_aggregator, docker_compose_kafka_broker [EXTRACTED 1.00]
- **Modelo de domínio do App B (3 raízes + VO derivado + handler)** — readme_productsalesrecord, readme_customerorderpattern, readme_orderrecord, readme_revenuewindow, readme_orderplacedhandler [EXTRACTED 1.00]
- **Desacoplamento de bounded contexts via contratos próprios e ACL** — readme_duplicated_event_contracts, readme_no_type_headers, readme_anticorruption_layer, metrics_consumer_metrics_consumer_adapters_src_main_resources_application_orderplacedmessage, log_aggregator_log_aggregator_adapters_src_main_resources_application_applicationlogmessage [INFERRED 0.95]

## Communities (49 total, 20 thin omitted)

### Community 0 - "org.junit.jupiter.params.ParameterizedTest"
Cohesion: 0.34
Nodes (6): Connection, LogLevel, LogRepositoryContractTest, LogRepository, org.junit.jupiter.params.ParameterizedTest, org.junit.jupiter.params.provider.MethodSource

### Community 1 - "ProductSalesRecord"
Cohesion: 0.06
Nodes (27): DuckDbProductSalesRepository, Override, ProductId, InMemoryProductSalesRepository, Override, Money, Override, ProductId (+19 more)

### Community 2 - "CustomerOrderPattern"
Cohesion: 0.07
Nodes (22): java.sql.PreparedStatement, java.sql.ResultSet, DuckDbCustomerPatternRepository, Override, InMemoryCustomerPatternRepository, Override, MoneyCents, Override (+14 more)

### Community 3 - "org.springframework.http.ResponseEntity"
Cohesion: 0.30
Nodes (7): LogExceptionHandler, AnalyticsExceptionHandler, ApiError, ApiExceptionHandler, org.springframework.http.ResponseEntity, org.springframework.web.bind.annotation.ExceptionHandler, org.springframework.web.bind.annotation.RestControllerAdvice

### Community 4 - "ActivityLog"
Cohesion: 0.23
Nodes (5): PlaceOrderService, ActivityLog, ActivityLogPublisher, RecordingLogPublisher, OrderEventPublisher

### Community 5 - "DomainEvent"
Cohesion: 0.19
Nodes (7): ApplicationLogMessage, Override, KafkaDomainEventPublisher, RecordingPublisher, DomainEvent, SuspiciousPatternDetected, DomainEventPublisher

### Community 6 - "metrics-consumer (App B)"
Cohesion: 0.06
Nodes (53): KAFKA_AUTO_CREATE_TOPICS_ENABLE=false, Listeners PLAINTEXT (9092) e PLAINTEXT_HOST (9094), Broker Kafka (apache/kafka:4.1.2), Kafka UI (kafbat/kafka-ui:8090), KRaft: nó único broker+controller, Fatores de replicação 1 para nó único, ApplicationLogMessage (default type do consumidor C), logs.duckdb em arquivo próprio (+45 more)

### Community 7 - "java.sql.Connection"
Cohesion: 0.07
Nodes (21): java.sql.Connection, DuckDbOrderLedgerRepository, Override, InMemoryOrderLedgerRepository, Override, Override, SqliteOrderLedgerRepository, MetricsController (+13 more)

### Community 8 - "org.springframework.context.annotation.Bean"
Cohesion: 0.11
Nodes (13): LogAggregatorWiring, DuckDbPersistence, JsonlPersistence, LogPersistenceConfiguration, StdoutPersistence, AnalyticsWiring, DuckDbPersistence, InMemoryPersistence (+5 more)

### Community 9 - "LogController.java"
Cohesion: 0.12
Nodes (10): LogController, LogEntryView, LogQueryService, OrderController, PlaceOrderRequest, PlaceOrderResponse, PlaceOrderUseCase, org.springframework.web.bind.annotation.PostMapping (+2 more)

### Community 10 - "org.junit.jupiter.api.Test"
Cohesion: 0.06
Nodes (24): KafkaErrorHandlingConfigTest, LogFilterTest, KafkaErrorHandlingConfigTest, OrderPlaced, ProductId, OrderPlacedHandlerTest, BurstBehaviour, CustomerOrderPatternTest (+16 more)

### Community 11 - "Order"
Cohesion: 0.07
Nodes (14): OrderPlacedMessage, OrderPlaced, CustomerId, Override, Override, Money, Override, Order (+6 more)

### Community 14 - "orders-load.js"
Cohesion: 0.17
Nodes (13): acceptanceRate, CATALOG, options, orderLatency, ordersAccepted, ordersRejected, placeNormalOrder(), placeOrder() (+5 more)

### Community 15 - "package.json"
Cohesion: 0.14
Nodes (13): esbuild, @faker-js/faker, description, devDependencies, esbuild, @faker-js/faker, name, private (+5 more)

### Community 16 - "LogPersistenceConfiguration.java"
Cohesion: 0.17
Nodes (7): com.fasterxml.jackson.databind.ObjectMapper, DuckDbLogRepository, Override, ObjectMapper, Override, JsonlFileLogRepository, StoredLine

### Community 17 - "org.springframework.boot.autoconfigure.SpringBootApplication"
Cohesion: 0.27
Nodes (4): LogAggregatorApplication, MetricsConsumerApplication, OrderServiceApplication, org.springframework.boot.autoconfigure.SpringBootApplication

### Community 18 - "LogFilter"
Cohesion: 0.12
Nodes (9): LogFilter, isAtLeast(), LogLevel, DEBUG, ERROR, INFO, WARN, parse() (+1 more)

### Community 34 - "logs/adapters/config/KafkaErrorHandlingConfig.java"
Cohesion: 0.24
Nodes (10): KafkaTemplate, KafkaErrorHandlingConfig, KafkaTemplate, KafkaErrorHandlingConfig, org.springframework.boot.autoconfigure.condition.ConditionalOnProperty, org.springframework.boot.autoconfigure.kafka.KafkaProperties, org.springframework.boot.context.properties.EnableConfigurationProperties, org.springframework.kafka.core.KafkaOperations (+2 more)

### Community 35 - "LogEntry"
Cohesion: 0.21
Nodes (7): Override, StdoutLogRepository, StdoutLogRepositoryTest, ApplicationName, Override, LogEntry, org.junit.jupiter.api.AfterEach

### Community 36 - "org.springframework.kafka.core.KafkaTemplate"
Cohesion: 0.29
Nodes (5): OrderServiceWiring, KafkaActivityLogPublisher, KafkaOrderEventPublisher, org.slf4j.Logger, org.springframework.kafka.core.KafkaTemplate

### Community 37 - "InvalidLogException"
Cohesion: 0.23
Nodes (4): java.time.format.DateTimeParseException, ApplicationLogMessage, LogEntryTranslator, InvalidLogException

### Community 38 - "OrderPlacedHandler"
Cohesion: 0.20
Nodes (3): OrderListener, OrderPlacedHandler, org.junit.jupiter.api.BeforeEach

### Community 39 - ".toDomainEvent"
Cohesion: 0.23
Nodes (5): OrderPlacedMessage, OrderPlaced, Quantity, OrderPlacedTranslator, InvalidValueException

### Community 40 - ".place"
Cohesion: 0.21
Nodes (5): CustomerId, Money, ProductId, Quantity, Violations

### Community 41 - "Violation"
Cohesion: 0.29
Nodes (4): InvalidOrderException, Override, Violation, org.springframework.http.converter.HttpMessageNotReadableException

### Community 42 - "IngestLogService"
Cohesion: 0.27
Nodes (4): ApplicationLogListener, IngestLogService, org.springframework.kafka.annotation.KafkaListener, org.springframework.stereotype.Component

### Community 43 - "DomainEvent"
Cohesion: 0.20
Nodes (4): Override, Override, RecordingEventPublisher, DomainEvent

### Community 44 - "org.apache.kafka.clients.admin.NewTopic"
Cohesion: 0.24
Nodes (3): KafkaTopicsConfig, Topics, org.apache.kafka.clients.admin.NewTopic

### Community 47 - "LogLevel"
Cohesion: 0.40
Nodes (4): LogLevel, ERROR, INFO, WARN

## Knowledge Gaps
- **40 isolated node(s):** `name`, `version`, `private`, `description`, `build` (+35 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **20 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `DeadLetterProperties` connect `DeadLetterProperties` to `logs/adapters/config/KafkaErrorHandlingConfig.java`, `org.apache.kafka.clients.admin.NewTopic`, `SuspicionProperties`?**
  _High betweenness centrality (0.042) - this node is a cross-community bridge._
- **Why does `DeadLetterProperties` connect `DeadLetterProperties` to `logs/adapters/config/KafkaErrorHandlingConfig.java`, `org.apache.kafka.clients.admin.NewTopic`, `SuspicionProperties`?**
  _High betweenness centrality (0.042) - this node is a cross-community bridge._
- **Why does `LogEntry` connect `LogEntry` to `org.junit.jupiter.params.ParameterizedTest`, `InvalidLogException`, `LogController.java`, `IngestLogService`, `org.junit.jupiter.api.Test`, `LogPersistenceConfiguration.java`, `LogFilter`?**
  _High betweenness centrality (0.038) - this node is a cross-community bridge._
- **What connects `name`, `version`, `private` to the rest of the system?**
  _40 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `ProductSalesRecord` be split into smaller, more focused modules?**
  _Cohesion score 0.0594059405940594 - nodes in this community are weakly interconnected._
- **Should `CustomerOrderPattern` be split into smaller, more focused modules?**
  _Cohesion score 0.06975308641975309 - nodes in this community are weakly interconnected._
- **Should `metrics-consumer (App B)` be split into smaller, more focused modules?**
  _Cohesion score 0.059506531204644414 - nodes in this community are weakly interconnected._