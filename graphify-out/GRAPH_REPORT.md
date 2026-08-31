# Graph Report - training_kafka  (2026-08-31)

## Corpus Check
- 148 files · ~29,689 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 944 nodes · 2593 edges · 57 communities (33 shown, 24 thin omitted)
- Extraction: 88% EXTRACTED · 12% INFERRED · 0% AMBIGUOUS · INFERRED: 309 edges (avg confidence: 0.82)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `5d1020ec`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- OrderId
- ProductSalesRecord
- CustomerOrderPattern
- Violation
- ActivityLogPublisher
- org.slf4j.Logger
- metrics-consumer (App B)
- org.junit.jupiter.params.ParameterizedTest
- org.springframework.context.annotation.Bean
- MetricsController
- org.junit.jupiter.api.Test
- OrderPlaced
- DeadLetterProperties
- DeadLetterProperties
- orders-load.js
- package.json
- ApplicationName
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
- StdoutLogRepository
- LogAggregatorWiring.java
- InvalidLogException
- Order
- AnalyticsWiring.java
- .place
- KafkaErrorHandlingConfigTest
- LogEntry
- DomainEvent
- org.apache.kafka.clients.admin.NewTopic
- KafkaActivityLogPublisher
- SuspicionProperties
- ActivityLog
- Topics
- org.junit.jupiter.params.provider.Arguments
- .handle
- JsonlFileLogRepository
- LogLevel
- Anticorruption Layer de tradução na fronteira
- log-aggregator-bootstrap
- metrics-consumer-bootstrap
- order-service-bootstrap

## God Nodes (most connected - your core abstractions)
1. `ProductSalesRecord` - 48 edges
2. `LogEntry` - 43 edges
3. `ProductId` - 41 edges
4. `CustomerOrderPattern` - 40 edges
5. `ProductSalesRepository` - 34 edges
6. `LogRepository` - 33 edges
7. `OrderLedgerRepository` - 31 edges
8. `LogFilter` - 30 edges
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
  log-aggregator/log-aggregator-adapters/src/main/java/dev/joaolaureano/trainingkafka/logs/adapters/config/KafkaErrorHandlingConfig.java → log-aggregator/log-aggregator-adapters/src/main/java/dev/joaolaureano/trainingkafka/logs/adapters/config/DeadLetterProperties.java
- `ApplicationLogListener` --references--> `IngestLogPort`  [EXTRACTED]
  log-aggregator/log-aggregator-adapters/src/main/java/dev/joaolaureano/trainingkafka/logs/adapters/messaging/ApplicationLogListener.java → log-aggregator/log-aggregator-adapters/src/main/java/dev/joaolaureano/trainingkafka/logs/adapters/messaging/IngestLogPort.java

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Pipeline de eventos A → orders → B → application-logs → C** — readme_order_service, readme_orders_topic, readme_metrics_consumer, readme_application_logs_topic, readme_log_aggregator, docker_compose_kafka_broker [EXTRACTED 1.00]
- **Modelo de domínio do App B (3 raízes + VO derivado + handler)** — readme_productsalesrecord, readme_customerorderpattern, readme_orderrecord, readme_revenuewindow, readme_orderplacedhandler [EXTRACTED 1.00]

## Communities (57 total, 24 thin omitted)

### Community 0 - "OrderId"
Cohesion: 0.16
Nodes (7): PlaceOrderPort, PlaceOrderRequest, PlaceOrderUseCase, Override, PlaceOrderFacade, Override, OrderId

### Community 1 - "ProductSalesRecord"
Cohesion: 0.06
Nodes (25): java.sql.Connection, DuckDbProductSalesRepository, Override, ProductId, InMemoryProductSalesRepository, Override, Money, Override (+17 more)

### Community 2 - "CustomerOrderPattern"
Cohesion: 0.06
Nodes (27): java.sql.ResultSet, DuckDbCustomerPatternRepository, Override, InMemoryCustomerPatternRepository, Override, MoneyCents, Override, SqliteCustomerPatternRepository (+19 more)

### Community 3 - "Violation"
Cohesion: 0.11
Nodes (13): java.time.format.DateTimeParseException, LogExceptionHandler, AnalyticsExceptionHandler, ApiError, ApiExceptionHandler, InvalidOrderException, Override, Violation (+5 more)

### Community 4 - "ActivityLogPublisher"
Cohesion: 0.27
Nodes (4): PlaceOrderService, ActivityLogPublisher, OrderServiceWiring, OrderEventPublisher

### Community 5 - "org.slf4j.Logger"
Cohesion: 0.24
Nodes (6): ApplicationLogMessage, Override, KafkaDomainEventPublisher, KafkaOrderEventPublisher, org.slf4j.Logger, org.springframework.kafka.core.KafkaTemplate

### Community 6 - "metrics-consumer (App B)"
Cohesion: 0.08
Nodes (38): KAFKA_AUTO_CREATE_TOPICS_ENABLE=false, Listeners PLAINTEXT (9092) e PLAINTEXT_HOST (9094), Broker Kafka (apache/kafka:4.1.2), Kafka UI (kafbat/kafka-ui:8090), KRaft: nó único broker+controller, Fatores de replicação 1 para nó único, Tópico Kafka "application-logs", Gargalo: commit por mensagem no consumidor (+30 more)

### Community 7 - "org.junit.jupiter.params.ParameterizedTest"
Cohesion: 0.07
Nodes (23): java.sql.PreparedStatement, Connection, LogLevel, LogRepositoryContractTest, LogRepository, DuckDbOrderLedgerRepository, Override, InMemoryOrderLedgerRepository (+15 more)

### Community 8 - "org.springframework.context.annotation.Bean"
Cohesion: 0.12
Nodes (12): DuckDbPersistence, JsonlPersistence, LogPersistenceConfiguration, StdoutPersistence, AnalyticsWiring, DuckDbPersistence, InMemoryPersistence, PersistenceConfiguration (+4 more)

### Community 9 - "MetricsController"
Cohesion: 0.16
Nodes (10): MetricsController, ProductSalesView, RevenueView, MetricsQueryPort, OrderController, PlaceOrderResponse, org.springframework.web.bind.annotation.GetMapping, org.springframework.web.bind.annotation.PostMapping (+2 more)

### Community 10 - "org.junit.jupiter.api.Test"
Cohesion: 0.20
Nodes (5): LogFilterTest, TimeRange, MoneyRules, org.junit.jupiter.api.DisplayName, org.junit.jupiter.api.Test

### Community 11 - "OrderPlaced"
Cohesion: 0.09
Nodes (10): OrderPlacedMessage, OrderPlaced, CustomerId, Override, Override, Money, Override, ProductId (+2 more)

### Community 14 - "orders-load.js"
Cohesion: 0.17
Nodes (13): acceptanceRate, CATALOG, options, orderLatency, ordersAccepted, ordersRejected, placeNormalOrder(), placeOrder() (+5 more)

### Community 15 - "package.json"
Cohesion: 0.14
Nodes (13): esbuild, @faker-js/faker, description, devDependencies, esbuild, @faker-js/faker, name, private (+5 more)

### Community 16 - "ApplicationName"
Cohesion: 0.18
Nodes (5): com.fasterxml.jackson.databind.ObjectMapper, DuckDbLogRepository, Override, ApplicationName, Override

### Community 17 - "org.springframework.boot.autoconfigure.SpringBootApplication"
Cohesion: 0.27
Nodes (4): LogAggregatorBootstrap, MetricsConsumerBootstrap, OrderServiceBootstrap, org.springframework.boot.autoconfigure.SpringBootApplication

### Community 34 - "logs/adapters/config/KafkaErrorHandlingConfig.java"
Cohesion: 0.30
Nodes (8): KafkaErrorHandlingConfig, KafkaErrorHandlingConfig, org.springframework.boot.autoconfigure.condition.ConditionalOnProperty, org.springframework.boot.autoconfigure.kafka.KafkaProperties, org.springframework.boot.context.properties.EnableConfigurationProperties, org.springframework.kafka.core.KafkaOperations, org.springframework.kafka.listener.CommonErrorHandler, org.springframework.util.backoff.BackOff

### Community 35 - "StdoutLogRepository"
Cohesion: 0.26
Nodes (4): Override, StdoutLogRepository, StdoutLogRepositoryTest, org.junit.jupiter.api.AfterEach

### Community 36 - "LogAggregatorWiring.java"
Cohesion: 0.17
Nodes (7): LogController, LogEntryView, LogQueryPort, LogQueryService, LogAggregatorWiring, Override, LogQueryFacade

### Community 37 - "InvalidLogException"
Cohesion: 0.24
Nodes (3): ApplicationLogMessage, LogEntryTranslator, InvalidLogException

### Community 38 - "Order"
Cohesion: 0.18
Nodes (4): Override, Order, OrderTest, WhenValid

### Community 39 - "AnalyticsWiring.java"
Cohesion: 0.05
Nodes (25): ApplicationLogListener, OrderListener, OrderPlacedMessage, OrderPlacedPort, OrderPlaced, Quantity, OrderPlacedTranslator, OrderPlacedHandler (+17 more)

### Community 40 - ".place"
Cohesion: 0.24
Nodes (5): CustomerId, Money, ProductId, Quantity, WhenInvalid

### Community 41 - "KafkaErrorHandlingConfigTest"
Cohesion: 0.18
Nodes (3): KafkaErrorHandlingConfigTest, KafkaErrorHandlingConfigTest, org.springframework.boot.test.context.runner.ApplicationContextRunner

### Community 42 - "LogEntry"
Cohesion: 0.26
Nodes (5): IngestLogPort, IngestLogService, IngestLogFacade, Override, LogEntry

### Community 43 - "DomainEvent"
Cohesion: 0.16
Nodes (5): Override, Override, RecordingEventPublisher, RecordingLogPublisher, DomainEvent

### Community 44 - "org.apache.kafka.clients.admin.NewTopic"
Cohesion: 0.24
Nodes (3): KafkaTopicsConfig, Topics, org.apache.kafka.clients.admin.NewTopic

### Community 45 - "KafkaActivityLogPublisher"
Cohesion: 0.32
Nodes (3): ApplicationLogMessage, KafkaActivityLogPublisher, ActivityLogFacade

### Community 47 - "ActivityLog"
Cohesion: 0.17
Nodes (6): ActivityLog, LogLevel, ERROR, INFO, WARN, Override

### Community 50 - ".handle"
Cohesion: 0.38
Nodes (3): PlaceOrderCommand, Override, PlaceOrderServiceTest

### Community 51 - "JsonlFileLogRepository"
Cohesion: 0.33
Nodes (4): ObjectMapper, Override, JsonlFileLogRepository, StoredLine

### Community 52 - "LogLevel"
Cohesion: 0.32
Nodes (7): isAtLeast(), LogLevel, DEBUG, ERROR, INFO, WARN, parse()

### Community 53 - "Anticorruption Layer de tradução na fronteira"
Cohesion: 0.50
Nodes (5): Anticorruption Layer de tradução na fronteira, Contratos de evento duplicados por serviço, LogEntryTranslator, spring.json.add.type.headers=false, OrderPlacedTranslator

## Knowledge Gaps
- **42 isolated node(s):** `name`, `version`, `private`, `description`, `build` (+37 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **24 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `LogEntry` connect `LogEntry` to `StdoutLogRepository`, `LogAggregatorWiring.java`, `InvalidLogException`, `org.junit.jupiter.params.ParameterizedTest`, `org.junit.jupiter.api.Test`, `ApplicationName`, `LogFilter`, `JsonlFileLogRepository`, `LogLevel`?**
  _High betweenness centrality (0.053) - this node is a cross-community bridge._
- **Why does `DeadLetterProperties` connect `DeadLetterProperties` to `logs/adapters/config/KafkaErrorHandlingConfig.java`, `org.apache.kafka.clients.admin.NewTopic`, `SuspicionProperties`?**
  _High betweenness centrality (0.041) - this node is a cross-community bridge._
- **Why does `DeadLetterProperties` connect `DeadLetterProperties` to `logs/adapters/config/KafkaErrorHandlingConfig.java`, `org.apache.kafka.clients.admin.NewTopic`, `SuspicionProperties`?**
  _High betweenness centrality (0.041) - this node is a cross-community bridge._
- **Are the 3 inferred relationships involving `LogEntry` (e.g. with `.omitsEmptyContext()` and `.printsTheEssentials()`) actually correct?**
  _`LogEntry` has 3 INFERRED edges - model-reasoned connections that need verification._
- **What connects `name`, `version`, `private` to the rest of the system?**
  _42 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `ProductSalesRecord` be split into smaller, more focused modules?**
  _Cohesion score 0.062066063538817585 - nodes in this community are weakly interconnected._
- **Should `CustomerOrderPattern` be split into smaller, more focused modules?**
  _Cohesion score 0.05998763141620284 - nodes in this community are weakly interconnected._