# Graph Report - training_kafka  (2026-09-02)

## Corpus Check
- cluster-only mode — file stats not available

## Summary
- 944 nodes · 2593 edges · 54 communities (26 shown, 28 thin omitted)
- Extraction: 88% EXTRACTED · 12% INFERRED · 0% AMBIGUOUS · INFERRED: 309 edges (avg confidence: 0.82)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `419f6b04`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- OrderId
- ProductSalesRecord
- CustomerOrderPattern
- .place
- LogAggregatorWiring.java
- ApplicationName
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
- logs/adapters/config/KafkaErrorHandlingConfig.java
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
- StdoutLogRepository
- .publish
- LogEntry
- Retry
- Order
- AnalyticsWiring.java
- OrderServiceWiring.java
- InvalidLogException
- Topics
- DomainEvent
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
- `OrderController` --references--> `PlaceOrderPort`  [EXTRACTED]
  order-service/order-service-adapters/src/main/java/dev/joaolaureano/trainingkafka/orders/adapters/web/OrderController.java → order-service/order-service-adapters/src/main/java/dev/joaolaureano/trainingkafka/orders/adapters/web/PlaceOrderPort.java
- `PlaceOrderService` --implements--> `PlaceOrderUseCase`  [EXTRACTED]
  order-service/order-service-application/src/main/java/dev/joaolaureano/trainingkafka/orders/application/PlaceOrderService.java → order-service/order-service-application/src/main/java/dev/joaolaureano/trainingkafka/orders/application/PlaceOrderUseCase.java

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Pipeline de eventos A → orders → B → application-logs → C** — readme_order_service, readme_orders_topic, readme_metrics_consumer, readme_application_logs_topic, readme_log_aggregator, docker_compose_kafka_broker [EXTRACTED 1.00]
- **Modelo de domínio do App B (3 raízes + VO derivado + handler)** — readme_productsalesrecord, readme_customerorderpattern, readme_orderrecord, readme_revenuewindow, readme_orderplacedhandler [EXTRACTED 1.00]

## Communities (54 total, 28 thin omitted)

### Community 0 - "OrderId"
Cohesion: 0.15
Nodes (7): PlaceOrderPort, PlaceOrderRequest, PlaceOrderUseCase, Override, PlaceOrderFacade, Override, OrderId

### Community 1 - "ProductSalesRecord"
Cohesion: 0.06
Nodes (25): java.sql.Connection, DuckDbProductSalesRepository, Override, ProductId, InMemoryProductSalesRepository, Override, Money, Override (+17 more)

### Community 2 - "CustomerOrderPattern"
Cohesion: 0.06
Nodes (27): java.sql.ResultSet, DuckDbCustomerPatternRepository, Override, InMemoryCustomerPatternRepository, Override, MoneyCents, Override, SqliteCustomerPatternRepository (+19 more)

### Community 3 - ".place"
Cohesion: 0.07
Nodes (21): java.time.format.DateTimeParseException, LogExceptionHandler, OrderPlaced, Quantity, OrderPlacedTranslator, AnalyticsExceptionHandler, InvalidValueException, ApiError (+13 more)

### Community 4 - "LogAggregatorWiring.java"
Cohesion: 0.17
Nodes (7): LogController, LogEntryView, LogQueryPort, LogQueryService, LogAggregatorWiring, Override, LogQueryFacade

### Community 5 - "ApplicationName"
Cohesion: 0.18
Nodes (5): com.fasterxml.jackson.databind.ObjectMapper, DuckDbLogRepository, Override, ApplicationName, Override

### Community 6 - "metrics-consumer (App B)"
Cohesion: 0.08
Nodes (38): KAFKA_AUTO_CREATE_TOPICS_ENABLE=false, Listeners PLAINTEXT (9092) e PLAINTEXT_HOST (9094), Broker Kafka (apache/kafka:4.1.2), Kafka UI (kafbat/kafka-ui:8090), KRaft: nó único broker+controller, Fatores de replicação 1 para nó único, Tópico Kafka "application-logs", Gargalo: commit por mensagem no consumidor (+30 more)

### Community 7 - "org.junit.jupiter.params.ParameterizedTest"
Cohesion: 0.07
Nodes (23): java.sql.PreparedStatement, Connection, LogLevel, LogRepositoryContractTest, LogRepository, DuckDbOrderLedgerRepository, Override, InMemoryOrderLedgerRepository (+15 more)

### Community 8 - "org.springframework.context.annotation.Bean"
Cohesion: 0.12
Nodes (13): DuckDbPersistence, JsonlPersistence, LogPersistenceConfiguration, StdoutPersistence, DuckDbPersistence, InMemoryPersistence, PersistenceConfiguration, SqlitePersistence (+5 more)

### Community 9 - "MetricsController"
Cohesion: 0.15
Nodes (10): MetricsController, ProductSalesView, RevenueView, MetricsQueryPort, OrderController, PlaceOrderResponse, org.springframework.web.bind.annotation.GetMapping, org.springframework.web.bind.annotation.PostMapping (+2 more)

### Community 10 - "org.junit.jupiter.api.Test"
Cohesion: 0.07
Nodes (18): KafkaErrorHandlingConfigTest, LogFilterTest, KafkaErrorHandlingConfigTest, FakeProductSales, CustomerId, OrderPlaced, Override, ProductId (+10 more)

### Community 11 - "OrderPlaced"
Cohesion: 0.09
Nodes (10): OrderPlacedMessage, OrderPlaced, CustomerId, Override, Override, Money, Override, ProductId (+2 more)

### Community 14 - "orders-load.js"
Cohesion: 0.17
Nodes (13): acceptanceRate, CATALOG, options, orderLatency, ordersAccepted, ordersRejected, placeNormalOrder(), placeOrder() (+5 more)

### Community 15 - "package.json"
Cohesion: 0.14
Nodes (13): esbuild, @faker-js/faker, description, devDependencies, esbuild, @faker-js/faker, name, private (+5 more)

### Community 16 - "logs/adapters/config/KafkaErrorHandlingConfig.java"
Cohesion: 0.30
Nodes (8): KafkaErrorHandlingConfig, KafkaErrorHandlingConfig, org.springframework.boot.autoconfigure.condition.ConditionalOnProperty, org.springframework.boot.autoconfigure.kafka.KafkaProperties, org.springframework.boot.context.properties.EnableConfigurationProperties, org.springframework.kafka.core.KafkaOperations, org.springframework.kafka.listener.CommonErrorHandler, org.springframework.util.backoff.BackOff

### Community 17 - "org.springframework.boot.autoconfigure.SpringBootApplication"
Cohesion: 0.27
Nodes (4): LogAggregatorBootstrap, MetricsConsumerBootstrap, OrderServiceBootstrap, org.springframework.boot.autoconfigure.SpringBootApplication

### Community 34 - "StdoutLogRepository"
Cohesion: 0.26
Nodes (4): Override, StdoutLogRepository, StdoutLogRepositoryTest, org.junit.jupiter.api.AfterEach

### Community 36 - "LogEntry"
Cohesion: 0.26
Nodes (5): IngestLogPort, IngestLogService, IngestLogFacade, Override, LogEntry

### Community 39 - "AnalyticsWiring.java"
Cohesion: 0.07
Nodes (16): ApplicationLogListener, SuspicionProperties, OrderListener, OrderPlacedMessage, OrderPlacedPort, OrderPlacedHandler, RecordingPublisher, AnalyticsWiring (+8 more)

### Community 40 - "OrderServiceWiring.java"
Cohesion: 0.18
Nodes (10): KafkaDomainEventPublisher, KafkaActivityLogPublisher, KafkaOrderEventPublisher, PlaceOrderService, ActivityLogPublisher, OrderServiceWiring, ActivityLogFacade, OrderEventPublisher (+2 more)

### Community 41 - "InvalidLogException"
Cohesion: 0.24
Nodes (3): ApplicationLogMessage, LogEntryTranslator, InvalidLogException

### Community 47 - "ActivityLog"
Cohesion: 0.13
Nodes (7): ApplicationLogMessage, ActivityLog, LogLevel, ERROR, INFO, WARN, Override

### Community 50 - ".handle"
Cohesion: 0.24
Nodes (6): PlaceOrderCommand, Override, Override, PlaceOrderServiceTest, RecordingEventPublisher, RecordingLogPublisher

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
- **42 isolated node(s):** `acceptanceRate`, `CATALOG`, `options`, `orderLatency`, `ordersAccepted` (+37 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **28 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `LogEntry` connect `LogEntry` to `StdoutLogRepository`, `LogAggregatorWiring.java`, `ApplicationName`, `org.junit.jupiter.params.ParameterizedTest`, `InvalidLogException`, `org.junit.jupiter.api.Test`, `LogFilter`, `JsonlFileLogRepository`, `LogLevel`?**
  _High betweenness centrality (0.053) - this node is a cross-community bridge._
- **Why does `DeadLetterProperties` connect `DeadLetterProperties` to `logs/adapters/config/KafkaErrorHandlingConfig.java`, `org.springframework.context.annotation.Bean`, `Retry`?**
  _High betweenness centrality (0.041) - this node is a cross-community bridge._
- **Why does `DeadLetterProperties` connect `DeadLetterProperties` to `logs/adapters/config/KafkaErrorHandlingConfig.java`, `org.springframework.context.annotation.Bean`, `DeadLetterProperties`?**
  _High betweenness centrality (0.041) - this node is a cross-community bridge._
- **Are the 3 inferred relationships involving `LogEntry` (e.g. with `.omitsEmptyContext()` and `.printsTheEssentials()`) actually correct?**
  _`LogEntry` has 3 INFERRED edges - model-reasoned connections that need verification._
- **What connects `acceptanceRate`, `CATALOG`, `options` to the rest of the system?**
  _42 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `ProductSalesRecord` be split into smaller, more focused modules?**
  _Cohesion score 0.062066063538817585 - nodes in this community are weakly interconnected._
- **Should `CustomerOrderPattern` be split into smaller, more focused modules?**
  _Cohesion score 0.05998763141620284 - nodes in this community are weakly interconnected._