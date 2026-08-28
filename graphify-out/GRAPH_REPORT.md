# Graph Report - training_kafka  (2026-08-28)

## Corpus Check
- 134 files · ~26,270 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 905 nodes · 2474 edges · 46 communities (19 shown, 27 thin omitted)
- Extraction: 87% EXTRACTED · 13% INFERRED · 0% AMBIGUOUS · INFERRED: 316 edges (avg confidence: 0.82)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `b8188057`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- org.junit.jupiter.api.DisplayName
- ProductSalesRecord
- CustomerOrderPattern
- .place
- ActivityLog
- OrderPlaced
- metrics-consumer (App B)
- ProductId
- org.springframework.context.annotation.Bean
- OrderController.java
- org.junit.jupiter.api.Test
- Order
- DeadLetterProperties
- DeadLetterProperties
- orders-load.js
- package.json
- .handle
- org.springframework.boot.autoconfigure.SpringBootApplication
- LogEntry
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
- org.junit.jupiter.params.provider.Arguments
- Retry
- Retry
- OrderPlaced
- Money
- .createsOrder
- RevenueWindowTest
- OrderId
- ApplicationLogListener
- DomainEvent
- Quantity
- SuspicionProperties

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

## Communities (46 total, 27 thin omitted)

### Community 0 - "org.junit.jupiter.api.DisplayName"
Cohesion: 0.14
Nodes (5): WindowPruning, MoneyRules, WhenInvalid, org.junit.jupiter.api.DisplayName, org.junit.jupiter.api.Nested

### Community 1 - "ProductSalesRecord"
Cohesion: 0.07
Nodes (22): DuckDbProductSalesRepository, Override, ProductId, InMemoryProductSalesRepository, Override, Money, Override, ProductId (+14 more)

### Community 2 - "CustomerOrderPattern"
Cohesion: 0.06
Nodes (25): java.sql.PreparedStatement, java.sql.ResultSet, DuckDbCustomerPatternRepository, Override, InMemoryCustomerPatternRepository, Override, MoneyCents, Override (+17 more)

### Community 3 - ".place"
Cohesion: 0.06
Nodes (25): java.time.format.DateTimeParseException, ApplicationLogMessage, LogEntryTranslator, LogExceptionHandler, InvalidLogException, OrderPlacedMessage, OrderPlaced, Quantity (+17 more)

### Community 4 - "ActivityLog"
Cohesion: 0.14
Nodes (7): ApplicationLogMessage, Override, ActivityLog, LogLevel, ERROR, INFO, WARN

### Community 5 - "OrderPlaced"
Cohesion: 0.11
Nodes (14): OrderPlacedHandler, FakeCustomerPatterns, FakeLedger, FakeProductSales, CustomerId, OrderPlaced, Override, ProductId (+6 more)

### Community 6 - "metrics-consumer (App B)"
Cohesion: 0.06
Nodes (53): KAFKA_AUTO_CREATE_TOPICS_ENABLE=false, Listeners PLAINTEXT (9092) e PLAINTEXT_HOST (9094), Broker Kafka (apache/kafka:4.1.2), Kafka UI (kafbat/kafka-ui:8090), KRaft: nó único broker+controller, Fatores de replicação 1 para nó único, ApplicationLogMessage (default type do consumidor C), logs.duckdb em arquivo próprio (+45 more)

### Community 7 - "ProductId"
Cohesion: 0.09
Nodes (19): DuckDbOrderLedgerRepository, Override, InMemoryOrderLedgerRepository, Override, Override, SqliteOrderLedgerRepository, MetricsController, ProductSalesView (+11 more)

### Community 8 - "org.springframework.context.annotation.Bean"
Cohesion: 0.05
Nodes (38): java.sql.Connection, KafkaTemplate, KafkaErrorHandlingConfig, DuckDbPersistence, JsonlPersistence, LogPersistenceConfiguration, StdoutPersistence, Topics (+30 more)

### Community 9 - "OrderController.java"
Cohesion: 0.22
Nodes (5): OrderController, PlaceOrderRequest, PlaceOrderResponse, PlaceOrderUseCase, org.springframework.web.bind.annotation.PostMapping

### Community 10 - "org.junit.jupiter.api.Test"
Cohesion: 0.16
Nodes (5): KafkaErrorHandlingConfigTest, LogFilterTest, KafkaErrorHandlingConfigTest, org.junit.jupiter.api.Test, org.springframework.boot.test.context.runner.ApplicationContextRunner

### Community 11 - "Order"
Cohesion: 0.23
Nodes (4): Override, Order, Override, ProductId

### Community 14 - "orders-load.js"
Cohesion: 0.17
Nodes (13): acceptanceRate, CATALOG, options, orderLatency, ordersAccepted, ordersRejected, placeNormalOrder(), placeOrder() (+5 more)

### Community 15 - "package.json"
Cohesion: 0.14
Nodes (13): esbuild, @faker-js/faker, description, devDependencies, esbuild, @faker-js/faker, name, private (+5 more)

### Community 16 - ".handle"
Cohesion: 0.24
Nodes (6): PlaceOrderCommand, Override, Override, PlaceOrderServiceTest, RecordingEventPublisher, RecordingLogPublisher

### Community 17 - "org.springframework.boot.autoconfigure.SpringBootApplication"
Cohesion: 0.27
Nodes (4): LogAggregatorApplication, MetricsConsumerApplication, OrderServiceApplication, org.springframework.boot.autoconfigure.SpringBootApplication

### Community 18 - "LogEntry"
Cohesion: 0.05
Nodes (36): com.fasterxml.jackson.databind.ObjectMapper, LogAggregatorWiring, DuckDbLogRepository, Override, ObjectMapper, Override, JsonlFileLogRepository, StoredLine (+28 more)

### Community 37 - "OrderPlaced"
Cohesion: 0.24
Nodes (4): OrderPlacedMessage, OrderPlaced, CustomerId, Override

### Community 42 - "ApplicationLogListener"
Cohesion: 0.24
Nodes (8): ApplicationLogMessage, dev.joaolaureano.trainingkafka.analytics.application.OrderPlacedHandler, dev.joaolaureano.trainingkafka.logs.application.IngestLogService, ApplicationLogListener, OrderListener, OrderPlacedMessage, org.springframework.kafka.annotation.KafkaListener, org.springframework.stereotype.Component

## Knowledge Gaps
- **40 isolated node(s):** `acceptanceRate`, `CATALOG`, `options`, `orderLatency`, `ordersAccepted` (+35 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **27 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `DeadLetterProperties` connect `DeadLetterProperties` to `org.springframework.context.annotation.Bean`, `Retry`, `SuspicionProperties`?**
  _High betweenness centrality (0.042) - this node is a cross-community bridge._
- **Why does `DeadLetterProperties` connect `DeadLetterProperties` to `org.springframework.context.annotation.Bean`, `Retry`, `SuspicionProperties`?**
  _High betweenness centrality (0.042) - this node is a cross-community bridge._
- **Why does `LogEntry` connect `LogEntry` to `org.junit.jupiter.api.Test`, `.place`?**
  _High betweenness centrality (0.040) - this node is a cross-community bridge._
- **What connects `acceptanceRate`, `CATALOG`, `options` to the rest of the system?**
  _40 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `org.junit.jupiter.api.DisplayName` be split into smaller, more focused modules?**
  _Cohesion score 0.1422924901185771 - nodes in this community are weakly interconnected._
- **Should `ProductSalesRecord` be split into smaller, more focused modules?**
  _Cohesion score 0.06629243517775996 - nodes in this community are weakly interconnected._
- **Should `CustomerOrderPattern` be split into smaller, more focused modules?**
  _Cohesion score 0.059575345289631 - nodes in this community are weakly interconnected._