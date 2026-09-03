# Graph Report - training_kafka  (2026-09-03)

## Corpus Check
- 155 files · ~27,610 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 936 nodes · 2329 edges · 60 communities (30 shown, 30 thin omitted)
- Extraction: 89% EXTRACTED · 11% INFERRED · 0% AMBIGUOUS · INFERRED: 256 edges (avg confidence: 0.82)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `9b7d00bd`
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
- ProductId
- org.springframework.context.annotation.Bean
- .place
- org.junit.jupiter.api.Test
- Order
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
- OrderServiceWiring.java
- Retry
- AuditRepository
- metrics-consumer
- metrics-consumer-adapters
- metrics-consumer-application
- metrics-consumer-domain
- order-service
- order-service-adapters
- order-service-application
- order-service-domain
- JsonlFileAuditRepository
- AuditLevel
- OrderPlaced
- audit-service
- audit-service-adapters
- InvalidAuditException
- analytics/adapters/messaging/AuditEventMessage.java
- audit-service-application
- org.apache.kafka.clients.admin.NewTopic
- Topics
- audit-service-bootstrap
- audit-service-domain
- AuditEventMessage
- OrderPlacedMessage
- DomainEvent
- ActivityLog
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
1. `ProductSalesRecord` - 43 edges
2. `AuditEvent` - 43 edges
3. `ProductId` - 39 edges
4. `AuditRepository` - 33 edges
5. `AuditFilter` - 30 edges
6. `TimeRange` - 30 edges
7. `ApplicationName` - 27 edges
8. `RevenueWindow` - 25 edges
9. `ProductSalesRepository` - 22 edges
10. `Order` - 22 edges

## Surprising Connections (you probably didn't know these)
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` --conceptually_related_to--> `order-service (App A)`  [INFERRED]
  docker-compose.yml → README.md
- `training_kafka (laboratório Kafka + DDD)` --references--> `Kafka UI (kafbat/kafka-ui:8090)`  [EXTRACTED]
  README.md → docker-compose.yml
- `training_kafka (laboratório Kafka + DDD)` --references--> `Broker Kafka (apache/kafka:4.1.2)`  [EXTRACTED]
  README.md → docker-compose.yml
- `FraudStreamsConfiguration` --references--> `FraudProperties`  [EXTRACTED]
  fraud-service/fraud-service-bootstrap/src/main/java/dev/joaolaureano/trainingkafka/fraud/bootstrap/config/FraudStreamsConfiguration.java → fraud-service/fraud-service-bootstrap/src/main/java/dev/joaolaureano/trainingkafka/fraud/bootstrap/config/FraudProperties.java
- `OrderController` --references--> `PlaceOrderPort`  [EXTRACTED]
  order-service/order-service-adapters/src/main/java/dev/joaolaureano/trainingkafka/orders/adapters/web/OrderController.java → order-service/order-service-adapters/src/main/java/dev/joaolaureano/trainingkafka/orders/adapters/web/PlaceOrderPort.java

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Pipeline de eventos A → orders → B → application-logs → C** — readme_order_service, readme_orders_topic, readme_metrics_consumer, readme_application_logs_topic, readme_log_aggregator, docker_compose_kafka_broker [EXTRACTED 1.00]
- **Modelo de domínio do App B (3 raízes + VO derivado + handler)** — readme_productsalesrecord, readme_customerorderpattern, readme_orderrecord, readme_revenuewindow, readme_orderplacedhandler [EXTRACTED 1.00]

## Communities (60 total, 30 thin omitted)

### Community 0 - "OrderId"
Cohesion: 0.12
Nodes (9): PlaceOrderPort, PlaceOrderRequest, PlaceOrderResponse, PlaceOrderUseCase, Override, PlaceOrderFacade, Override, OrderId (+1 more)

### Community 1 - "ProductSalesRecord"
Cohesion: 0.06
Nodes (23): java.sql.ResultSet, DuckDbProductSalesRepository, Override, ProductId, InMemoryProductSalesRepository, Override, Money, MoneyCents (+15 more)

### Community 3 - "TestRepositories"
Cohesion: 0.19
Nodes (3): Connection, TestRepositories, org.junit.jupiter.params.provider.Arguments

### Community 5 - "ApplicationName"
Cohesion: 0.23
Nodes (5): DuckDbAuditRepository, Override, ApplicationName, Override, com.fasterxml.jackson.databind.ObjectMapper

### Community 6 - "metrics-consumer (App B)"
Cohesion: 0.08
Nodes (38): KAFKA_AUTO_CREATE_TOPICS_ENABLE=false, Listeners PLAINTEXT (9092) e PLAINTEXT_HOST (9094), Broker Kafka (apache/kafka:4.1.2), Kafka UI (kafbat/kafka-ui:8090), KRaft: nó único broker+controller, Fatores de replicação 1 para nó único, Tópico Kafka "application-logs", Gargalo: commit por mensagem no consumidor (+30 more)

### Community 7 - "ProductId"
Cohesion: 0.06
Nodes (26): java.sql.PreparedStatement, DuckDbOrderLedgerRepository, Override, InMemoryOrderLedgerRepository, Override, Override, SqliteOrderLedgerRepository, MetricsController (+18 more)

### Community 8 - "org.springframework.context.annotation.Bean"
Cohesion: 0.17
Nodes (13): AuditPersistenceConfiguration, DuckDbPersistence, JsonlPersistence, StdoutPersistence, java.sql.Connection, DuckDbPersistence, InMemoryPersistence, PersistenceConfiguration (+5 more)

### Community 9 - ".place"
Cohesion: 0.06
Nodes (24): AuditExceptionHandler, java.time.format.DateTimeParseException, OrderPlacedMessage, OrderPlaced, Quantity, OrderPlacedTranslator, AnalyticsExceptionHandler, CustomerId (+16 more)

### Community 10 - "org.junit.jupiter.api.Test"
Cohesion: 0.07
Nodes (17): KafkaErrorHandlingConfigTest, AuditFilterTest, KafkaErrorHandlingConfigTest, ProductSalesRecordTest, TimeRange, RevenueWindowTest, PlaceOrderCommand, Override (+9 more)

### Community 11 - "Order"
Cohesion: 0.08
Nodes (12): OrderPlacedMessage, OrderPlaced, CustomerId, Override, Override, Money, Override, Order (+4 more)

### Community 12 - "FraudTopology.java"
Cohesion: 0.06
Nodes (32): AuditEventMessage, OrderPlacedMessage, Topics, CustomerFraudState, Override, OccurredAtTimestampExtractor, FraudDetectionService, FraudTopology (+24 more)

### Community 13 - "DeadLetterProperties"
Cohesion: 0.13
Nodes (3): FraudProperties, DeadLetterProperties, org.springframework.boot.context.properties.ConfigurationProperties

### Community 14 - "orders-load.js"
Cohesion: 0.17
Nodes (13): acceptanceRate, CATALOG, options, orderLatency, ordersAccepted, ordersRejected, placeNormalOrder(), placeOrder() (+5 more)

### Community 15 - "package.json"
Cohesion: 0.14
Nodes (13): esbuild, @faker-js/faker, description, devDependencies, esbuild, @faker-js/faker, name, private (+5 more)

### Community 16 - "audit/adapters/config/KafkaErrorHandlingConfig.java"
Cohesion: 0.29
Nodes (7): KafkaErrorHandlingConfig, KafkaErrorHandlingConfig, org.springframework.boot.autoconfigure.condition.ConditionalOnProperty, org.springframework.boot.autoconfigure.kafka.KafkaProperties, org.springframework.kafka.core.KafkaOperations, org.springframework.kafka.listener.CommonErrorHandler, org.springframework.util.backoff.BackOff

### Community 17 - "org.springframework.boot.autoconfigure.SpringBootApplication"
Cohesion: 0.21
Nodes (5): AuditServiceBootstrap, FraudServiceBootstrap, MetricsConsumerBootstrap, OrderServiceBootstrap, org.springframework.boot.autoconfigure.SpringBootApplication

### Community 18 - "AuditServiceWiring.java"
Cohesion: 0.15
Nodes (7): IngestAuditPort, AuditQueryService, IngestAuditService, AuditServiceWiring, AuditQueryFacade, IngestAuditFacade, Override

### Community 19 - "AuditEvent"
Cohesion: 0.24
Nodes (5): Override, StdoutAuditRepository, StdoutAuditRepositoryTest, Override, AuditEvent

### Community 22 - "AuditFilter"
Cohesion: 0.12
Nodes (6): AuditController, AuditEventView, AuditQueryPort, AuditFilter, TimeRange, org.springframework.web.bind.annotation.GetMapping

### Community 23 - "OrderServiceWiring.java"
Cohesion: 0.21
Nodes (8): KafkaActivityLogPublisher, KafkaOrderEventPublisher, PlaceOrderService, ActivityLogPublisher, OrderServiceWiring, ActivityLogFacade, OrderEventPublisher, org.springframework.kafka.core.KafkaTemplate

### Community 25 - "AuditRepository"
Cohesion: 0.42
Nodes (5): AuditRepositoryContractTest, AuditLevel, AuditRepository, org.junit.jupiter.params.ParameterizedTest, org.junit.jupiter.params.provider.MethodSource

### Community 34 - "JsonlFileAuditRepository"
Cohesion: 0.33
Nodes (4): ObjectMapper, Override, JsonlFileAuditRepository, StoredLine

### Community 35 - "AuditLevel"
Cohesion: 0.32
Nodes (7): AuditLevel, DEBUG, ERROR, INFO, WARN, isAtLeast(), parse()

### Community 36 - "OrderPlaced"
Cohesion: 0.08
Nodes (29): dev.joaolaureano.trainingkafka.analytics.adapters.messaging.OrderPlacedPort, dev.joaolaureano.trainingkafka.analytics.adapters.web.MetricsQueryPort, dev.joaolaureano.trainingkafka.analytics.application.MetricsQueryService, dev.joaolaureano.trainingkafka.analytics.domain.model.CustomerId, dev.joaolaureano.trainingkafka.analytics.domain.model.Money, dev.joaolaureano.trainingkafka.analytics.domain.model.OrderId, dev.joaolaureano.trainingkafka.analytics.domain.model.OrderRecord, dev.joaolaureano.trainingkafka.analytics.domain.model.ProductId (+21 more)

### Community 39 - "InvalidAuditException"
Cohesion: 0.16
Nodes (6): AuditEventListener, AuditEventMessage, AuditEventTranslator, InvalidAuditException, org.springframework.kafka.annotation.KafkaListener, org.springframework.stereotype.Component

### Community 42 - "org.apache.kafka.clients.admin.NewTopic"
Cohesion: 0.28
Nodes (3): KafkaTopicsConfig, Topics, org.apache.kafka.clients.admin.NewTopic

### Community 49 - "DomainEvent"
Cohesion: 0.16
Nodes (5): Override, Override, RecordingEventPublisher, RecordingLogPublisher, DomainEvent

### Community 50 - "ActivityLog"
Cohesion: 0.13
Nodes (7): AuditEventMessage, ActivityLog, AuditLevel, ERROR, INFO, WARN, Override

### Community 52 - "FraudStreamsConfiguration.java"
Cohesion: 0.43
Nodes (5): FraudStreamsConfiguration, KafkaStreamsConfiguration, org.springframework.boot.context.properties.EnableConfigurationProperties, org.springframework.kafka.annotation.EnableKafkaStreams, org.springframework.kafka.config.KafkaStreamsConfiguration

### Community 53 - "Anticorruption Layer de tradução na fronteira"
Cohesion: 0.50
Nodes (5): Anticorruption Layer de tradução na fronteira, Contratos de evento duplicados por serviço, LogEntryTranslator, spring.json.add.type.headers=false, OrderPlacedTranslator

## Knowledge Gaps
- **48 isolated node(s):** `fraud-service-adapters`, `fraud-service-application`, `fraud-service-bootstrap`, `fraud-service-domain`, `fraud-service` (+43 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **30 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `AuditEvent` connect `AuditEvent` to `JsonlFileAuditRepository`, `AuditLevel`, `ApplicationName`, `InvalidAuditException`, `org.junit.jupiter.api.Test`, `AuditServiceWiring.java`, `AuditFilter`, `AuditRepository`?**
  _High betweenness centrality (0.054) - this node is a cross-community bridge._
- **Why does `DeadLetterProperties` connect `DeadLetterProperties` to `Retry`, `audit/adapters/config/KafkaErrorHandlingConfig.java`, `org.apache.kafka.clients.admin.NewTopic`?**
  _High betweenness centrality (0.041) - this node is a cross-community bridge._
- **Why does `DeadLetterProperties` connect `DeadLetterProperties` to `audit/adapters/config/KafkaErrorHandlingConfig.java`, `DeadLetterProperties`?**
  _High betweenness centrality (0.041) - this node is a cross-community bridge._
- **Are the 3 inferred relationships involving `AuditEvent` (e.g. with `.omitsEmptyContext()` and `.printsTheEssentials()`) actually correct?**
  _`AuditEvent` has 3 INFERRED edges - model-reasoned connections that need verification._
- **What connects `fraud-service-adapters`, `fraud-service-application`, `fraud-service-bootstrap` to the rest of the system?**
  _48 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `OrderId` be split into smaller, more focused modules?**
  _Cohesion score 0.12 - nodes in this community are weakly interconnected._
- **Should `ProductSalesRecord` be split into smaller, more focused modules?**
  _Cohesion score 0.06139512661251792 - nodes in this community are weakly interconnected._