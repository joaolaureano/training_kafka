# Graph Report - training_kafka  (2026-08-25)

## Corpus Check
- Corpus is ~23,979 words - fits in a single context window. You may not need a graph.

## Summary
- 818 nodes · 2315 edges · 34 communities (18 shown, 16 thin omitted)
- Extraction: 86% EXTRACTED · 14% INFERRED · 0% AMBIGUOUS · INFERRED: 316 edges (avg confidence: 0.82)
- Token cost: 54,525 input · 0 output

## Community Hubs (Navigation)
- Persistência de Logs (App C)
- Repositórios de Vendas por Produto
- Padrão de Cliente e Suspeita
- Listeners Kafka e Tradutores ACL
- Caso de Uso Place Order (App A)
- Handler de OrderPlaced (App B)
- Infra Kafka e Decisões de Design
- Repositórios de Razão de Receita
- Persistência por Perfil Spring
- Controllers REST e DTOs
- Testes de Filtro e Métricas
- Modelo de Domínio de Pedidos
- Testes de CustomerOrderPattern
- Testes do Agregado Order
- Teste de Carga k6
- Build npm do Load Test
- Validação de Criação de Pedido
- Entrypoints Spring Boot
- Teste do StdoutLogRepository
- Constantes de Tópicos Kafka
- Exceção de Repositório SQLite
- POM Raiz Multi-módulo
- Módulo log-aggregator
- Módulo log-aggregator-adapters
- Módulo log-aggregator-application
- Módulo log-aggregator-domain
- Módulo metrics-consumer
- Módulo metrics-consumer-adapters
- Módulo metrics-consumer-application
- Módulo metrics-consumer-domain
- Módulo order-service
- Módulo order-service-adapters
- Módulo order-service-application
- Módulo order-service-domain

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
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` --conceptually_related_to--> `order-service (App A)`  [INFERRED]
  docker-compose.yml → README.md
- `analytics.suspicion (max-orders 5 / janela 10s)` --conceptually_related_to--> `CustomerOrderPattern (raiz de agregado)`  [INFERRED]
  metrics-consumer/metrics-consumer-adapters/src/main/resources/application.yml → README.md
- `training_kafka (laboratório Kafka + DDD)` --references--> `Kafka UI (kafbat/kafka-ui:8090)`  [EXTRACTED]
  README.md → docker-compose.yml

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Pipeline de eventos A → orders → B → application-logs → C** — readme_order_service, readme_orders_topic, readme_metrics_consumer, readme_application_logs_topic, readme_log_aggregator, docker_compose_kafka_broker [EXTRACTED 1.00]
- **Modelo de domínio do App B (3 raízes + VO derivado + handler)** — readme_productsalesrecord, readme_customerorderpattern, readme_orderrecord, readme_revenuewindow, readme_orderplacedhandler [EXTRACTED 1.00]
- **Desacoplamento de bounded contexts via contratos próprios e ACL** — readme_duplicated_event_contracts, readme_no_type_headers, readme_anticorruption_layer, metrics_consumer_metrics_consumer_adapters_src_main_resources_application_orderplacedmessage, log_aggregator_log_aggregator_adapters_src_main_resources_application_applicationlogmessage [INFERRED 0.95]

## Communities (34 total, 16 thin omitted)

### Community 0 - "Persistência de Logs (App C)"
Cohesion: 0.06
Nodes (31): com.fasterxml.jackson.databind.ObjectMapper, LogAggregatorWiring, DuckDbLogRepository, Override, ObjectMapper, Override, JsonlFileLogRepository, StoredLine (+23 more)

### Community 1 - "Repositórios de Vendas por Produto"
Cohesion: 0.07
Nodes (23): DuckDbProductSalesRepository, Override, ProductId, InMemoryProductSalesRepository, Override, Money, Override, ProductId (+15 more)

### Community 2 - "Padrão de Cliente e Suspeita"
Cohesion: 0.07
Nodes (21): java.sql.ResultSet, SuspicionProperties, DuckDbCustomerPatternRepository, Override, InMemoryCustomerPatternRepository, Override, MoneyCents, Override (+13 more)

### Community 3 - "Listeners Kafka e Tradutores ACL"
Cohesion: 0.06
Nodes (26): java.time.format.DateTimeParseException, ApplicationLogListener, ApplicationLogMessage, LogEntryTranslator, LogExceptionHandler, InvalidLogException, OrderListener, OrderPlacedMessage (+18 more)

### Community 4 - "Caso de Uso Place Order (App A)"
Cohesion: 0.07
Nodes (22): OrderServiceWiring, ApplicationLogMessage, Override, KafkaActivityLogPublisher, Override, KafkaOrderEventPublisher, PlaceOrderCommand, Override (+14 more)

### Community 5 - "Handler de OrderPlaced (App B)"
Cohesion: 0.08
Nodes (19): AnalyticsWiring, ApplicationLogMessage, Override, KafkaDomainEventPublisher, OrderPlacedHandler, FakeCustomerPatterns, FakeProductSales, CustomerId (+11 more)

### Community 6 - "Infra Kafka e Decisões de Design"
Cohesion: 0.06
Nodes (53): KAFKA_AUTO_CREATE_TOPICS_ENABLE=false, Listeners PLAINTEXT (9092) e PLAINTEXT_HOST (9094), Broker Kafka (apache/kafka:4.1.2), Kafka UI (kafbat/kafka-ui:8090), KRaft: nó único broker+controller, Fatores de replicação 1 para nó único, ApplicationLogMessage (default type do consumidor C), logs.duckdb em arquivo próprio (+45 more)

### Community 7 - "Repositórios de Razão de Receita"
Cohesion: 0.11
Nodes (14): java.sql.PreparedStatement, DuckDbOrderLedgerRepository, Override, InMemoryOrderLedgerRepository, Override, Override, SqliteOrderLedgerRepository, OrderLedgerRepositoryContractTest (+6 more)

### Community 8 - "Persistência por Perfil Spring"
Cohesion: 0.08
Nodes (17): java.sql.Connection, DuckDbPersistence, JsonlPersistence, LogPersistenceConfiguration, StdoutPersistence, DuckDbPersistence, InMemoryPersistence, PersistenceConfiguration (+9 more)

### Community 9 - "Controllers REST e DTOs"
Cohesion: 0.09
Nodes (15): LogController, MetricsController, ProductSalesView, RevenueView, MetricsQueryService, OrderController, PlaceOrderRequest, PlaceOrderResponse (+7 more)

### Community 10 - "Testes de Filtro e Métricas"
Cohesion: 0.16
Nodes (7): LogFilterTest, ProductSalesRecordTest, TimeRange, RevenueWindowTest, MoneyRules, org.junit.jupiter.api.DisplayName, org.junit.jupiter.api.Test

### Community 11 - "Modelo de Domínio de Pedidos"
Cohesion: 0.09
Nodes (10): OrderPlacedMessage, OrderPlaced, CustomerId, Override, Override, Money, Override, ProductId (+2 more)

### Community 12 - "Testes de CustomerOrderPattern"
Cohesion: 0.26
Nodes (6): BurstBehaviour, CustomerOrderPatternTest, Reconstitution, SuspicionDecision, WindowPruning, org.junit.jupiter.api.Nested

### Community 13 - "Testes do Agregado Order"
Cohesion: 0.20
Nodes (4): Override, Order, OrderTest, WhenValid

### Community 14 - "Teste de Carga k6"
Cohesion: 0.17
Nodes (13): acceptanceRate, CATALOG, options, orderLatency, ordersAccepted, ordersRejected, placeNormalOrder(), placeOrder() (+5 more)

### Community 15 - "Build npm do Load Test"
Cohesion: 0.14
Nodes (13): esbuild, @faker-js/faker, description, devDependencies, esbuild, @faker-js/faker, name, private (+5 more)

### Community 16 - "Validação de Criação de Pedido"
Cohesion: 0.22
Nodes (5): CustomerId, Money, ProductId, Quantity, WhenInvalid

### Community 17 - "Entrypoints Spring Boot"
Cohesion: 0.27
Nodes (4): LogAggregatorApplication, MetricsConsumerApplication, OrderServiceApplication, org.springframework.boot.autoconfigure.SpringBootApplication

## Knowledge Gaps
- **40 isolated node(s):** `name`, `version`, `private`, `description`, `build` (+35 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **16 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `LogEntry` connect `Persistência de Logs (App C)` to `Testes de Filtro e Métricas`, `Controllers REST e DTOs`, `Teste do StdoutLogRepository`, `Listeners Kafka e Tradutores ACL`?**
  _High betweenness centrality (0.042) - this node is a cross-community bridge._
- **Why does `ProductSalesRecord` connect `Repositórios de Vendas por Produto` to `Controllers REST e DTOs`, `Handler de OrderPlaced (App B)`, `Repositórios de Razão de Receita`?**
  _High betweenness centrality (0.037) - this node is a cross-community bridge._
- **Why does `ProductId` connect `Repositórios de Vendas por Produto` to `Listeners Kafka e Tradutores ACL`, `Handler de OrderPlaced (App B)`, `Repositórios de Razão de Receita`, `Controllers REST e DTOs`, `Testes de Filtro e Métricas`?**
  _High betweenness centrality (0.035) - this node is a cross-community bridge._
- **What connects `name`, `version`, `private` to the rest of the system?**
  _40 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Persistência de Logs (App C)` be split into smaller, more focused modules?**
  _Cohesion score 0.05701754385964912 - nodes in this community are weakly interconnected._
- **Should `Repositórios de Vendas por Produto` be split into smaller, more focused modules?**
  _Cohesion score 0.0651685393258427 - nodes in this community are weakly interconnected._
- **Should `Padrão de Cliente e Suspeita` be split into smaller, more focused modules?**
  _Cohesion score 0.06611813106082869 - nodes in this community are weakly interconnected._