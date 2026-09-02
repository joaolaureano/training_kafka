# training_kafka

Laboratório de Engenharia de Dados: três microsserviços Spring Boot ligados por Kafka,
modelados com **DDD tático** e **Ports & Adapters**, com carga gerada por k6.

O objetivo aqui não é o produto — é o desenho. A regra que governa tudo:

> **O domínio não sabe onde nem como os dados são persistidos.**
> Isso é decisão de wiring, tomada ao montar o sistema, nunca ao modelar o domínio.

E essa regra não depende de disciplina: os módulos `*-domain` declaram **zero** dependências,
então uma `@Service` escrita ali simplesmente não compila.

---

## O fluxo

```
  [k6]
    │  POST /orders
    ▼
┌─────────────────────┐   OrderPlaced    ┌──────────────────────┐
│  App A              │ ───────────────► │  App B               │
│  order-service      │  tópico "orders" │  metrics-consumer    │
│  :8080              │  key=customerId  │  :8081               │
└──────────┬──────────┘                  └──────────┬───────────┘
           │                                        │
           │  logs estruturados                     │  SuspiciousPatternDetected
           │                                        │
           ▼                                        ▼
      ┌──────────────────────────────────────────────────┐
      │            tópico "audit-events"             │
      └───────────────────────┬──────────────────────────┘
                              ▼
                   ┌──────────────────────┐
                   │  App C               │
                   │  audit-service      │
                   │  :8082               │
                   └──────────────────────┘
```

## Os três serviços

| App | Módulo | Porta | O que faz |
|---|---|---|---|
| **A** | `order-service` | 8080 | Recebe pedidos por HTTP, valida no domínio, publica em `orders` (particionado por `customerId`) e registra logs estruturados |
| **B** | `metrics-consumer` | 8081 | Consome `orders`, acumula métricas de venda e detecta padrões suspeitos de pedido |
| **C** | `audit-service` | 8082 | Consome `audit-events`, persiste e responde consultas por nível/app/período |

---

## Pré-requisitos

```bash
brew install openjdk@21 maven
brew install colima docker docker-compose
brew install k6 node          # só para os testes de carga
```

`openjdk@21` é *keg-only*: o Homebrew não coloca o `java` no PATH. Ou você resolve de vez —

```bash
echo 'export PATH="/opt/homebrew/opt/openjdk@21/bin:$PATH"' >> ~/.zshrc
```

— ou usa o caminho completo (`/opt/homebrew/opt/openjdk@21/bin/java`) nos comandos abaixo.

O plugin do Compose precisa ser registrado uma vez em `~/.docker/config.json`:

```json
{ "cliPluginsExtraDirs": ["/opt/homebrew/lib/docker/cli-plugins"] }
```

---

## Subindo tudo

### 1. Infraestrutura

```bash
colima start --cpus 4 --memory 8 --disk 40
docker compose up -d
```

Kafka em modo KRaft (sem ZooKeeper) e Kafka UI em **http://localhost:8090**.

O broker anuncia dois listeners: `kafka:9092` para a rede do Compose e `localhost:9094`
para processos no macOS. É o detalhe que mais quebra Kafka em Docker — um listener só
não consegue anunciar um endereço válido para os dois lados ao mesmo tempo.

A auto-criação de tópicos está **desligada** de propósito: assim um nome errado falha alto,
em vez de criar silenciosamente um tópico fantasma com 1 partição. Os tópicos são declarados
pelo App A no boot, com 3 partições cada.

### 2. Build

```bash
mvn clean install
```

### 3. Os três serviços

Cada um numa aba de terminal:

```bash
java -jar order-service/order-service-bootstrap/target/order-service-bootstrap-0.1.0-SNAPSHOT.jar
java -jar metrics-consumer/metrics-consumer-bootstrap/target/metrics-consumer-bootstrap-0.1.0-SNAPSHOT.jar
java -jar audit-service/audit-service-bootstrap/target/audit-service-bootstrap-0.1.0-SNAPSHOT.jar
```

### 4. Um pedido

```bash
curl -X POST localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-1","product":"Teclado","quantity":2,"amount":199.90}'
```

```bash
curl -s "localhost:8081/metrics/top-products?limit=5"
curl -s "localhost:8082/audit-events?level=WARN&limit=10"
```

---

## Trocando a persistência

É aqui que o desenho se prova. Os mesmos serviços, com implementações de repositório
completamente diferentes, **sem tocar numa linha de domínio**:

```bash
# App B
java -jar .../metrics-consumer-bootstrap-0.1.0-SNAPSHOT.jar --spring.profiles.active=inmemory   # default
java -jar .../metrics-consumer-bootstrap-0.1.0-SNAPSHOT.jar --spring.profiles.active=sqlite
java -jar .../metrics-consumer-bootstrap-0.1.0-SNAPSHOT.jar --spring.profiles.active=duckdb

# App C
java -jar .../audit-service-bootstrap-0.1.0-SNAPSHOT.jar --spring.profiles.active=stdout       # default
java -jar .../audit-service-bootstrap-0.1.0-SNAPSHOT.jar --spring.profiles.active=jsonl
java -jar .../audit-service-bootstrap-0.1.0-SNAPSHOT.jar --spring.profiles.active=duckdb
```

| App | Profile | Onde os dados param | Arquivo |
|---|---|---|---|
| B | `inmemory` | memória (somem ao reiniciar) | — |
| B | `sqlite` | SQLite | `data/metrics.db` |
| B | `duckdb` | DuckDB | `data/metrics.duckdb` |
| C | `stdout` | console — **não armazena** | — |
| C | `jsonl` | uma linha JSON por registro | `data/audit.jsonl` |
| C | `duckdb` | DuckDB, tabela `audit_events` | `data/audit.duckdb` |

> **O adapter `stdout` não armazena nada**, então `GET /audit-events` sempre devolve lista vazia.
> Isso é uma limitação declarada no javadoc do Port e fixada por um teste — uma lista vazia
> que significa "não há onde procurar" é diferente de uma que significa "nada casou".

> **DuckDB aceita um único processo escritor por arquivo.** Por isso o App C usa
> `data/audit.duckdb`, separado do App B. Apontar os dois para o mesmo arquivo com ambos
> no ar falha assim:
> ```
> IO Error: Could not set lock on file ".../data/metrics.duckdb":
> Conflicting lock is held in .../java (PID 61193)
> ```

---

## Testes de carga

```bash
cd load-tests
npm install
npm test                     # build + rampa completa (~3min30)
npm run smoke                # versão curta, 5 VUs por 15s
```

O runtime do k6 não é Node e não resolve `node_modules`, então o script passa por um
bundle com esbuild antes de rodar — é o que permite usar `@faker-js/faker`.

Dois cenários simultâneos:

- **`normal_traffic`** — rampa `10 → 100 → 500` VUs, cada requisição com cliente próprio.
  Não deve acionar detecção nenhuma.
- **`suspicious_burst`** — 5 VUs martelando um pool fixo de 5 clientes, sem pausa, para
  acionar `CustomerOrderPattern.isSuspicious()` de forma determinística e não por acidente
  estatístico.

Depois da carga, o efeito é visível nas duas pontas:

```bash
curl -s "localhost:8081/metrics/top-products?limit=5"
curl -s "localhost:8082/audit-events?level=WARN&app=metrics-consumer&limit=10"
```

### O que a carga revelou

Rodando a rampa completa numa máquina com 4 CPUs (Colima), com App B em DuckDB:

| | |
|---|---|
| Pedidos aceitos em 3 min | **2.342.364** |
| Rejeitados | 0 |
| Latência p(95) / p(99) | 47 ms / — |
| Throughput do App A | ~13.000 pedidos/s |

O App A aguenta. **Os consumidores não**, e por dois motivos que valem mais que o
teste em si:

**1. Escrita por mensagem, com commit por mensagem.** Medindo a taxa de drenagem do
mesmo backlog, só trocando o profile:

| Profile | Throughput |
|---|---|
| `inmemory` | ~3.150 msg/s |
| `duckdb` | ~383 msg/s |
| `sqlite` | ~283 msg/s |

Os dois bancos ficam ~10× abaixo da memória, e SQLite fica *abaixo* do DuckDB — ou seja,
não é uma questão de OLAP versus OLTP. Cada mensagem dispara ~6 idas ao banco
(`append` no ledger, `findOrCreate` + `save` de duas raízes de agregado), cada uma com
commit próprio e portanto `fsync`. O gargalo é a quantidade de commits, não o motor.

O caminho para melhorar é consumo em lote: acumular N mensagens, processar e commitar
uma vez só. Isso muda o adapter, não o domínio — o que é justamente o ponto.

**2. Distribuição desigual de partição no tópico `audit-events`.** Depois da carga:

```
partição 0:          3 mensagens
partição 1:          0 mensagens
partição 2:  1.320.709 mensagens
```

A chave da mensagem é o **nome do app**, então todos os logs de um serviço têm a mesma
chave e caem sempre na mesma partição. As outras duas ficam ociosas, e o paralelismo do
App C fica preso em 1 consumidor por app — por mais que se aumente `concurrency`.

Foi uma escolha ruim de chave da minha parte: ela preserva a ordem cronológica dos logs
de cada serviço, mas o custo é desproporcional. Para logs, a ordem global por app raramente
importa; deixar a chave nula (distribuição round-robin) devolveria as três partições ao jogo.
Ficou registrado aqui em vez de silenciosamente corrigido, porque o erro é mais instrutivo
que o acerto.

---

## A arquitetura

### Por que módulos Maven separados

Cada serviço são quatro módulos, e a dependência só aponta para dentro:

| Módulo | Depende de | Dependências externas |
|---|---|---|
| `*-domain` | **nada** | só JUnit e AssertJ, em escopo de teste |
| `*-application` | `*-domain` | nenhuma |
| `*-adapters` | `*-domain` | Spring, Kafka, Jackson, drivers JDBC |
| `*-bootstrap` | os três acima | Spring Boot (é onde o `main` mora) |

Convenção não segura isso; o compilador sim. Além disso, o `maven-enforcer-plugin` barra
dependências de infraestrutura nos módulos de domínio com mensagem explicativa — inclusive
as transitivas — e barra a dependência `*-adapters → *-application` no módulo de adapters.

### Por que o módulo `*-bootstrap`

Repare na tabela: **o adapter não depende da camada de aplicação**. Isso é deliberado, e é
o que o módulo `*-bootstrap` existe para permitir.

Um adapter de entrada precisa que alguém execute o caso de uso — mas não precisa saber
quem. Então ele declara, no próprio pacote, a interface de que precisa:
`PlaceOrderPort`, `AuditQueryPort`, `OrderPlacedPort`. Do outro lado, a camada de aplicação
oferece o que sabe fazer. Nenhum dos dois módulos importa o outro; quem costura os dois
lados é uma **facade** no `*-bootstrap` — o único módulo que enxerga tudo, porque enxergar
tudo é exatamente o trabalho dele.

O mesmo vale para o lado de saída: `KafkaActivityLogPublisher` publica logs com uma
assinatura em tipos crus e não implementa `ActivityLogPublisher`; a facade
`ActivityLogFacade`, no bootstrap, é que implementa o Port da aplicação e delega.

O que mora no `*-bootstrap`: a classe `main`, o `application.yml`, o `*Wiring` (quem
implementa cada Port), a escolha de persistência por profile e as facades. O que fica no
`*-adapters`: só tecnologia — controllers, listeners, repositórios, e a configuração
técnica que não escolhe implementação nenhuma (criação de tópicos, error handler da DLQ).

O POM raiz **não herda** de `spring-boot-starter-parent`, só importa o BOM de versões:
herdar traria o plugin do Spring para todos os módulos, inclusive os de domínio. O preço
dessa escolha é ter que declarar à mão coisas que o parent daria de graça — como
`maven.compiler.parameters`, sem a qual todo `@RequestParam` falha em runtime.

### O modelo de domínio do App B

Três raízes de agregado, porque são três fronteiras de consistência distintas:

- **`ProductSalesRecord`** (id: `ProductId`) — unidades e faturamento sempre mudam juntos.
  Garantido pela forma da classe: existe um único mutador, e ele move os três campos numa
  operação só.
- **`CustomerOrderPattern`** (id: `CustomerId`) — mantém a janela de pedidos recentes e
  **decide sozinho** se o padrão é suspeito. Nenhum serviço externo inspeciona a lista para
  concluir algo; pergunta-se ao agregado. O alerta dispara na *transição* de normal para
  suspeito, não a cada pedido acima do limiar — sem isso, uma rajada de 500 pedidos geraria
  centenas de alertas idênticos.
- **`OrderRecord`** — ledger append-only, imutável. É o que torna possível responder
  faturamento de **qualquer** período; `ProductSalesRecord` acumula totais sem dimensão
  temporal, e deve continuar assim.

E **`RevenueWindow`**, value object derivado por agregação sobre o ledger — nunca armazenado,
para não criar uma segunda verdade sobre o mesmo fato.

O `OrderPlacedHandler` que costura tudo isso **não tem um único `if` de negócio**. Esse é o
critério de qualidade do modelo: se uma regra precisar aparecer ali, é porque vazou de um
agregado, e o conserto é movê-la de volta — não escrever o `if`.

### Por que os contratos de evento são duplicados

Cada serviço tem sua **própria** classe para o JSON que trafega no tópico. Compartilhar uma
classe entre eles os colaria pelo classpath: uma refatoração interna no App A quebraria a
compilação do App B. O preço são seis nomes de campo repetidos; o retorno é que os bounded
contexts evoluem sozinhos, com a tradução acontecendo numa Anticorruption Layer explícita
(`OrderPlacedTranslator`, `AuditEventTranslator`).

Na mesma linha, `spring.json.add.type.headers=false`: com o header de tipo, o consumidor
precisaria conhecer o nome completo da classe do produtor para desserializar.

### Dinheiro

`Money` é um value object com duas casas decimais, e no banco vira **centavos inteiros**.
SQLite não tem tipo decimal de verdade (guardaria como `REAL`, com erro de ponto flutuante)
e `TEXT` não permitiria `SUM()` nas consultas de faturamento. Um `BIGINT` de centavos é
exato, somável, e se comporta igual nos dois bancos.

### Concorrência

`spring.kafka.listener.concurrency: 1` no App B, de propósito.

`CustomerOrderPattern` é seguro por construção — o tópico é particionado por `customerId`,
então cada cliente sempre cai na mesma partição. Mas `ProductSalesRecord` **não**: o mesmo
produto aparece em pedidos de clientes diferentes, logo em partições diferentes, logo em
threads diferentes, e dois ciclos `findOrCreate → registerSale → save` concorrentes perderiam
uma venda.

Aumentar esse número exige antes introduzir controle de versão otimista nos repositórios.
Ficou como exercício explícito, não como esquecimento.

---

## Testes

```bash
mvn test
```

140 testes. Os mais interessantes são os **de contrato**: a mesma bateria, escrita puramente
em vocabulário de domínio, roda contra todas as implementações de cada Port —
`{ProductSales, CustomerPattern, OrderLedger} × {inMemory, SQLite, DuckDB}` no App B e
`AuditRepository × {JSONL, DuckDB}` no App C.

Se algum desses testes precisasse de um `if (isSqlite)`, seria a prova de que o Port foi
moldado por acidente em torno de uma tecnologia. Nenhum precisou.

Os testes de domínio não sobem contexto Spring nem usam mock framework: rodam em
milissegundos, com fakes de cinco linhas. Isso só é possível porque o domínio não depende de
nada — é o retorno prático de tê-lo isolado.

---

## Estrutura

```
training_kafka/
├── docker-compose.yml            Kafka (KRaft) + Kafka UI
├── pom.xml                       POM raiz: versões, sem herdar do Spring Boot parent
├── data/                         arquivos gerados em runtime (gitignored)
│
├── order-service/                App A
│   ├── order-service-domain/         Order, Money, Quantity, Violations
│   ├── order-service-application/    PlaceOrderService
│   ├── order-service-adapters/       REST, Kafka
│   └── order-service-bootstrap/      main, application.yml, wiring, facades
│
├── metrics-consumer/             App B
│   ├── metrics-consumer-domain/      3 agregados, 4 Ports
│   ├── metrics-consumer-application/ OrderPlacedHandler, MetricsQueryService
│   ├── metrics-consumer-adapters/    Kafka, REST, 9 implementações de repositório
│   └── metrics-consumer-bootstrap/   main, application.yml, wiring, facades
│
├── audit-service/               App C
│   ├── audit-service-domain/        AuditEvent, AuditFilter, AuditRepository
│   ├── audit-service-application/   IngestAuditService, AuditQueryService
│   ├── audit-service-adapters/      Kafka, REST, 3 implementações
│   └── audit-service-bootstrap/     main, application.yml, wiring, facades
│
└── load-tests/                   k6 + faker, empacotado com esbuild
```

## Endpoints

| Método | Endpoint | Descrição |
|---|---|---|
| `POST` | `:8080/orders` | Registra um pedido. `202` se aceito, `400` com a lista completa de violações se não |
| `GET` | `:8081/metrics/top-products?limit=10` | Produtos mais vendidos |
| `GET` | `:8081/metrics/revenue?hours=24&product=X` | Faturamento e ticket médio no período |
| `GET` | `:8082/audit-events?level=WARN&app=X&limit=50` | Auditoria por severidade mínima, app e período |
| `GET` | `:808{0,1,2}/actuator/health` | Saúde de cada serviço |

`POST /orders` responde **202 Accepted**, e não 201: o pedido foi publicado no tópico, mas a
agregação do App B acontece de forma assíncrona. Prometer "Created" seria mentir sobre o que
já terminou.

O filtro `level` é por severidade **mínima** — pedir `WARN` traz `WARN` e `ERROR`. É o que
alguém investigando um incidente espera, e evita ter que consultar duas vezes.
