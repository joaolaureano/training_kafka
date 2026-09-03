import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import { faker } from '@faker-js/faker';

/*
 * Carga sobre o Order Service.
 *
 * Por que este arquivo passa por um bundler antes de rodar: o runtime do k6 não
 * é Node — ele não resolve imports de node_modules. O esbuild empacota o
 * @faker-js/faker dentro do script, e o k6 executa o bundle. Daí o `npm run build`.
 *
 * O que este teste mede mudou junto com a arquitetura. Antes, `202 Accepted` era
 * quase o fim da história: o pedido tinha sido publicado e o resto era agregação.
 * Com a Saga de pagamento, o 202 é o COMEÇO — o pedido nasce em PENDING_PAYMENT e
 * o desfecho chega depois, por evento. Um p(95) verde com o relay do outbox
 * travado seria um teste passando sobre um sistema quebrado. Por isso, além da
 * latência do POST, aqui se verifica a CONVERGÊNCIA: uma amostra dos pedidos é
 * consultada até chegar a um estado terminal.
 */

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

// Precisa bater com `fraud.max-orders` no application.yml do App D.
const FRAUD_MAX_ORDERS = Number(__ENV.FRAUD_MAX_ORDERS || 5);

/*
 * Precisa bater com `payment.gateway.approval-limit` no App E.
 *
 * Não é decoração: com o catálogo abaixo (15 a 900) e quantidade de 1 a 5, mais
 * da metade dos pedidos do cenário normal passa de 1000 e é RECUSADA pelo
 * gateway. Isso é comportamento correto, não falha — mas explica por que o teste
 * não exige que pedido normal termine em PAID.
 */
const APPROVAL_LIMIT = Number(__ENV.APPROVAL_LIMIT || 1000);

// Fração dos pedidos normais que tem o desfecho verificado. Verificar todos
// transformaria o teste de carga num teste de polling.
const VERIFY_SAMPLE_RATE = Number(__ENV.VERIFY_SAMPLE_RATE || 0.02);

const SETTLE_TIMEOUT_MS = Number(__ENV.SETTLE_TIMEOUT_MS || 15000);
const COMPENSATION_TIMEOUT_MS = Number(__ENV.COMPENSATION_TIMEOUT_MS || 45000);
const POLL_INTERVAL_S = 0.5;

const TERMINAL = ['PAID', 'CANCELLED'];

/*
 * Perfis de execução.
 *
 * As durações vivem aqui, e não espalhadas pelos cenários, porque o run.sh
 * precisa de uma passagem curta para validar a montagem sem esperar cinco
 * minutos — e reescrever o arquivo com sed para conseguir isso seria testar um
 * script que não é o que roda de verdade.
 */
const PROFILES = {
  full: {
    rampStages: [
      { duration: '30s', target: 10 },
      { duration: '30s', target: 100 },
      { duration: '1m', target: 500 },
      { duration: '30s', target: 500 },
      { duration: '30s', target: 0 },
    ],
    burstStart: '20s',
    burstDuration: '3m',
    compensationStart: '45s',
    compensationIterations: 3,
    compensationMaxDuration: '4m',
    minSuspiciousOrders: 50,
  },
  smoke: {
    rampStages: [
      { duration: '5s', target: 10 },
      { duration: '10s', target: 40 },
      { duration: '5s', target: 0 },
    ],
    burstStart: '3s',
    burstDuration: '20s',
    compensationStart: '6s',
    compensationIterations: 1,
    compensationMaxDuration: '90s',
    minSuspiciousOrders: 10,
  },
};

const PROFILE = PROFILES[(__ENV.PROFILE || 'full').toLowerCase()] ?? PROFILES.full;

// --- Métricas próprias, além das que o k6 já coleta -------------------------

const ordersAccepted = new Counter('orders_accepted');
const ordersRejected = new Counter('orders_rejected');
const suspiciousOrdersSent = new Counter('suspicious_orders_sent');
const acceptanceRate = new Rate('order_acceptance_rate');
const orderLatency = new Trend('order_placement_duration', true);

// A Saga, que é o que o 202 não podia prometer.
const sagaSettled = new Rate('saga_settled');
const sagaSettleDuration = new Trend('saga_settle_duration', true);
const ordersPaid = new Counter('orders_settled_paid');
const ordersCancelled = new Counter('orders_settled_cancelled');
const compensationsObserved = new Counter('fraud_compensations_observed');
const paidBeforeCompensation = new Counter('fraud_compensations_after_payment');

// --- Catálogo -------------------------------------------------------------

/*
 * O catálogo é gerado UMA vez, no init, e não a cada iteração.
 *
 * Se cada requisição inventasse um produto novo, o App B acumularia dezenas de
 * milhares de agregados de venda com uma unidade cada — o teste mediria a
 * criação de linhas, não a agregação. Um catálogo fixo faz os pedidos se
 * concentrarem em produtos repetidos, que é como uma loja real se comporta e o
 * que torna "top produtos" um número com significado.
 */
faker.seed(42);

const CATALOG = Array.from({ length: 40 }, () => ({
  name: faker.commerce.productName(),
  price: Number(faker.commerce.price({ min: 15, max: 900 })),
}));

// Pool pequeno e fixo de clientes "suspeitos", para a rajada bater sempre nos mesmos.
const SUSPICIOUS_CUSTOMERS = Array.from({ length: 5 }, (_, i) => `suspeito-${i + 1}`);

export const options = {
  /*
   * Por padrão o k6 só calcula avg/min/med/max/p(90)/p(95). Sem declarar p(99)
   * aqui, ele não é computado para o resumo — e o relatório imprimiria 0.0 ms,
   * que parece um número excelente e é só ausência de dado.
   */
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],

  scenarios: {
    /*
     * Tráfego normal: rampa até 500 VUs, cada um com cliente próprio.
     * Não deve acionar detecção de fraude nenhuma.
     */
    normal_traffic: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: PROFILE.rampStages,
      gracefulRampDown: '15s',
      exec: 'placeNormalOrder',
      tags: { scenario: 'normal' },
    },

    /*
     * Rajada: poucos VUs martelando um pool fixo de 5 clientes, sem pausa.
     * Existe para acionar CustomerFraudPattern.isFraudulent() de forma
     * determinística, e não por acidente estatístico do tráfego normal.
     */
    suspicious_burst: {
      executor: 'constant-vus',
      vus: 5,
      duration: PROFILE.burstDuration,
      startTime: PROFILE.burstStart,
      exec: 'placeSuspiciousBurst',
      tags: { scenario: 'burst' },
    },

    /*
     * A compensação, isolada num cenário próprio.
     *
     * Não dá para verificá-la dentro de `suspicious_burst`: esperar o estorno
     * bloquearia o VU por dezenas de segundos e mataria a rajada que ele existe
     * para produzir. Aqui um único VU usa um cliente dedicado, com janela limpa,
     * e acompanha o ciclo inteiro do primeiro pedido — PENDING_PAYMENT, PAID,
     * CANCELLED — sem atrapalhar ninguém.
     */
    fraud_compensation_check: {
      executor: 'per-vu-iterations',
      vus: 1,
      iterations: PROFILE.compensationIterations,
      maxDuration: PROFILE.compensationMaxDuration,
      startTime: PROFILE.compensationStart,
      exec: 'verifyFraudCompensation',
      tags: { scenario: 'compensation' },
    },
  },

  thresholds: {
    // Pedidos válidos não podem falhar. Se falharem, o serviço não aguentou.
    'http_req_failed{scenario:normal}': ['rate<0.01'],
    'http_req_duration{scenario:normal}': ['p(95)<500', 'p(99)<1500'],
    'order_acceptance_rate': ['rate>0.99'],
    // A rajada TEM que acontecer, senão o cenário não testou o que prometeu.
    'suspicious_orders_sent': [`count>${PROFILE.minSuspiciousOrders}`],
    /*
     * A Saga precisa terminar. Um pedido que fica em PENDING_PAYMENT para sempre
     * é o sintoma de outbox parado, consumidor morto ou evento na DLQ — nenhum
     * dos quais aparece na latência do POST.
     */
    'saga_settled': ['rate>0.95'],
    'saga_settle_duration': ['p(95)<10000'],
    // E a compensação por fraude precisa acontecer de fato.
    'fraud_compensations_observed': ['count>0'],
  },
};

// --- Colocação de pedido ---------------------------------------------------

function placeOrder(payload, tags) {
  const response = http.post(`${BASE_URL}/orders`, JSON.stringify(payload), {
    headers: { 'Content-Type': 'application/json' },
    tags,
  });

  const accepted = response.status === 202;
  acceptanceRate.add(accepted);
  orderLatency.add(response.timings.duration);

  if (accepted) {
    ordersAccepted.add(1);
  } else {
    ordersRejected.add(1);
  }

  check(response, {
    'pedido aceito com 202': (r) => r.status === 202,
    'resposta traz orderId': (r) => accepted && String(r.body).includes('orderId'),
  });

  return accepted ? response.json('orderId') : null;
}

/*
 * Consulta o estado do pedido.
 *
 * Marcada com um `scenario` próprio de propósito: os thresholds de latência são
 * escopados em {scenario:normal}, e contaminar essa métrica com o polling faria
 * o teste medir a si mesmo.
 */
function readOrder(orderId, scenario) {
  const response = http.get(`${BASE_URL}/orders/${orderId}`, {
    tags: { scenario, endpoint: 'read' },
  });
  return response.status === 200 ? response.json('status') : null;
}

/*
 * Espera o pedido chegar a um dos estados aceitos, ou desiste.
 *
 * Devolve o estado observado (ou null no timeout) e registra quanto demorou —
 * que é a métrica que de fato descreve a Saga de ponta a ponta: outbox drenado,
 * pagamento processado, resultado consumido de volta.
 */
function awaitStatus(orderId, wanted, timeoutMs, scenario) {
  const startedAt = Date.now();

  while (Date.now() - startedAt < timeoutMs) {
    const status = readOrder(orderId, scenario);
    if (status !== null && wanted.includes(status)) {
      sagaSettleDuration.add(Date.now() - startedAt);
      sagaSettled.add(true);
      return status;
    }
    sleep(POLL_INTERVAL_S);
  }

  sagaSettled.add(false);
  return null;
}

function randomProduct() {
  return CATALOG[Math.floor(Math.random() * CATALOG.length)];
}

// --- Cenários --------------------------------------------------------------

export function placeNormalOrder() {
  const product = randomProduct();
  const quantity = 1 + Math.floor(Math.random() * 5);

  const orderId = placeOrder({
    // Cliente distinto por VU e iteração: espalha pelas partições e mantém
    // cada cliente muito abaixo do limiar de fraude.
    customerId: `cliente-${__VU}-${__ITER}`,
    product: product.name,
    quantity,
    amount: Number((product.price * quantity).toFixed(2)),
  }, { scenario: 'normal' });

  if (orderId === null || Math.random() >= VERIFY_SAMPLE_RATE) {
    return;
  }

  /*
   * PAID **ou** CANCELLED: acima de APPROVAL_LIMIT o gateway recusa, e recusa é
   * desfecho legítimo, não erro. Exigir PAID aqui reprovaria o teste por mais da
   * metade dos pedidos estarem se comportando exatamente como deveriam.
   */
  const status = awaitStatus(orderId, TERMINAL, SETTLE_TIMEOUT_MS, 'verify');
  if (status === 'PAID') {
    ordersPaid.add(1);
  } else if (status === 'CANCELLED') {
    ordersCancelled.add(1);
  }
  check(status, { 'Saga chegou a estado terminal': (s) => s !== null });
}

export function placeSuspiciousBurst() {
  const customerId = SUSPICIOUS_CUSTOMERS[__VU % SUSPICIOUS_CUSTOMERS.length];
  const product = randomProduct();

  // Uma rajada de tamanho suficiente para cruzar o limiar dentro da janela.
  // Sem sleep entre os envios: é exatamente o comportamento que deve alertar.
  const burstSize = FRAUD_MAX_ORDERS + 3;

  for (let i = 0; i < burstSize; i++) {
    placeOrder({
      customerId,
      product: product.name,
      quantity: 1,
      amount: Number(product.price.toFixed(2)),
    }, { scenario: 'burst' });
    suspiciousOrdersSent.add(1);
  }
}

/*
 * O ciclo completo da compensação, do jeito que só um cliente dedicado permite
 * observar.
 *
 * A detecção dispara na TRANSIÇÃO para fraudulento — `register` só emite quando
 * o cliente ainda não era suspeito e passa a ser. Um cliente já sinalizado que
 * segue comprando não gera evento novo, e é por isso que a rajada de 3 minutos
 * produz poucos estornos, não milhares. Um cliente novo a cada iteração garante
 * uma transição limpa por vez.
 *
 * O valor fica abaixo do limite do gateway de propósito: assim o pagamento é
 * APROVADO antes de a fraude aparecer, e o que se observa é o caminho que
 * interessa — PAID sendo desfeito por compensação, e não um pedido que já ia ser
 * recusado de qualquer jeito.
 */
export function verifyFraudCompensation() {
  const customerId = `compensacao-${__ITER}-${Date.now()}`;
  const product = CATALOG.find((item) => item.price < APPROVAL_LIMIT) ?? CATALOG[0];
  const placed = [];

  for (let i = 0; i < FRAUD_MAX_ORDERS; i++) {
    const orderId = placeOrder({
      customerId,
      product: product.name,
      quantity: 1,
      amount: Number(product.price.toFixed(2)),
    }, { scenario: 'compensation' });
    if (orderId !== null) {
      placed.push(orderId);
    }
  }

  if (placed.length === 0) {
    return;
  }

  const target = placed[0];

  // Primeiro o pagamento aprova...
  if (awaitStatus(target, ['PAID', 'CANCELLED'], SETTLE_TIMEOUT_MS, 'compensation') === 'PAID') {
    paidBeforeCompensation.add(1);
  }

  // ...e só então a fraude o desfaz.
  const compensated = awaitStatus(target, ['CANCELLED'], COMPENSATION_TIMEOUT_MS, 'compensation');
  if (compensated === 'CANCELLED') {
    compensationsObserved.add(1);
  }

  check(compensated, {
    'pedido fraudulento foi compensado': (s) => s === 'CANCELLED',
  });
}

/*
 * Entrada usada apenas em execução avulsa (`k6 run --vus N --duration Xs`).
 * Passar --vus/--duration pela linha de comando faz o k6 descartar os cenários
 * declarados em `options` e procurar um export default — daí este atalho, útil
 * para um smoke test rápido sem esperar a rampa inteira.
 */
export default function () {
  placeNormalOrder();
}

export function handleSummary(data) {
  const count = (name) => data.metrics[name]?.values?.count ?? 0;
  const accepted = count('orders_accepted');
  const rejected = count('orders_rejected');
  const suspicious = count('suspicious_orders_sent');
  const paid = count('orders_settled_paid');
  const cancelled = count('orders_settled_cancelled');
  const compensated = count('fraud_compensations_observed');
  const paidThenCompensated = count('fraud_compensations_after_payment');
  const duration = data.metrics.http_req_duration?.values ?? {};
  const settle = data.metrics.saga_settle_duration?.values ?? {};
  const settledRate = data.metrics.saga_settled?.values?.rate ?? 0;

  /*
   * Sobrescrever handleSummary substitui o relatório padrão do k6 INTEIRO —
   * inclusive o resultado dos thresholds. Como o exit code sozinho só diz
   * "passou" ou "não passou", sem indicar qual regra falhou, reconstruímos essa
   * parte aqui a partir de data.metrics[*].thresholds.
   */
  const thresholdLines = [];
  for (const [metricName, metric] of Object.entries(data.metrics)) {
    for (const [expression, result] of Object.entries(metric.thresholds ?? {})) {
      const passed = result.ok !== false;
      thresholdLines.push(`  ${passed ? '✓' : '✗'} ${metricName} ${expression}`);
    }
  }
  const allPassed = thresholdLines.every((line) => line.includes('✓'));

  const summary = [
    '',
    '─────────────────────────────────────────────',
    '  COLOCAÇÃO (síncrona)',
    `  pedidos aceitos (202)     ${accepted}`,
    `  pedidos rejeitados        ${rejected}`,
    `  enviados na rajada        ${suspicious}`,
    `  latência p(95)            ${(duration['p(95)'] ?? 0).toFixed(1)} ms`,
    `  latência p(99)            ${(duration['p(99)'] ?? 0).toFixed(1)} ms`,
    `  latência máxima           ${(duration.max ?? 0).toFixed(1)} ms`,
    '─────────────────────────────────────────────',
    '  SAGA (assíncrona, sobre a amostra verificada)',
    `  convergiram               ${(settledRate * 100).toFixed(1)}%`,
    `  tempo até terminal p(95)  ${(settle['p(95)'] ?? 0).toFixed(0)} ms`,
    `  terminaram PAID           ${paid}`,
    `  terminaram CANCELLED      ${cancelled}   (recusa do gateway acima de ${APPROVAL_LIMIT})`,
    '─────────────────────────────────────────────',
    '  COMPENSAÇÃO POR FRAUDE',
    `  pagos antes do estorno    ${paidThenCompensated}`,
    `  estornos observados       ${compensated}`,
    '─────────────────────────────────────────────',
    '  THRESHOLDS',
    ...(thresholdLines.length ? thresholdLines : ['  (nenhum avaliado)']),
    '─────────────────────────────────────────────',
    `  RESULTADO: ${allPassed ? 'PASSOU' : 'FALHOU'}`,
    '',
    '  Confira o efeito nos outros serviços:',
    '    curl -s localhost:8081/metrics/top-products?limit=5',
    '    curl -s "localhost:8082/audit-events?level=WARN&app=fraud-service&limit=10"',
    '',
  ].join('\n');

  return { stdout: summary };
}
