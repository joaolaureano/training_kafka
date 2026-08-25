import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import { faker } from '@faker-js/faker';

/*
 * Carga sobre o Order Service.
 *
 * Por que este arquivo passa por um bundler antes de rodar: o runtime do k6 não
 * é Node — ele não resolve imports de node_modules. O esbuild empacota o
 * @faker-js/faker dentro do script, e o k6 executa o bundle. Daí o `npm run build`.
 */

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

// Precisa bater com analytics.suspicion no application.yml do App B.
const SUSPICION_THRESHOLD = Number(__ENV.SUSPICION_THRESHOLD || 5);

// --- Métricas próprias, além das que o k6 já coleta -------------------------

const ordersAccepted = new Counter('orders_accepted');
const ordersRejected = new Counter('orders_rejected');
const suspiciousOrdersSent = new Counter('suspicious_orders_sent');
const acceptanceRate = new Rate('order_acceptance_rate');
const orderLatency = new Trend('order_placement_duration', true);

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
     * Não deve acionar detecção de padrão suspeito nenhuma.
     */
    normal_traffic: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '30s', target: 10 },
        { duration: '30s', target: 100 },
        { duration: '1m', target: 500 },
        { duration: '30s', target: 500 },
        { duration: '30s', target: 0 },
      ],
      gracefulRampDown: '15s',
      exec: 'placeNormalOrder',
      tags: { scenario: 'normal' },
    },

    /*
     * Rajada: poucos VUs martelando um pool fixo de 5 clientes, sem pausa.
     * Existe para acionar CustomerOrderPattern.isSuspicious() de forma
     * determinística, e não por acidente estatístico do tráfego normal.
     */
    suspicious_burst: {
      executor: 'constant-vus',
      vus: 5,
      duration: '3m',
      startTime: '20s',
      exec: 'placeSuspiciousBurst',
      tags: { scenario: 'burst' },
    },
  },

  thresholds: {
    // Pedidos válidos não podem falhar. Se falharem, o serviço não aguentou.
    'http_req_failed{scenario:normal}': ['rate<0.01'],
    'http_req_duration{scenario:normal}': ['p(95)<500', 'p(99)<1500'],
    'order_acceptance_rate': ['rate>0.99'],
    // A rajada TEM que acontecer, senão o cenário não testou o que prometeu.
    'suspicious_orders_sent': ['count>50'],
  },
};

// --- Cenários --------------------------------------------------------------

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

  return response;
}

function randomProduct() {
  return CATALOG[Math.floor(Math.random() * CATALOG.length)];
}

export function placeNormalOrder() {
  const product = randomProduct();
  const quantity = 1 + Math.floor(Math.random() * 5);

  placeOrder({
    // Cliente distinto por VU e iteração: espalha pelas partições e mantém
    // cada cliente muito abaixo do limiar de suspeita.
    customerId: `cliente-${__VU}-${__ITER}`,
    product: product.name,
    quantity,
    amount: Number((product.price * quantity).toFixed(2)),
  }, { scenario: 'normal' });
}

export function placeSuspiciousBurst() {
  const customerId = SUSPICIOUS_CUSTOMERS[__VU % SUSPICIOUS_CUSTOMERS.length];
  const product = randomProduct();

  // Uma rajada de tamanho suficiente para cruzar o limiar dentro da janela.
  // Sem sleep entre os envios: é exatamente o comportamento que deve alertar.
  const burstSize = SUSPICION_THRESHOLD + 3;

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
 * Entrada usada apenas em execução avulsa (`k6 run --vus N --duration Xs`).
 * Passar --vus/--duration pela linha de comando faz o k6 descartar os cenários
 * declarados em `options` e procurar um export default — daí este atalho, útil
 * para um smoke test rápido sem esperar a rampa inteira.
 */
export default function () {
  placeNormalOrder();
}

export function handleSummary(data) {
  const accepted = data.metrics.orders_accepted?.values?.count ?? 0;
  const rejected = data.metrics.orders_rejected?.values?.count ?? 0;
  const suspicious = data.metrics.suspicious_orders_sent?.values?.count ?? 0;
  const duration = data.metrics.http_req_duration?.values ?? {};

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
    `  pedidos aceitos (202)     ${accepted}`,
    `  pedidos rejeitados        ${rejected}`,
    `  enviados na rajada        ${suspicious}`,
    `  latência p(95)            ${(duration['p(95)'] ?? 0).toFixed(1)} ms`,
    `  latência p(99)            ${(duration['p(99)'] ?? 0).toFixed(1)} ms`,
    `  latência máxima           ${(duration.max ?? 0).toFixed(1)} ms`,
    '─────────────────────────────────────────────',
    '  THRESHOLDS',
    ...(thresholdLines.length ? thresholdLines : ['  (nenhum avaliado)']),
    '─────────────────────────────────────────────',
    `  RESULTADO: ${allPassed ? 'PASSOU' : 'FALHOU'}`,
    '',
    '  Confira o efeito no App B e no App C:',
    '    curl -s localhost:8081/metrics/top-products?limit=5',
    '    curl -s "localhost:8082/logs?level=WARN&app=metrics-consumer&limit=10"',
    '',
  ].join('\n');

  return { stdout: summary };
}
