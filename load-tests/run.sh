#!/usr/bin/env bash
#
# Sobe a stack inteira, roda o k6 e desmonta tudo.
#
# Existe porque a Saga tornou o teste de carga um teste de INTEGRAÇÃO: ele não
# mede mais só a latência do POST, ele verifica que o pedido converge para um
# estado terminal e que a fraude compensa um pagamento aprovado. Isso exige os
# cinco serviços e o broker no ar ao mesmo tempo — sequência longa o bastante
# para alguém errar na mão, e errar de um jeito que parece falha do sistema.
#
#   ./run.sh                 perfil completo (~5 min de carga)
#   ./run.sh --smoke         passagem curta (~40s), para validar a montagem
#   ./run.sh --keep          deixa a stack no ar depois do teste
#   ./run.sh --skip-build    reusa os jars existentes
#   ./run.sh --no-infra      assume Kafka já rodando (não mexe no compose)
#
set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
readonly LOG_DIR="${SCRIPT_DIR}/logs"
readonly VERSION="0.1.0-SNAPSHOT"

PROFILE="full"
JAVA_BIN=""
KEEP_RUNNING=0
SKIP_BUILD=0
MANAGE_INFRA=1
INFRA_STARTED=0
SERVICE_PIDS=()

# App A é o primeiro de propósito: é ele quem declara os tópicos no boot, e a
# auto-criação está desligada no broker.
readonly SERVICES=(
  "order-service:order-service/order-service-bootstrap:8080"
  "metrics-consumer:metrics-consumer/metrics-consumer-bootstrap:8081"
  "audit-service:audit-service/audit-service-bootstrap:8082"
  "fraud-service:fraud-service/fraud-service-bootstrap:"
  "payment-service:payment-service/payment-service-bootstrap:"
  "inventory-service:inventory-service/inventory-service-bootstrap:8083"
)

log()  { printf '\033[1;34m▸\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m!\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m✗\033[0m %s\n' "$*" >&2; exit 1; }

# --- Dead Letter Queue ----------------------------------------------------
#
# Uma mensagem na DLQ não é um detalhe de infraestrutura: é uma Saga que não vai
# terminar sozinha. O pedido correspondente fica num estado não terminal para
# sempre, e nada no sistema o varre depois.
#
# Isto existe porque o k6 NÃO enxerga essa falha. Numa execução real deste
# projeto ele reportou 13/13 thresholds e "convergiram 100%" enquanto 103
# pedidos estavam permanentemente travados e 103 mensagens estavam na DLQ — a
# amostragem dele cobre 2% do tráfego normal, e os travados vieram da rajada,
# que ele não verifica. Sem esta checagem, a suíte declara sucesso sobre um
# sistema quebrado.

readonly BROKER=training-kafka-broker
DLQ_BASELINE=""
DLQ_CHECKED=0

# Profundidade de cada tópico `-dlt`: uma linha "tópico total" por tópico.
#
# `kafka-get-offsets.sh`, e não `kafka.tools.GetOffsetShell`: essa classe saiu no
# Kafka 4.x. Invocá-la falha com ClassNotFound — e se a saída de erro estiver
# sendo descartada, como é comum num pipe, o resultado é uma soma vazia. Ou seja,
# a ferramenta errada faz uma DLQ cheia parecer zerada, que é o pior modo de
# falha possível para uma checagem como esta.
dlq_depths() {
  local bruto
  bruto="$(docker exec "${BROKER}" /opt/kafka/bin/kafka-get-offsets.sh \
      --bootstrap-server localhost:9092 --topic '.*-dlt' 2>&1)" || return 1

  # A ferramenta reclamou (regex inválida, broker recusando, tópico inexistente):
  # devolver soma zero aqui seria transformar "não consegui medir" em "está tudo
  # limpo", que é o erro exato que esta função existe para não cometer.
  if grep -qE 'Exception|Error occurred' <<<"${bruto}"; then
    return 1
  fi

  awk -F: 'NF == 3 { total[$1] += $3 } END { for (t in total) print t, total[t] }' \
      <<<"${bruto}" | sort
}

dlq_reachable() { docker exec "${BROKER}" true >/dev/null 2>&1; }

# Linha de base, tirada antes da carga.
#
# Comparar contra zero seria errado: `docker compose down` sem `-v` preserva o
# volume do broker, então mensagens de uma execução anterior sobrevivem e
# reprovariam esta por algo que não aconteceu aqui. O que se afirma é o DELTA.
dlq_snapshot_baseline() {
  if ! dlq_reachable; then
    warn "Broker fora de alcance: a checagem de DLQ será PULADA nesta execução."
    return 0
  fi
  if ! DLQ_BASELINE="$(dlq_depths)"; then
    warn "Não foi possível ler os offsets da DLQ: a checagem será PULADA."
    return 0
  fi
  DLQ_CHECKED=1

  local herdadas
  herdadas="$(awk '{ s += $2 } END { print s+0 }' <<<"${DLQ_BASELINE}")"
  if (( herdadas > 0 )); then
    warn "A DLQ já tinha ${herdadas} mensagem(ns) de execuções anteriores; só o delta desta conta."
  fi
}

# Devolve 1 se qualquer `-dlt` cresceu durante a execução.
dlq_assert_empty() {
  (( DLQ_CHECKED )) || return 0

  # A carga acabou, mas o pipeline ainda drena: uma mensagem pode chegar à DLQ um
  # instante depois do k6 sair. Medir cedo demais é falso negativo.
  sleep 5

  local agora topic depois antes delta total=0
  local -a cresceram=()

  # A leitura funcionou na linha de base, então falhar agora é anomalia de
  # verdade — e "não consegui verificar" não pode passar por "está limpo".
  if ! agora="$(dlq_depths)"; then
    warn "A DLQ não pôde ser verificada no fim da execução (a leitura inicial funcionou)."
    return 1
  fi

  while read -r topic depois; do
    [[ -z "${topic}" ]] && continue
    antes="$(awk -v t="${topic}" '$1 == t { print $2 }' <<<"${DLQ_BASELINE}")"
    delta=$(( depois - ${antes:-0} ))
    if (( delta > 0 )); then
      cresceram+=("${topic} ${delta}")
      total=$(( total + delta ))
    fi
  done <<<"${agora}"

  if (( total == 0 )); then
    log "Dead Letter Queue: nenhuma mensagem nova."
    return 0
  fi

  warn "${total} mensagem(ns) foram para a Dead Letter Queue nesta execução:"
  printf '      %s\n' "${cresceram[@]}" >&2
  warn "Cada uma é uma Saga que não termina sozinha. Para ver a causa:"
  printf '      docker exec %s /opt/kafka/bin/kafka-console-consumer.sh \\\n' "${BROKER}" >&2
  printf '        --bootstrap-server localhost:9092 --topic %s \\\n' "${cresceram[0]%% *}" >&2
  printf '        --from-beginning --max-messages 1 --property print.headers=true\n' >&2
  return 1
}

usage() { sed -n '3,16p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --smoke)      PROFILE="smoke" ;;
    --keep)       KEEP_RUNNING=1 ;;
    --skip-build) SKIP_BUILD=1 ;;
    --no-infra)   MANAGE_INFRA=0 ;;
    -h|--help)    usage ;;
    *)            die "opção desconhecida: $1  (--help para as válidas)" ;;
  esac
  shift
done

# --- Desmontagem ----------------------------------------------------------

# Roda em qualquer saída, inclusive Ctrl-C e erro: um serviço órfão segurando a
# 8080 faz a próxima execução falhar por um motivo que não tem nada a ver com o
# teste.
cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM

  if (( KEEP_RUNNING )); then
    log "--keep: a stack continua no ar. Para derrubar:"
    printf '    kill %s\n' "${SERVICE_PIDS[*]:-}"
    (( INFRA_STARTED )) && printf '    docker compose -f %s/docker-compose.yml down\n' "${REPO_ROOT}"
    exit "${exit_code}"
  fi

  if (( ${#SERVICE_PIDS[@]} )); then
    log "Encerrando os serviços..."
    kill "${SERVICE_PIDS[@]}" 2>/dev/null || true
    # SIGTERM primeiro: o Spring fecha a conexão SQLite e o consumidor commita o
    # offset. Matar de imediato deixaria o outbox com linhas em aberto.
    wait "${SERVICE_PIDS[@]}" 2>/dev/null || true
  fi

  # INFRA_STARTED, e não MANAGE_INFRA: morrer na checagem de pré-requisitos não
  # deve anunciar a desmontagem de algo que nunca subiu.
  if (( INFRA_STARTED )); then
    log "Derrubando o Kafka..."
    docker compose -f "${REPO_ROOT}/docker-compose.yml" down >/dev/null 2>&1 || true
  fi

  exit "${exit_code}"
}
trap cleanup EXIT INT TERM

# --- Pré-requisitos -------------------------------------------------------

require() { command -v "$1" >/dev/null 2>&1 || die "$1 não encontrado no PATH. $2"; }

# `command -v java` não serve como checagem no macOS: existe um shim em
# /usr/bin/java que responde ao `which` e falha ao rodar, dizendo "Unable to
# locate a Java Runtime". Um JDK do Homebrew, que o Maven encontra sozinho, não
# fica no PATH — então a busca precisa ser explícita e terminar num binário que
# de fato executa.
resolve_java() {
  local candidates=()
  [[ -n "${JAVA_HOME:-}" ]] && candidates+=("${JAVA_HOME}/bin/java")
  if home="$(/usr/libexec/java_home 2>/dev/null)"; then
    candidates+=("${home}/bin/java")
  fi
  candidates+=(/opt/homebrew/opt/openjdk/bin/java /usr/local/opt/openjdk/bin/java)
  command -v java >/dev/null 2>&1 && candidates+=("$(command -v java)")

  for candidate in "${candidates[@]}"; do
    if [[ -x "${candidate}" ]] && "${candidate}" -version >/dev/null 2>&1; then
      printf '%s' "${candidate}"
      return 0
    fi
  done
  return 1
}

log "Verificando pré-requisitos..."
require k6 "Instale com: brew install k6"
require npm "Instale o Node"
JAVA_BIN="$(resolve_java)" || die "Nenhum JDK executável encontrado. Instale um JDK 21+ (brew install openjdk) ou aponte JAVA_HOME."
log "JDK: $("${JAVA_BIN}" -version 2>&1 | head -1)"
if (( MANAGE_INFRA )); then
  require docker "Instale o Docker ou o Colima"
  docker info >/dev/null 2>&1 || die "O daemon do Docker não responde. Suba o Colima: colima start --cpus 4 --memory 8"
fi

# --- Infraestrutura -------------------------------------------------------

if (( MANAGE_INFRA )); then
  log "Subindo o Kafka..."
  docker compose -f "${REPO_ROOT}/docker-compose.yml" up -d
  INFRA_STARTED=1

  log "Esperando o broker ficar saudável..."
  for _ in $(seq 60); do
    status="$(docker inspect -f '{{.State.Health.Status}}' training-kafka-broker 2>/dev/null || echo starting)"
    [[ "${status}" == "healthy" ]] && break
    sleep 2
  done
  [[ "${status:-}" == "healthy" ]] || die "O broker não ficou saudável a tempo. Veja: docker logs training-kafka-broker"
fi

# --- Build ----------------------------------------------------------------

if (( SKIP_BUILD )); then
  log "--skip-build: reusando os jars existentes"
else
  log "Compilando (mvn -q clean install)..."
  (cd "${REPO_ROOT}" && mvn -q clean install -DskipTests) || die "O build falhou"
fi

# --- Serviços -------------------------------------------------------------

mkdir -p "${LOG_DIR}"
rm -f "${LOG_DIR}"/*.log

# Um serviço pronto significa coisas diferentes conforme ele exponha HTTP ou
# não: App D e App E não têm porta, então a prova de vida é a linha de boot do
# Spring no log.
await_http() {
  local name="$1" port="$2"
  for _ in $(seq 60); do
    if curl -fsS -m 2 "http://localhost:${port}/actuator/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

await_log() {
  local name="$1"
  for _ in $(seq 60); do
    if grep -q "Started .*Bootstrap" "${LOG_DIR}/${name}.log" 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}

for entry in "${SERVICES[@]}"; do
  IFS=':' read -r name module port <<< "${entry}"
  jar="${REPO_ROOT}/${module}/target/$(basename "${module}")-${VERSION}.jar"
  [[ -f "${jar}" ]] || die "jar não encontrado: ${jar}  (rode sem --skip-build)"

  log "Iniciando ${name}..."
  "${JAVA_BIN}" -jar "${jar}" > "${LOG_DIR}/${name}.log" 2>&1 &
  SERVICE_PIDS+=($!)

  if [[ -n "${port}" ]]; then
    await_http "${name}" "${port}" || die "${name} não respondeu em :${port}. Veja ${LOG_DIR}/${name}.log"
  else
    await_log "${name}" || die "${name} não subiu. Veja ${LOG_DIR}/${name}.log"
  fi
done

log "Os seis serviços estão no ar."

# O fraud-service é Kafka Streams: entre "subiu" e "a topology está consumindo"
# existe o rebalance. Começar a carga antes disso faria a rajada inicial passar
# sem ser vista, e o teste cobraria uma compensação que nunca teve chance.
log "Aguardando o rebalance do Kafka Streams..."
sleep 15

# --- Carga ----------------------------------------------------------------

dlq_snapshot_baseline

log "Rodando o k6 (perfil: ${PROFILE})..."
cd "${SCRIPT_DIR}"
npm run --silent build

set +e
PROFILE="${PROFILE}" k6 run dist/orders-load.js
K6_EXIT=$?
set -e

if (( K6_EXIT != 0 )); then
  warn "O k6 saiu com ${K6_EXIT} — algum threshold falhou."
  warn "Os logs dos serviços ficaram em ${LOG_DIR}/"
fi

set +e
dlq_assert_empty
DLQ_EXIT=$?
set -e

# O código do k6 tem precedência quando ele falhou, para não mascarar qual
# threshold estourou; a DLQ já foi reportada em texto de qualquer forma.
if (( K6_EXIT != 0 )); then
  exit "${K6_EXIT}"
fi
if (( DLQ_EXIT != 0 )); then
  warn "Os logs dos serviços ficaram em ${LOG_DIR}/"
fi
exit "${DLQ_EXIT}"
