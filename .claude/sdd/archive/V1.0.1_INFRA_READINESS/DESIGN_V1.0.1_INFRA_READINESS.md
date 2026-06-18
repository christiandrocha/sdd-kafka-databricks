# DESIGN: v1.0.1 Infra Readiness — gaps reais antes do primeiro `make up`

> Technical design para completar o PostgreSQL CDC source, registrar o Debezium connector, provisionar o Unity Catalog e corrigir a ADR-02

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | V1.0.1_INFRA_READINESS |
| **Date** | 2026-06-17 |
| **Author** | design-agent |
| **DEFINE** | [DEFINE_V1.0.1_INFRA_READINESS.md](./DEFINE_V1.0.1_INFRA_READINESS.md) |
| **Status** | ✅ Shipped (2026-06-17) |

---

## Architecture Overview

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│                      LOCAL STACK (docker compose)                              │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  tests/data/*.json ──load_to_postgres.py──→ PostgreSQL 16                      │
│                                               │  sql/init.sql (★ expandir)      │
│                                               │  20 CREATE TABLE + publication  │
│                                               ▼                                │
│                                          Debezium (kafka-connect)              │
│                                               │  connectors/debezium.json (★new)│
│                                               │  SMT ExtractNewRecordState      │
│                                               │  registrado por                 │
│                                               │  scripts/register_connectors.sh │
│                                               ▼  (★ new)                       │
│                                            Kafka (20 tópicos pg.public.*)       │
│                                               │  + Confluent Schema Registry    │
└───────────────────────────────────────────────┼────────────────────────────────┘
                                                  │
┌─────────────────────────────────────────────── ▼ ────────────────────────────┐
│                      DATABRICKS WORKSPACE (dev / prod)                         │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  scripts/preflight_unity_catalog.sh (★ new) ──cria antes do 1º deploy──→       │
│    Catalog (ubereats_dev/prod)                                                 │
│    ├── Schema bronze / silver / gold / quarantine   (dados)                    │
│    └── Schema checkpoints                                                      │
│         ├── Volume bronze   ← /Volumes/{catalog}/checkpoints/bronze/{table}    │
│         └── Volume silver   ← /Volumes/{catalog}/checkpoints/silver/{table}    │
│                                                                                  │
│  databricks bundle deploy → pipeline_bronze.ipynb (espera schema pós-SMT)      │
│                            → pipeline_silver.ipynb → Gold (inalterado)         │
└──────────────────────────────────────────────────────────────────────────────┘

  ★ = arquivo criado/modificado nesta feature
```

---

## Components

| Component | Purpose | Technology |
|-----------|---------|------------|
| `sql/init.sql` | Schema PostgreSQL completo (20 tabelas) + replication slot + publication | PostgreSQL 16 DDL |
| `connectors/debezium.json` | Config do connector CDC, com SMT `ExtractNewRecordState` mantida | Debezium 2.7.1 PostgreSQL Connector |
| `scripts/register_connectors.sh` | Registra o connector via REST API do Kafka Connect, idempotente | Bash + curl |
| `scripts/preflight_unity_catalog.sh` | Provisiona Catalog + Schemas + Volumes de checkpoint via CLI, idempotente | Bash + Databricks CLI |
| ADR-02 (CLAUDE.md, 03_design.md, 02_define.md) | Documentação da decisão de manter a SMT — corrigida para bater com o código | Markdown / JSON |
| `06_retrospective.md` / `05_implementation_log.md` | Fecham TD-06 (JMX) e TD-04 (Volumes) | Markdown |

---

## Key Decisions

### Decision 1: Manter a SMT `ExtractNewRecordState` e corrigir a ADR-02 escrita

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-17 |

**Context:** `pipeline_bronze.ipynb` já espera Avro pós-SMT (`business fields + __op + __source_ts_ms`) e `pipeline_silver.ipynb` não tem lógica de unwrap de envelope. A ADR-02 escrita em `CLAUDE.md`/`03_design.md`/`02_define.md` diz o oposto ("sem SMT, envelope raw").

**Choice:** `connectors/debezium.json` mantém `transforms.unwrap.type: io.debezium.transforms.ExtractNewRecordState`. Os 3 documentos são corrigidos para descrever essa decisão como a real.

**Rationale:** O código é a fonte de verdade executável; documentar uma arquitetura que não existe é mais perigoso do que não documentar nada — o próximo agente que ler a ADR-02 confiaria nela.

**Alternatives Rejected:**
1. Implementar o unwrap manualmente no Silver (envelope raw + parse em Spark) — rejeitado: exigiria reescrever `pipeline_bronze.ipynb`/`pipeline_silver.ipynb` e os 19 contratos, puro retrabalho sem ganho, já que a decisão "SMT correta para topologia unidirecional" já foi validada na retrospectiva de design.
2. Deixar a ADR-02 como está e só criar os arquivos de infra — rejeitado pelo usuário no Define (cria um `debezium.json` que contradiz a doc no mesmo commit).

**Consequences:**
- Bronze não preserva o envelope Debezium completo (before/after/source) — trade-off já aceito e documentado como decisão consciente, não acidente.
- Qualquer auditoria futura que precise do envelope completo exigiria uma nova ADR (fora de escopo aqui).

---

### Decision 2: `register_connectors.sh` registra só 1 connector

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-17 |

**Context:** O script de referência (`sdd-kafka-snowflake`) registra 3 connectors (Debezium + 2 Sinks Snowflake).

**Choice:** Registrar só `debezium-postgres-cdc`.

**Rationale:** ADR-01 já decidiu Databricks Structured Streaming lendo Kafka direto — não há Sink Connector neste projeto.

**Alternatives Rejected:**
1. Copiar os 3 connectors e desabilitar os 2 sinks via flag — rejeitado: código morto não testável, viola YAGNI.

**Consequences:**
- Script fica ~40% menor que a referência, sem lógica de sink.

---

### Decision 3: Schema dedicado `checkpoints` com 2 Volumes (`bronze`, `silver`)

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-17 |

**Context:** `databricks.yml` já define `checkpoint_base: /Volumes/ubereats_dev/checkpoints` e cada task usa `${var.checkpoint_base}/bronze/{table}` ou `${var.checkpoint_base}/silver/{table}`. Um path de Volume do Unity Catalog é sempre `/Volumes/<catalog>/<schema>/<volume>/<subpath>` (3 níveis) — então o path real resolvido é catalog=`ubereats_dev`, **schema=`checkpoints`**, **volume=`bronze`** (ou `silver`), subpath=`{table}`. Não é "1 Volume chamado checkpoints" como a descrição solta do Define sugeria — é 1 schema com 2 Volumes.

**Choice:** `scripts/preflight_unity_catalog.sh` cria uma 5ª schema `checkpoints` (separada das 4 schemas de dados) contendo os Volumes `bronze` e `silver`. Nenhuma mudança em `databricks.yml` é necessária — o path já estava certo, só nunca foi provisionado.

**Rationale:** Confirmei nos 2 notebooks que só Bronze e Silver fazem streaming com checkpoint (`writeStream`); os 6 notebooks Gold usam `spark.sql(MERGE)` batch, sem checkpoint. Logo só 2 Volumes são necessários, não 1 por domínio.

**Alternatives Rejected:**
1. Reinterpretar `checkpoint_base` como path de Volume único e mudar `databricks.yml` — rejeitado: expandiria o escopo (DEFINE não pede mudança em `databricks.yml`) e o path atual já é válido sob a leitura correta de 3 níveis.

**Consequences:**
- `06_schemas` mencionados em `CLAUDE.md`/`03_design.md` (bronze/silver/gold/quarantine) ganham uma 5ª, operacional, que não guarda dados de domínio — vale uma nota no `CLAUDE.md` para não confundir com os 4 schemas "de negócio".

---

### Decision 4: Pre-flight como script bash idempotente, não notebook nem bundle resource

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-17 |

**Context:** `databricks.yml` `resources:` só declara `jobs:`. Confirmado no Define/Brainstorm.

**Choice:** `scripts/preflight_unity_catalog.sh --target {dev|prod}`, usando `databricks catalogs/schemas/volumes get` antes de `create` (idempotência manual, sem depender de flags `--if-not-exists` que variam por versão do CLI).

**Rationale:** Roda uma vez, fora do ciclo de jobs do DABs; simples de tornar idempotente e de dar mensagem de erro clara se o CLI não estiver autenticado (Assumption A-002 do Define).

**Alternatives Rejected:**
1. Notebook `.ipynb` dedicado — rejeitado: exigiria subir o notebook ao workspace antes mesmo do catalog existir (problema de ordem — `workspace_root` em `databricks.yml` já assume Repos sincronizado, mas a Volume em si bloqueia o primeiro `CREATE TABLE`, não o sync do notebook).
2. Declarar como `resources.volumes`/`resources.schemas` no bundle — rejeitado nesta iteração: adicionaria uma dependência de versão mínima do Databricks CLI/DAB schema não verificada; fica como melhoria futura.

**Consequences:**
- Mais um script bash no projeto (consistente com `register_connectors.sh`) em vez de um recurso declarativo — aceito como trade-off de simplicidade.

---

## File Manifest

| # | File | Action | Purpose | Agent | Dependencies |
|---|------|--------|---------|-------|---------------|
| 1 | `sql/init.sql` | Modify | Expandir as 14 linhas atuais para as 20 `CREATE TABLE`, adaptadas de `sdd-kafka-snowflake/scripts/init.sql`, validadas contra `contracts/*.yml` | @schema-designer | None |
| 2 | `connectors/debezium.json` | Create | Config do connector Debezium com SMT `ExtractNewRecordState` mantida, `publication.name: debezium_publication` (bater com #1) | @streaming-engineer | 1 |
| 3 | `scripts/register_connectors.sh` | Create | Registra `debezium-postgres-cdc`, seta BACKWARD compatibility, idempotente (HTTP 409 = sucesso) | @shell-script-specialist | 2 |
| 4 | `scripts/preflight_unity_catalog.sh` | Create | Cria Catalog + 4 schemas de dados + schema `checkpoints` (2 Volumes: bronze/silver), idempotente, `--target dev\|prod` | @shell-script-specialist | None |
| 5 | `CLAUDE.md` | Modify | Corrige narrativa da ADR-02 ("usa SMT"); nota sobre a 5ª schema `checkpoints` | (general) | 1, 2 |
| 6 | `.claude/03_design.md` | Modify | Corrige bloco formal da ADR-02; corrige `file_manifest` (`sql/init.sql` em vez de `scripts/init.sql`, remove `scripts/set_compatibility.sh`) | (general) | 1, 2 |
| 7 | `.claude/02_define.md` | Modify | Corrige bullet de `out_of_scope` sobre SMT | (general) | 1, 2 |
| 8 | `.claude/06_retrospective.md` | Modify | Fecha TD-06 (JMX) e TD-04 (Volumes), aponta para #4 | (general) | 1, 2, 3, 4 |
| 9 | `.claude/05_implementation_log.md` | Modify | Nova entrada datada documentando os 2 gaps resolvidos + correção de ADR-02 | (general) | 1–8 |
| 10 | `Makefile` | Modify | Adiciona target `register-connectors` (COULD, conveniência) | @shell-script-specialist | 3 |

**Total Files:** 10

---

## Agent Assignment Rationale

> Agents discovered from `.claude/agents/` — Build phase invoca os especialistas casados.

| Agent | Files Assigned | Why This Agent |
|-------|------------------|------------------|
| @schema-designer | 1 | `.claude/agents/architect/schema-designer.md` — especialista em modelagem de dados; `init.sql` é puramente definição de schema (DDL), sem lógica de pipeline |
| @streaming-engineer | 2 | `.claude/agents/data-engineering/streaming-engineer.md` — exemplo próprio do agente é literalmente "Set up Debezium CDC from Postgres to Kafka" |
| @shell-script-specialist | 3, 4, 10 | `.claude/agents/dev/shell-script-specialist.md` — "building production-grade Bash scripts with best practices, error handling"; exemplos do agente incluem script de deploy e script de limpeza idempotente, mesmo perfil dos 2 scripts aqui |
| (general) | 5, 6, 7, 8, 9 | Edição de Markdown/JSON de documentação — não há um agente de "doc maintainer"; Build phase aplica os diffs diretamente, como já foi feito na sessão anterior (remoção do `payment_current_state`) |

**Agent Discovery:**
- Scanned: `.claude/agents/**/*.md` (54 agentes)
- Matched by: tipo de arquivo (`.sql`/`.json`/`.sh`/`.md`), keywords de propósito (CDC, schema, shell script), exemplos do próprio agente

---

## Code Patterns

### Pattern 1: `CREATE TABLE` a partir do contrato (repetir para os 20 domínios)

```sql
-- Cada bloco segue 1:1 o schema de contracts/<table>.yml — campos de negócio
-- apenas; __op/__source_ts_ms/_ingested_at são adicionados pela SMT/Bronze,
-- nunca existem na tabela Postgres.

-- payment_events (kafka_events) — 2.208 registros
CREATE TABLE IF NOT EXISTS payment_events (
    event_id             UUID        NOT NULL,
    payment_id           UUID        NOT NULL,
    event                JSONB       NOT NULL,  -- {event_name, timestamp} — vira `string` no contrato
    dt_current_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_payment_events PRIMARY KEY (event_id)
);
CREATE INDEX IF NOT EXISTS idx_payment_events_payment_id ON payment_events (payment_id);

-- Casos especiais confirmados nesta sessão (verificados contra contracts/*.yml):
--   order_status.status_id  → INTEGER (não UUID) — PK
--   receipts                → sem dt_current_timestamp, usa receipt_generated_at
--   search_events            → sem dt_current_timestamp, usa timestamp
--   inventory                → sem dt_current_timestamp, usa last_updated
-- Mapa de tipos: UUID→string, INTEGER→integer, TIMESTAMPTZ→timestamp, JSONB→string
-- (idêntico ao mapa já usado em contracts/spark_schema.py)
```

### Pattern 2: `connectors/debezium.json`

```json
{
  "name": "debezium-postgres-cdc",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",

    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "${POSTGRES_USER}",
    "database.password": "${POSTGRES_PASSWORD}",
    "database.dbname": "${POSTGRES_DB}",
    "topic.prefix": "pg",

    "table.include.list": "public.payment_events,public.orders,public.payments,public.order_items,public.gps_events,public.order_status,public.routes,public.receipts,public.driver_shifts,public.search_events,public.recommendations,public.support_tickets,public.users_mongo,public.users_mssql,public.restaurants,public.drivers,public.products,public.menu_sections,public.ratings,public.inventory",

    "plugin.name": "pgoutput",
    "publication.name": "debezium_publication",
    "slot.name": "debezium_slot",
    "snapshot.mode": "initial",

    "decimal.handling.mode": "double",
    "time.precision.mode": "connect",
    "interval.handling.mode": "string",

    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.add.fields": "op,source.ts_ms",
    "transforms.unwrap.add.fields.prefix": "__",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.drop.tombstones": "false",

    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter.auto.register.schemas": "true"
  }
}
```

> Único diff real vs. a referência: `"publication.name": "debezium_publication"` (não `"dbz_publication"`) — precisa bater com o que `sql/init.sql` cria.

### Pattern 3: `scripts/register_connectors.sh` (núcleo, sem o boilerplate de cores/echo)

```bash
#!/bin/bash
# Registra o connector Debezium (único — Databricks lê Kafka direto, ADR-01)
set -euo pipefail

CONNECT_URL="http://localhost:8083"
REGISTRY_URL="http://localhost:8081"
CONNECTORS_DIR="$(dirname "$0")/../connectors"

set -a; source ".env"; set +a

# 1. Espera Kafka Connect ficar pronto (loop com timeout — ver referência)
# 2. Seta BACKWARD compatibility global (substitui scripts/set_compatibility.sh — YAGNI)
curl -sf -X PUT "${REGISTRY_URL}/config" \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d '{"compatibility": "BACKWARD"}' > /dev/null

# 3. Registra — 409 (já existe) é sucesso, não erro
register_connector() {
    local name="$1" file="$2"
    RESOLVED=$(envsubst < "$file")
    HTTP=$(echo "$RESOLVED" | curl -sf -o /tmp/connect_resp.json -w "%{http_code}" \
        -X POST "${CONNECT_URL}/connectors" -H "Content-Type: application/json" -d @-)
    case "$HTTP" in
        201) echo "✅  ${name} created" ;;
        409) echo "⚠️   ${name} already exists" ;;
        *)   echo "✖   Failed ${name} (HTTP ${HTTP})"; cat /tmp/connect_resp.json; exit 1 ;;
    esac
}

register_connector "debezium-postgres-cdc" "${CONNECTORS_DIR}/debezium.json"
```

### Pattern 4: `scripts/preflight_unity_catalog.sh` (núcleo)

```bash
#!/bin/bash
# Provisiona Catalog + Schemas + Volumes de checkpoint antes do 1º bundle deploy.
# Idempotente: "get" antes de "create" em cada recurso.
set -euo pipefail

TARGET="${1:-dev}"   # uso: ./preflight_unity_catalog.sh --target dev|prod
case "$TARGET" in
    --target) TARGET="${2:?}" ;;
esac
CATALOG="ubereats_${TARGET}"

# Falha rápida e clara se o CLI não está autenticado (Assumption A-002) — sem retry silencioso
databricks current-user me > /dev/null 2>&1 || {
    echo "✖   Databricks CLI não autenticado. Rode: databricks auth login"
    exit 1
}

ensure_catalog() {
    databricks catalogs get "$CATALOG" > /dev/null 2>&1 || databricks catalogs create "$CATALOG"
}
ensure_schema() {
    databricks schemas get "${CATALOG}.${1}" > /dev/null 2>&1 || databricks schemas create "$1" "$CATALOG"
}
ensure_volume() {
    databricks volumes read "${CATALOG}.checkpoints.${1}" > /dev/null 2>&1 \
        || databricks volumes create "$CATALOG" checkpoints "$1" MANAGED
}

ensure_catalog
for schema in bronze silver gold quarantine checkpoints; do ensure_schema "$schema"; done
for volume in bronze silver; do ensure_volume "$volume"; done

echo "✅  Unity Catalog pronto: ${CATALOG} (4 schemas de dados + checkpoints/{bronze,silver})"
```

---

## Data Flow

```text
1. make up
   │  docker compose sobe postgres com sql/init.sql montado em /docker-entrypoint-initdb.d/
   ▼
2. PostgreSQL inicializa: pgcrypto, replication slot, 20 CREATE TABLE, publication FOR ALL TABLES
   ▼
3. make produce-initial / produce-incremental
   │  load_to_postgres.py faz INSERT INTO nas 20 tabelas agora existentes
   ▼
4. scripts/register_connectors.sh
   │  seta BACKWARD compatibility no Schema Registry
   │  POST connectors/debezium.json → debezium-postgres-cdc (snapshot.mode=initial)
   ▼
5. Debezium lê o snapshot + WAL via debezium_slot/debezium_publication
   │  aplica a SMT ExtractNewRecordState → registros flat + __op + __source_ts_ms
   ▼
6. Kafka recebe nos 20 tópicos pg.public.*, Avro registrado no Schema Registry
   ▼
7. scripts/preflight_unity_catalog.sh --target dev   (uma vez, antes do 1º deploy)
   │  cria ubereats_dev + 4 schemas de dados + checkpoints/{bronze,silver}
   ▼
8. databricks bundle deploy --target dev
   │  pipeline_bronze.ipynb consome os 20 tópicos (schema pós-SMT, checkpoint em
   │  /Volumes/ubereats_dev/checkpoints/bronze/{table}) → Bronze
   ▼
9. pipeline_silver.ipynb → Silver (checkpoint em .../checkpoints/silver/{table})
   ▼
10. Gold (6 notebooks, batch MERGE, sem checkpoint) — inalterado por esta feature
```

---

## Integration Points

| External System | Integration Type | Authentication |
|------------------|-------------------|------------------|
| Kafka Connect REST API (`localhost:8083`) | HTTP/REST (`register_connectors.sh`) | Nenhuma (dev local, sem auth no Connect REST) |
| Confluent Schema Registry (`localhost:8081`) | HTTP/REST (`PUT /config`) | Nenhuma (dev local) |
| PostgreSQL | Driver nativo do Debezium (JDBC interno) | `${POSTGRES_USER}`/`${POSTGRES_PASSWORD}` via `.env` → `envsubst` |
| Databricks Workspace | Databricks CLI (`catalogs`/`schemas`/`volumes`) | `DATABRICKS_HOST`/`DATABRICKS_TOKEN` via `.env` ou profile do CLI |

---

## Testing Strategy

| Test Type | Scope | Files | Tools | Coverage Goal |
|-----------|-------|-------|-------|-----------------|
| Syntax | `debezium.json` é JSON válido | `connectors/debezium.json` | `python3 -c "import json; json.load(open(...))"` | 100% |
| Syntax | Scripts bash sem erro de sintaxe/lint | `scripts/*.sh` | `bash -n`, `shellcheck` (se disponível) | 100% |
| Integration | Schema Postgres completo | `sql/init.sql` | `docker compose up --wait` + query `information_schema.tables` | AT-001 |
| Integration | Load inicial sem erro | `tests/load_to_postgres.py` | `make produce-initial` | AT-002 |
| Integration | Connector único, RUNNING | `register_connectors.sh` | `curl localhost:8083/connectors[/status]` | AT-003, AT-004, AT-006 |
| Integration | Idempotência do registro | `register_connectors.sh` | Rodar 2x, checar exit code | AT-005 |
| Integration | Pre-flight idempotente | `preflight_unity_catalog.sh` | Rodar 2x com `--target dev`, checar exit code | AT-007, AT-008 |
| Manual | ADR-02 consistente | `CLAUDE.md`, `03_design.md`, `02_define.md` | `grep -i ExtractNewRecordState` | AT-009 |
| Manual | TDs fechados | `06_retrospective.md` | Leitura manual | AT-010 |
| E2E | Ponta a ponta local | Stack completo | `make up` → `register-connectors` → `produce-initial` → Kafka UI mostra mensagens nos 20 tópicos | Happy path |

---

## Error Handling

| Error Type | Handling Strategy | Retry? |
|------------|----------------------|--------|
| `sql/init.sql` com erro de sintaxe SQL | Postgres falha o healthcheck, `docker compose up --wait` expira com erro visível | Não — corrigir e re-rodar `make up` |
| Kafka Connect não está pronto ainda | Loop de espera com timeout (30 tentativas × 5s), como na referência | Sim, com timeout |
| Connector já registrado (HTTP 409) | Tratado como sucesso, log de aviso | N/A |
| Registro falha com outro HTTP code | `exit 1` + dump do corpo da resposta | Não |
| Databricks CLI não autenticado | Mensagem clara (`databricks auth login`) + `exit 1` imediato — sem retry silencioso (Assumption A-002) | Não |
| Catalog/Schema/Volume já existe | `get` antes de `create` — tratado como no-op, não como erro | N/A |

---

## Configuration

| Config Key | Type | Default | Description |
|------------|------|---------|--------------|
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | string | (via `.env`) | Credenciais resolvidas em `debezium.json` via `envsubst` |
| `DATABRICKS_HOST` / `DATABRICKS_TOKEN` | string | (via `.env` ou CLI profile) | Autenticação do `preflight_unity_catalog.sh` |
| `CONNECT_URL` | string | `http://localhost:8083` | Endpoint do Kafka Connect REST, hardcoded no script (dev local) |
| `REGISTRY_URL` | string | `http://localhost:8081` | Endpoint do Schema Registry, hardcoded no script (dev local) |
| `--target` (arg do preflight) | string | `dev` | Seleciona `ubereats_dev` ou `ubereats_prod`, espelha os targets de `databricks.yml` |

---

## Security Considerations

- `orders.user_key` (CPF) e `orders.restaurant_key` (CNPJ) continuam sem masking no `sql/init.sql` — consistente com o tratamento já existente nos contratos (nenhum dos 19 aplica masking hoje); não é regressão introduzida por esta feature.
- `connectors/debezium.json` nunca contém credenciais literais — usa `${POSTGRES_USER}`/`${POSTGRES_PASSWORD}` resolvidos via `envsubst` no momento do registro, igual à referência.
- `.env` permanece fora do git (`AC-15` já cobre isso) — `DATABRICKS_TOKEN` nunca é hardcoded em `preflight_unity_catalog.sh`.
- `scripts/register_connectors.sh` e `scripts/preflight_unity_catalog.sh` não logam o conteúdo de `.env` nem tokens em caso de erro (`set -x` não deve ser usado em produção).

---

## Observability

| Aspect | Implementation |
|--------|------------------|
| Logging | Scripts imprimem status por etapa (✅/⚠️/✖), mesmo padrão da referência — sem dependência de ferramenta externa |
| Metrics | Nenhuma métrica nova — JMX (`jmx-kafka-connect:9404`, já resolvido no TD-06) já cobre o worker do Kafka Connect; sucesso do registro é validado por exit code do script, não por métrica |
| Tracing | N/A — fora de escopo para scripts de provisionamento one-shot |

---

## Pipeline Architecture

### DAG Diagram

```text
[20 JSON files] ──load_to_postgres.py──→ [PostgreSQL: 20 tabelas, sql/init.sql] ──Debezium (SMT)──→
  [Kafka: 20 tópicos] ──pipeline_bronze.ipynb──→ [Bronze: 20 tabelas Delta] ──pipeline_silver.ipynb──→
  [Silver: 11 tabelas Delta] ──gold_*.ipynb (6x)──→ [Gold: 6 tabelas Delta]

Checkpoints (Structured Streaming, só Bronze e Silver):
  /Volumes/{catalog}/checkpoints/bronze/{table}
  /Volumes/{catalog}/checkpoints/silver/{table}
```

### Partition Strategy

N/A — não há mudança de particionamento/clustering nesta feature; Liquid Clustering (ADR-04) já cobre Bronze/Silver/Gold e não é afetado.

### Incremental Strategy

| Model | Strategy | Key Column | Lookback |
|-------|----------|-------------|-----------|
| Bronze (todos) | `trigger(availableNow=True)` + `MERGE ... WHEN NOT MATCHED` (append-only) | `merge_key` do contrato | N/A — lê desde o `startingOffsets` configurado, sem janela de lookback |
| Debezium snapshot | `snapshot.mode: initial` — snapshot completo na 1ª conexão, depois streaming via WAL | PK de cada tabela | N/A |

### Schema Evolution Plan

| Change Type | Handling | Rollback |
|-------------|----------|-----------|
| Novo domínio adicionado | Precisa de 3 edições coordenadas: novo `CREATE TABLE` em `sql/init.sql`, novo item em `table.include.list` de `connectors/debezium.json`, novo contrato YAML + task em `databricks.yml` — `publication FOR ALL TABLES` já cobre automaticamente, mas o `table.include.list` do Debezium **não é automático** | Remover a tabela do `table.include.list` e reiniciar o connector |
| Nova coluna em tabela existente | `ALTER TABLE ... ADD COLUMN` + contrato YAML com `new_fields: allowed` (já é o default nos 19 contratos) | `ALTER TABLE ... DROP COLUMN` |

### Data Quality Gates

Inalteradas — herdadas dos 19 contratos YAML existentes (`quality.rules` por campo, `on_failure: reject/quarantine/warn`). Esta feature só viabiliza que os dados cheguem ao pipeline; não adiciona nem remove regras de qualidade.

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-06-17 | design-agent | Initial version, a partir de DEFINE_V1.0.1_INFRA_READINESS.md |
| 1.1 | 2026-06-17 | ship-agent | Shipped and archived |

---

## Next Step

**Ready for:** `/build .claude/sdd/features/DESIGN_V1.0.1_INFRA_READINESS.md`
