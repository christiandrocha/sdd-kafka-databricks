# DESIGN: Free Edition Bronze — modo Volume além de Kafka streaming

> Technical design para um segundo modo de ingestão Bronze (Unity Catalog Volume, batch) que roda no Databricks Free Edition, e para reestruturar databricks.yml de modo que os 37 tasks possam usar compute serverless quando necessário

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | FREE_EDITION_BRONZE |
| **Date** | 2026-06-17 |
| **Author** | design-agent |
| **DEFINE** | [DEFINE_FREE_EDITION_BRONZE.md](./DEFINE_FREE_EDITION_BRONZE.md) |
| **Status** | ✅ Shipped (2026-06-17) |

---

## Scope Note (divergência do DEFINE)

Durante o design, foi descoberto que os 37 tasks do `databricks.yml` referenciam
`job_cluster_key: ubereats_cluster` (cluster clássico, `new_cluster` com `node_type_id`
fixo) — incompatível com Free Edition, que só aceita compute serverless. Isso bloquearia
`databricks bundle deploy`/`run` por completo no Free Edition, independente da correção do
Bronze. Esse achado não estava no DEFINE original (que só cobria o `source_mode` do Bronze).
Apresentado ao usuário via `AskUserQuestion` durante o `/design`; aprovado: reestruturar
`databricks.yml` com YAML anchors para os 37 tasks, permitindo dois jobs (clássico e
serverless) sem duplicar o corpo de cada task. Ver Decision 3.

---

## Architecture Overview

```text
┌──────────────────────────────────────────────────────────────────────────┐
│  MODO KAFKA (dev/prod, já validado em v1.0.1 — sem mudança)               │
│                                                                             │
│  [Kafka pg.public.*] ──readStream──→ [from_avro] ──foreachBatch──→ [Bronze]│
│                              (checkpoint, trigger availableNow)            │
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│  MODO VOLUME (novo — Free Edition)                                        │
│                                                                             │
│  [Kafka pg.public.*] ──export_kafka_to_volume.py──→ [Parquet local]       │
│                                                            │                │
│                                              databricks fs cp (manual)     │
│                                                            ▼                │
│                          [/Volumes/<catalog>/landing/kafka_export/<table>]│
│                                                            │                │
│                                            spark.read (batch, sem stream) │
│                                                            ▼                │
│                                          [merge_to_bronze()] ──→ [Bronze]  │
│                          (mesma função, sem checkpoint — idempotente via  │
│                           MERGE INTO ... WHEN NOT MATCHED)                 │
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│  databricks.yml — COMPUTE POR TARGET                                      │
│                                                                             │
│  targets.dev / targets.prod        →  resources.jobs.ubereats_pipeline    │
│    job_clusters: [ubereats_cluster]   tasks: *classic_tasks (37, com      │
│    (new_cluster, classic)             job_cluster_key)                    │
│                                                                             │
│  targets.free_edition              →  resources.jobs.ubereats_pipeline    │
│    (sem job_clusters)                 tasks: *serverless_tasks (37, sem   │
│                                        job_cluster_key — serverless)      │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Components

| Component | Purpose | Technology |
|-----------|---------|------------|
| `scripts/export_kafka_to_volume.py` | Consome os 20 tópicos `pg.public.*` do início ao fim, decodifica Avro, grava Parquet local | Python, `confluent-kafka[avro,schemaregistry]`, `pyarrow` |
| `pipeline_bronze.ipynb` (modo `volume`) | Lê o Parquet do Volume em batch e reusa `merge_to_bronze()` existente | PySpark, Delta Lake MERGE |
| `databricks.yml` (reestruturado) | 37 tasks definidos uma vez via YAML anchors, compostos diferentemente por target (clássico vs. serverless) | Databricks Asset Bundles (DABs) |
| `scripts/preflight_unity_catalog.sh` (estendido) | Cria o novo schema `landing` + Volume `kafka_export`, junto com o resto | Bash + Databricks CLI |

---

## Key Decisions

### Decision 1: Bronze ganha `source_mode` (kafka \| volume), não um notebook separado

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-17 |

**Context:** Free Edition não alcança o Kafka local (restrição de rede do compute
serverless), mas o resto da lógica de Bronze (contrato, DDL, MERGE) deve continuar
idêntica.

**Choice:** Widget `source_mode` (default `kafka`) em `pipeline_bronze.ipynb`. As células
de leitura/decodificação Avro (Schema Registry + `from_avro` + `readStream`) só executam
se `source_mode == "kafka"`. Um novo branch `source_mode == "volume"` faz
`spark.read.format("parquet")` no Volume e chama `merge_to_bronze()` diretamente, uma vez.

**Rationale:** Reaproveita 100% da função `merge_to_bronze()`, do fetch de DDL via
contrato, e da regra de qualidade (`merge_key` não-nulo). Um único arquivo para manter.

**Alternatives Rejected:**
1. Notebook separado (`pipeline_bronze_volume.ipynb`) — duplicaria a lógica de
   contrato/DDL/MERGE; qualquer mudança futura precisaria ser replicada em dois lugares.
2. Detectar o modo automaticamente (ex.: tentar Kafka, cair para Volume em caso de
   timeout) — adiciona complexidade e mascara erros de configuração; explícito via widget
   é mais simples de depurar.

**Consequences:**
- Notebook fica com um `if/elif` adicional, mas sem duplicação de lógica
- Quem rodar localmente (`source_mode=kafka`, default) não precisa mudar nada

---

### Decision 2: Novo schema `landing` + Volume `kafka_export`, sem reaproveitar `checkpoints`

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-17 |

**Context:** O Parquet exportado precisa de um lugar para morar antes do Bronze ler em
modo `volume`.

**Choice:** Novo schema `landing`, um Volume `kafka_export`, subpastas por domínio
(`/Volumes/<catalog>/landing/kafka_export/<table_name>/`).

**Rationale:** `checkpoints` é documentado em CLAUDE.md como "operational only, no data
tables" — colocar dados reais lá quebraria essa separação. `landing` mantém a hierarquia
`catalog.schema.volume` já usada por bronze/silver/gold/quarantine/checkpoints.

**Alternatives Rejected:**
1. Reaproveitar `checkpoints.bronze` — economiza 1 schema, mas mistura dado real com
   metadado operacional de streaming; rejeitado no brainstorm.

**Consequences:**
- `scripts/preflight_unity_catalog.sh` precisa de uma chamada extra de `ensure_schema`/
  `ensure_volume`
- Mais um schema para documentar no CLAUDE.md (5 → 6, incluindo `checkpoints`)

---

### Decision 3: databricks.yml — YAML anchors + jobs escopados por target (não no nível raiz)

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-17 |

**Context:** Free Edition só aceita compute serverless; `dev`/`prod` usam cluster clássico
(`new_cluster`). DABs **não suporta excluir um resource do nível raiz `resources:` para um
target específico** ([databricks/cli#2872](https://github.com/databricks/cli/issues/2872) —
feature request aberta, não implementada). Se o job clássico ficasse no nível raiz (como
hoje), o target `free_edition` também tentaria implantá-lo e falharia.

**Choice:** Os 37 tasks são definidos uma única vez como YAML anchors, sob uma chave de
topo dedicada (`task_definitions:`). Dois arrays de tasks são montados a partir desses
anchors: `classic_tasks` (cada task com `job_cluster_key: ubereats_cluster` mesclado) e
`serverless_tasks` (sem `job_cluster_key`). O `resources.jobs.ubereats_pipeline` deixa de
existir no nível raiz — passa a ser definido dentro de `targets.dev.resources`,
`targets.prod.resources` (referenciando `*classic_tasks` + `job_clusters`) e
`targets.free_edition.resources` (referenciando `*serverless_tasks`, sem `job_clusters`).

**Rationale:** Esse é o único jeito de garantir que `free_edition` nunca veja o cluster
clássico, dado que DABs não suporta exclusão de resource por target. YAML anchors/merge
keys são suportados pelo parser do Databricks CLI (confirmado — padrão documentado pela
comunidade) e eliminam a duplicação real: o corpo de cada task (notebook_path,
base_parameters) é escrito uma única vez.

**Alternatives Rejected:**
1. Manter o job no nível raiz e aceitar que `free_edition` também o receba — rejeitado,
   falharia o deploy inteiro nesse target.
2. Bundle file separado (`databricks.free_edition.yml`) com os 37 tasks redefinidos do
   zero — rejeitado no brainstorm/design por duplicar 100% do conteúdo sem nenhum
   reaproveitamento.
3. Reescrever a feature inteira em Lakeflow/DLT (que abstrai compute) — fora de escopo,
   mudança arquitetural muito maior que o necessário.

**Consequences:**
- `databricks.yml` cresce em estrutura (chave nova `task_definitions:`, mais dois arrays
  ancorados), mas o conteúdo de cada task não é duplicado
- `dev` e dev `prod` continuam comportando-se exatamente como antes (mesmo
  `job_clusters`, mesmos 37 tasks) — zero regressão
- `source_mode`/`volume_path` do Bronze são resolvidos via variáveis (`${var.bronze_source_mode}`,
  `${var.landing_base}`), igual ao padrão já usado para `${var.catalog}` — não precisam de
  anchors separados

---

### Decision 4: Export script casta para os tipos do contrato, não para os tipos brutos do Avro

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-17 |

**Context:** Campos Debezium configurados com `time.precision.mode=connect`
(`connectors/debezium.json`) podem chegar como inteiros (millis desde epoch) em vez do
tipo lógico Avro `timestamp`. Se o export gravar esses valores brutos no Parquet, o Bronze
em modo `volume` pode receber tipos incompatíveis com o DDL gerado por
`to_create_table_ddl()` (que espera `TIMESTAMP` para campos `type: timestamp` no
contrato).

**Choice:** O export script usa `contracts.loader.load_contract()` para saber o tipo
declarado de cada coluna, e converte explicitamente (ex.: `long` → `datetime` quando o
contrato diz `timestamp`) antes de escrever o Parquet.

**Rationale:** O contrato já é a fonte única de verdade do schema Bronze — usá-lo também
para a conversão do export evita uma segunda definição de schema dessincronizada.

**Alternatives Rejected:**
1. Confiar no tipo lógico Avro tal como vem do Schema Registry — arriscado dado o
   `time.precision.mode=connect` já configurado; pode silenciosamente gravar o tipo
   errado.

**Consequences:**
- O export script depende de `contracts/loader.py` (import direto, sem duplicar lógica)
- Qualquer contrato mal declarado (tipo errado) afeta os dois modos igualmente —
  comportamento consistente, não uma divergência nova

---

## File Manifest

| # | File | Action | Purpose | Agent | Dependencies |
|---|------|--------|---------|-------|--------------|
| 1 | `scripts/export_kafka_to_volume.py` | Create | Consome os 20 tópicos Kafka, grava Parquet castado pelo contrato | @streaming-engineer | None |
| 2 | `notebooks/pipeline_bronze.ipynb` | Modify | Widget `source_mode` + `volume_path`; branch de leitura batch | @spark-engineer | None |
| 3 | `databricks.yml` | Modify | YAML anchors para os 37 tasks; jobs escopados por target; novo target `free_edition` | @ci-cd-specialist | 2 |
| 4 | `scripts/preflight_unity_catalog.sh` | Modify | `ensure_schema landing` + `ensure_volume kafka_export` | @shell-script-specialist | None |
| 5 | `pyproject.toml` | Modify | Adiciona `pyarrow` como dependência | (direct) | 1 |
| 6 | `CLAUDE.md` | Modify | Documenta os dois `source_mode` e o novo schema `landing` | (direct) | 1, 2, 3, 4 |

**Total Files:** 6

---

## Agent Assignment Rationale

| Agent | Files Assigned | Why This Agent |
|-------|-----------------|-------------------|
| @streaming-engineer | 1 | "Set up Debezium CDC from Postgres to Kafka" — especialista em consumidores Kafka/Avro/Schema Registry |
| @spark-engineer | 2 | "Create a PySpark job to process order events" — lógica de leitura batch + MERGE Delta dentro de um notebook PySpark já existente |
| @ci-cd-specialist | 3 | Único agente cuja descrição cita explicitamente "Databricks Asset Bundles" |
| @shell-script-specialist | 4 | Mesmo agente que já escreveu `preflight_unity_catalog.sh` em v1.0.1 — convenção idempotente já estabelecida |
| (direct) | 5, 6 | Edição trivial de uma linha (dependência) e de documentação — não justifica um agente especializado |

**Agent Discovery:**
- Scanned: lista de agentes disponíveis na sessão
- Matched by: especialização declarada (Kafka/CDC, PySpark, DABs, bash) e precedente do
  próprio projeto (v1.0.1 usou os mesmos três primeiros agentes para tarefas análogas)

---

## Code Patterns

### Pattern 1: `scripts/export_kafka_to_volume.py` — esqueleto

```python
#!/usr/bin/env python3
"""Exporta os tópicos Kafka pg.public.* (pós-SMT) para Parquet local,
um diretório por domínio, castando os tipos conforme contracts/*.yml."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import DeserializingConsumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from contracts.loader import load_contract  # noqa: E402

_PYARROW_TYPE_MAP = {
    "string": pa.string(),
    "integer": pa.int32(),
    "long": pa.int64(),
    "double": pa.float64(),
    "boolean": pa.bool_(),
    "timestamp": pa.timestamp("ms"),
    "date": pa.date32(),
}


def _arrow_schema_for(contract: dict) -> pa.Schema:
    return pa.schema([
        (f["name"], _PYARROW_TYPE_MAP[f["type"]])
        for f in contract["schema"]
        if f["name"] != "_ingested_at"  # adicionado pelo Bronze, não pelo export
    ])


def export_topic(domain: str, contract_path: Path, args) -> int:
    contract = load_contract(contract_path)
    topic = contract["table"]["kafka_topic"]
    arrow_schema = _arrow_schema_for(contract)

    sr_client = SchemaRegistryClient({"url": args.schema_registry_url})
    avro_deserializer = AvroDeserializer(sr_client)

    consumer = DeserializingConsumer({
        "bootstrap.servers": args.kafka_bootstrap,
        "group.id": f"export-{domain}-{topic}",
        "key.deserializer": None,
        "value.deserializer": avro_deserializer,
        "auto.offset.reset": "earliest",
    })

    tp = TopicPartition(topic, 0)
    consumer.assign([tp])
    low, high = consumer.get_watermark_offsets(tp, timeout=10)

    records: list[dict] = []
    while consumer.position([tp])[0].offset < high:
        msg = consumer.poll(timeout=5.0)
        if msg is None:
            break
        if msg.error():
            continue
        records.append(msg.value())  # dict já decodificado pelo AvroDeserializer

    consumer.close()

    out_dir = Path(args.output_dir) / domain
    out_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(records, schema=arrow_schema)
    pq.write_table(table, out_dir / "data.parquet")

    print(f"[export] {domain:<20} topic={topic:<30} records={len(records):>6}")
    return len(records)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka-bootstrap", default="localhost:9092")
    parser.add_argument("--schema-registry-url", default="http://localhost:8081")
    parser.add_argument("--output-dir", default="./kafka_export")
    parser.add_argument("--domain", default=None, help="Exporta só um domínio (debug)")
    args = parser.parse_args()

    contracts_dir = Path(__file__).resolve().parent.parent / "contracts"
    contract_files = (
        [contracts_dir / f"{args.domain}.yml"]
        if args.domain
        else sorted(contracts_dir.glob("*.yml"))
    )

    total = 0
    for contract_path in contract_files:
        domain = contract_path.stem
        total += export_topic(domain, contract_path, args)

    print(f"\n[export] done — {len(contract_files)} domains, {total} records total")


if __name__ == "__main__":
    main()
```

> Nota para o Build: validar se `DeserializingConsumer` + `position()`/`get_watermark_offsets`
> é a forma mais robusta de "ler até o fim e parar" no `confluent-kafka` 2.4 — alternativa
> aceitável é um timeout de inatividade (ex.: `poll()` retorna `None` 3x seguidas → encerra).

---

### Pattern 2: `pipeline_bronze.ipynb` — diff conceitual

```python
# Widgets — adicionar:
dbutils.widgets.text("source_mode", "kafka")  # "kafka" | "volume"
dbutils.widgets.text("volume_path", "/Volumes/ubereats_dev/landing/kafka_export/payment_events")

# ... (células de widget get, contrato/DDL — inalteradas) ...

source_mode = dbutils.widgets.get("source_mode")
volume_path = dbutils.widgets.get("volume_path")

if source_mode == "kafka":
    # Células existentes de fetch do Schema Registry + from_avro + readStream
    # ficam dentro deste branch, inalteradas.
    ...
    (
        parsed_stream
        .writeStream
        .foreachBatch(merge_to_bronze)
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )
elif source_mode == "volume":
    batch_df = spark.read.format("parquet").load(volume_path)
    merge_to_bronze(batch_df, 0)
    print(f"[bronze] volume mode — {batch_df.count()} rows from {volume_path}")
else:
    raise ValueError(f"Unknown source_mode: {source_mode!r}")
```

`merge_to_bronze()` (já existente) não muda — funciona com qualquer DataFrame batch,
streaming ou não.

---

### Pattern 3: `databricks.yml` — anchors + jobs por target

```yaml
# Definições de task — uma vez, reaproveitadas pelos dois jobs (clássico/serverless)
task_definitions:
  bronze_payment_events: &bronze_payment_events_task
    task_key: bronze_payment_events
    notebook_task:
      notebook_path: notebooks/pipeline_bronze.ipynb
      base_parameters:
        table_name: payment_events
        kafka_topic: pg.public.payment_events
        kafka_bootstrap: ${var.kafka_bootstrap}
        schema_registry_url: ${var.schema_registry_url}
        bronze_table: ${var.catalog}.bronze.payment_events
        checkpoint_path: ${var.checkpoint_base}/bronze/payment_events
        max_offsets: "1000"
        starting_offsets: earliest
        contract_path: ${var.workspace_root}/contracts/payment_events.yml
        source_mode: ${var.bronze_source_mode}
        volume_path: ${var.landing_base}/payment_events
  # ... os outros 19 tasks bronze_*, os 10 silver_*, silver_users, e os 6 gold_*
  # seguem o mesmo padrão — cada um ganha source_mode/volume_path só se for bronze_*
  # (os tasks silver_*/gold_* não leem Kafka nem Volume, então não precisam desses campos)

classic_job_clusters: &classic_job_clusters
  - job_cluster_key: ubereats_cluster
    new_cluster:
      spark_version: 15.4.x-scala2.12
      node_type_id: Standard_DS3_v2
      num_workers: 2
      data_security_mode: SINGLE_USER
      spark_conf:
        spark.databricks.delta.preview.enabled: "true"
        spark.databricks.delta.changeDataFeed.enabled: "true"

classic_tasks: &classic_tasks
  - <<: *bronze_payment_events_task
    job_cluster_key: ubereats_cluster
  # ... os outros 36, cada um com `<<: *<task>_task` + `job_cluster_key: ubereats_cluster`

serverless_tasks: &serverless_tasks
  - *bronze_payment_events_task
  # ... os outros 36, cada um como alias direto (sem job_cluster_key)

email_notifications: &email_notifications
  on_failure:
    - christiandr@gmail.com
  no_alert_for_skipped_runs: true

variables:
  catalog:
    default: ubereats_dev
  checkpoint_base:
    default: /Volumes/ubereats_dev/checkpoints
  landing_base:
    description: Base path do Volume de export Kafka→Parquet (modo volume)
    default: /Volumes/ubereats_dev/landing/kafka_export
  bronze_source_mode:
    description: "kafka (streaming, default) ou volume (batch, Free Edition)"
    default: kafka
  workspace_root:
    default: /Workspace/Repos/christiandr@gmail.com/sdd-kafka-databricks
  kafka_bootstrap:
    default: localhost:9092
  schema_registry_url:
    default: http://localhost:8081

targets:
  dev:
    mode: development
    default: true
    variables:
      catalog: ubereats_dev
      checkpoint_base: /Volumes/ubereats_dev/checkpoints
      landing_base: /Volumes/ubereats_dev/landing/kafka_export
      bronze_source_mode: kafka
    resources:
      jobs:
        ubereats_pipeline:
          name: ubereats_pipeline
          email_notifications: *email_notifications
          job_clusters: *classic_job_clusters
          tasks: *classic_tasks

  prod:
    mode: production
    variables:
      catalog: ubereats_prod
      checkpoint_base: /Volumes/ubereats_prod/checkpoints
      landing_base: /Volumes/ubereats_prod/landing/kafka_export
      bronze_source_mode: kafka
    resources:
      jobs:
        ubereats_pipeline:
          name: ubereats_pipeline
          email_notifications: *email_notifications
          job_clusters: *classic_job_clusters
          tasks: *classic_tasks

  free_edition:
    mode: development
    variables:
      catalog: ubereats_dev
      checkpoint_base: /Volumes/ubereats_dev/checkpoints
      landing_base: /Volumes/ubereats_dev/landing/kafka_export
      bronze_source_mode: volume
    resources:
      jobs:
        ubereats_pipeline:
          name: ubereats_pipeline
          email_notifications: *email_notifications
          tasks: *serverless_tasks
          # sem job_clusters — compute serverless implícito (Free Edition exige isso)
```

> Nota para o Build: o `resources:` de nível raiz deixa de existir — os 37 tasks (e os dois
> `job_clusters`) só existem dentro de `task_definitions`/`classic_job_clusters`/
> `classic_tasks`/`serverless_tasks` no topo do arquivo, e cada target referencia o que
> precisa. `dev`/`prod` devem continuar produzindo exatamente o mesmo plano de deploy que
> produziam antes — validar com `databricks bundle validate -t dev` antes e depois da
> mudança (diff do plano, não só "passou sem erro").

---

### Pattern 4: `scripts/preflight_unity_catalog.sh` — adição

```bash
# Junto às outras DATA_SCHEMAS / CHECKPOINT_VOLUMES já existentes:
echo -e "\n${YELLOW}📁  Landing schema (Kafka export → Volume, modo Free Edition)${RESET}"
ensure_schema "landing"

echo -e "\n${YELLOW}🗂️   Landing volume${RESET}"
if databricks volumes read "${CATALOG}.landing.kafka_export" > /dev/null 2>&1; then
    echo -e "  ${YELLOW}⚠️   volume ${CATALOG}.landing.kafka_export already exists${RESET}"
else
    databricks volumes create "$CATALOG" landing kafka_export MANAGED > /dev/null
    echo -e "  ${GREEN}✅  volume ${CATALOG}.landing.kafka_export created${RESET}"
fi
```

Reaproveita as funções `ensure_schema`/idempotência já existentes — mesmo padrão, sem
introduzir uma convenção nova.

---

## Data Flow

```text
1. Kafka local com os 20 tópicos pg.public.* populados (estado pós-v1.0.1)
   │
   ▼
2. python3 scripts/export_kafka_to_volume.py --output-dir ./kafka_export/
   (consome cada tópico do início ao fim, decodifica Avro, casta pelo contrato)
   │
   ▼
3. databricks fs cp -r ./kafka_export/<domain>/ /Volumes/<catalog>/landing/kafka_export/<domain>/
   (manual, uma vez por domínio ou em lote — fora do escopo de automação desta feature)
   │
   ▼
4. databricks bundle deploy -t free_edition && databricks bundle run -t free_edition ubereats_pipeline
   │
   ▼
5. pipeline_bronze.ipynb (source_mode=volume) lê o Parquet, chama merge_to_bronze()
   │
   ▼
6. Bronze Delta populado — Silver/Gold seguem inalterados (leem Bronze, não Kafka/Volume)
```

---

## Integration Points

| External System | Integration Type | Authentication |
|------------------|----------------------|--------------------|
| Kafka local (export) | `confluent-kafka` consumer | Nenhuma (cluster local sem auth, igual ao resto do projeto) |
| Schema Registry local (export) | REST via `confluent-kafka.schema_registry` | Nenhuma |
| Unity Catalog Volume (upload) | `databricks fs cp` / CLI | PAT/OAuth já configurado (igual a `preflight_unity_catalog.sh`) |
| Unity Catalog Volume (leitura no notebook) | `spark.read.format("parquet")` | Nenhuma adicional — já dentro do workspace |

---

## Testing Strategy

| Test Type | Scope | Files | Tools | Coverage Goal |
|-----------|-------|-------|-------|-----------------|
| Syntax | `export_kafka_to_volume.py` | `scripts/export_kafka_to_volume.py` | `ruff check`, `python3 -c "import ast; ast.parse(...)"` | 100% |
| Syntax | `preflight_unity_catalog.sh` | `scripts/preflight_unity_catalog.sh` | `bash -n` | 100% |
| Syntax | `databricks.yml` | `databricks.yml` | `python3 -c "import yaml; yaml.safe_load(...)"` (valida anchors) | 100% |
| Integration | Export real contra Kafka local | `scripts/export_kafka_to_volume.py` | Docker (Kafka já validado em v1.0.1) | 20/20 domínios exportados |
| Integration | Bronze modo `volume` | `notebooks/pipeline_bronze.ipynb` | **Não executável no `/build`** — requer um workspace Databricks real (Free Edition ou outro) | Usuário valida manualmente após o build |
| Regression | Bronze modo `kafka` | `notebooks/pipeline_bronze.ipynb` | **Não executável no `/build`** — mesma restrição de v1.0.1 (sem chamadas ao vivo) | Usuário confirma que o modo default continua igual |
| E2E | `databricks bundle validate -t free_edition` | `databricks.yml` | **Não executável no `/build`** — exige CLI autenticado contra um workspace real | Usuário roda manualmente |

---

## Error Handling

| Error Type | Handling Strategy | Retry? |
|------------|------------------------|------------|
| Tópico Kafka vazio (0 mensagens) | Export grava um Parquet vazio (0 linhas) para o domínio; Bronze trata como `batch_df.isEmpty()` (já existente) | No |
| Tipo do contrato não mapeado em `_PYARROW_TYPE_MAP` | `KeyError` explícito na exportação — falha rápida, não silenciosa | No |
| `source_mode` desconhecido no notebook | `ValueError` explícito (`raise ValueError(...)`) | No |
| Volume/schema `landing` já existe (preflight) | Idempotente — loga "already exists", `exit 0` (mesmo padrão dos outros `ensure_*`) | N/A |
| Kafka inacessível durante o export | `confluent-kafka` propaga erro de conexão — falha visível, sem retry automático (script roda localmente, sob supervisão do usuário) | No |

---

## Configuration

| Config Key | Type | Default | Description |
|------------|------|---------|-----------------|
| `var.bronze_source_mode` | string | `kafka` | `kafka` (streaming) ou `volume` (batch, Free Edition) |
| `var.landing_base` | string | `/Volumes/ubereats_dev/landing/kafka_export` | Base path do Volume de export, por target |
| `--kafka-bootstrap` (export script) | string | `localhost:9092` | Endereço do broker Kafka local |
| `--schema-registry-url` (export script) | string | `http://localhost:8081` | Schema Registry local |
| `--output-dir` (export script) | string | `./kafka_export` | Diretório local de saída do Parquet |

---

## Security Considerations

- O export script roda localmente, sem credenciais novas — reaproveita o Kafka/Schema
  Registry locais já sem autenticação (mesmo modelo de ameaça do resto do projeto, que é
  um ambiente de demonstração)
- Upload para o Volume usa as credenciais já configuradas do Databricks CLI (PAT/OAuth) —
  nenhuma credencial nova introduzida
- Nenhum dado PII adicional é exposto — o Parquet exportado tem exatamente os mesmos
  campos que já trafegam pelo Kafka local

---

## Observability

| Aspect | Implementation |
|--------|---------------------|
| Logging | `export_kafka_to_volume.py` imprime contagem de registros por domínio (`print`, sem dependência de logging estruturado — consistente com `load_to_postgres.py`) |
| Metrics | Nenhuma nova — o modo `volume` não passa por Kafka, então não aparece no `kafka-exporter`/Grafana (esperado; documentar essa lacuna no CLAUDE.md) |
| Tracing | N/A — escopo de demonstração |

---

## Pipeline Architecture

### DAG Diagram

```text
[Kafka pg.public.* (20)] ──export_kafka_to_volume.py──→ [Parquet local (20 dirs)]
                                                                 │
                                                   databricks fs cp (manual)
                                                                 ▼
                                          [Volume landing.kafka_export (20 dirs)]
                                                                 │
                                              pipeline_bronze.ipynb × 20 (source_mode=volume)
                                                                 ▼
                                                        [Bronze Delta × 20]
                                                                 │
                                         (inalterado — Silver/Gold leem Bronze, não Kafka/Volume)
                                                                 ▼
                                                   [Silver × 11] ──→ [Gold × 6]
```

### Incremental Strategy

| Model | Strategy | Key Column | Lookback |
|-------|----------|-------------|----------|
| Export (modo `volume`) | Full snapshot único — sem incremental | N/A | N/A (fora de escopo, ver DEFINE) |
| Bronze (ambos os modos) | `MERGE INTO ... WHEN NOT MATCHED THEN INSERT` (já existente) | `merge_key` (por contrato) | N/A |

### Schema Evolution Plan

| Change Type | Handling | Rollback |
|-------------|----------|--------------|
| Novo campo no contrato | Atualiza `_PYARROW_TYPE_MAP`/schema do export automaticamente (deriva do contrato) — sem mudança de código | Reverter o contrato |
| Mudança de tipo no contrato | Mesma DDL/MERGE para os dois modos — testar os dois antes de promover | Reverter o contrato |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|-------------|
| 1.0 | 2026-06-17 | design-agent | Initial version, a partir de DEFINE_FREE_EDITION_BRONZE.md; inclui Decision 3 (databricks.yml anchors), descoberta durante o design e aprovada via AskUserQuestion |
| 1.1 | 2026-06-17 | ship-agent | Shipped and archived |

---

## Next Step

**Ready for:** `/build .claude/sdd/features/DESIGN_FREE_EDITION_BRONZE.md`
