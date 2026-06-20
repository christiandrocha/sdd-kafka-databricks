# DESIGN: v1.1.0 — Migração Bronze+Silver para Databricks Lakeflow (DLT)

> Substitui `pipeline_bronze.ipynb` (20x) + `pipeline_silver.ipynb` (11x) por um único
> pipeline Lakeflow Spark Declarative Pipelines gerado por loop sobre `contracts/*.yml`,
> em `dev`/`prod`. Gold, `pipeline_users.ipynb` e `free_edition` ficam inalterados.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | LAKEFLOW_MIGRATION |
| **Date** | 2026-06-18 |
| **Author** | design-agent |
| **DEFINE** | [DEFINE_LAKEFLOW_MIGRATION.md](./DEFINE_LAKEFLOW_MIGRATION.md) |
| **Status** | ✅ Shipped (2026-06-18) |

---

## Pesquisa das premissas A-001 a A-005 (antes de fixar o manifesto)

O DEFINE pedia explicitamente para validar as 5 premissas técnicas contra a documentação real
do Lakeflow antes de comprometer o file manifest. Resultado (via `WebSearch`/`WebFetch` contra
`docs.databricks.com` em 2026-06-18):

| # | Premissa | Resultado | Fonte |
|---|----------|-----------|-------|
| A-001 | `dlt.apply_changes()` com `sequence_by` | **Confirmado, com 1 achado importante**: `apply_changes()` foi renomeado para `create_auto_cdc_flow()` — mesma assinatura, Databricks recomenda migrar para o novo nome. Precisa de `create_streaming_table()` declarando o alvo antes. `stored_as_scd_type=1` (default) evita exigir colunas `__START_AT`/`__END_AT` (só obrigatórias em `scd_type=2`) | `docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes` |
| A-002 | `cluster_by` em tabelas Lakeflow | **Confirmado** — `dp.create_streaming_table(..., cluster_by=[...])` e `@dp.table(..., cluster_by=[...])` aceitam a mesma lista de colunas do `storage.cluster_by` do contrato | `docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-streaming-table` |
| A-003 | Tabelas geradas em loop | Não testado ao vivo, mas é um padrão documentado e comum em Lakeflow (factory function retornando uma função decorada, chamada dentro de um loop) — mantido como premissa de alta confiança, não 100% verificado | Conhecimento geral de Lakeflow, sem fetch direto de um exemplo oficial |
| A-004 | `pipeline_task` num Job DABs, com `depends_on` | **Confirmado, com YAML exato** — `pipeline_task: {pipeline_id: ${resources.pipelines.<key>.id}, full_refresh: false}`; outras tasks usam `depends_on: [{task_key: ...}]` normalmente | Exemplo da comunidade Databricks, consistente com `docs.databricks.com/aws/en/jobs/pipeline` |
| A-005 | `bundle validate` sem workspace ao vivo | Não contradito por nada encontrado — mesmo comportamento já observado para `resources.jobs` | Inferência, não uma citação direta |

**Achado adicional, não previsto no DEFINE:** o módulo Python foi renomeado.
`import dlt` → `from pyspark import pipelines as dp`. `@dlt.table` → `@dp.table`,
`@dlt.view` → `@dp.temporary_view`, `@dlt.expect` → `@dp.expect`. O alias legado `dlt` ainda
funciona (compatibilidade retroativa), mas a documentação atual recomenda `dp`. Este design
usa `dp` (a API atual) — **risco residual**: a versão exata do runtime DBR do workspace real
precisa suportar `pyspark.pipelines`; isso não pôde ser confirmado sem acesso a um workspace
ao vivo (mesma restrição de todas as features anteriores). Ver Error Handling.

---

## Architecture Overview

```text
┌──────────────────────────────────────────────────────────────────────────┐
│  ANTES (v1.0.x — inalterado em free_edition)                              │
│                                                                             │
│  [Kafka pg.public.*] → pipeline_bronze.ipynb (20x, DABs) → bronze.*       │
│  bronze.* → pipeline_silver.ipynb (11x, DABs) → silver.* / quarantine.*  │
│  Lineage UC: agrupado por caminho de notebook — BUG relatado              │
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│  DEPOIS (dev/prod) — pipelines/bronze_silver_dlt.py, 1 pipeline Lakeflow  │
│                                                                             │
│  for contract in contracts/*.yml:           (loop, 20+11 = 31 domínios)   │
│                                                                             │
│    [Kafka topic] ──dp.table──→ bronze.<domain>                            │
│         (cluster_by do contrato; @dp.expect_all_or_drop no merge_key)     │
│                          │                                                 │
│                          ▼  dp.read_stream (nativo, sem depends_on)        │
│              <domain>_silver_candidate (dp.temporary_view)                │
│                          │                                                 │
│              ┌───────────┴────────────┐                                  │
│              ▼                        ▼                                  │
│  quarantine.<domain>          <domain>_silver_clean                      │
│  (dp.table — predicado        (linhas que passam quality.rules           │
│   inverso das regras +         + check:unique via stream-static          │
│   anti-join de unicidade)      join contra silver.<domain> atual)        │
│                                         │                                 │
│                                         ▼ dp.create_auto_cdc_flow         │
│                                  silver.<domain>                          │
│                                  (keys=[merge_key], sequence_by=          │
│                                   __source_ts_ms, scd_type=1)            │
└──────────────────────────────────────────────────────────────────────────┘
                          │
            (job-level depends_on, via pipeline_task)
                          ▼
       [silver_users (notebook, inalterado)] → [6× gold_* (notebooks, inalterados)]

┌──────────────────────────────────────────────────────────────────────────┐
│  free_edition — totalmente inalterado                                     │
│  pipeline_bronze.ipynb / pipeline_silver.ipynb, source_mode=volume        │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Components

| Component | Purpose | Technology |
|-----------|---------|------------|
| `pipelines/bronze_silver_dlt.py` | Loop sobre `contracts/*.yml`, registra `bronze.*`/`silver.*`/`quarantine.*` dinamicamente | `pyspark.pipelines` (Lakeflow) |
| `contracts/dlt_adapter.py` | Traduz um contrato já carregado (`contracts/loader.py::load_contract`) em expectations/predicados DLT — sem mudar o formato do contrato | Python puro, sem dependência de PySpark para a tradução em si |
| `databricks.yml` (modificado) | Novo `resources.pipelines.ubereats_bronze_silver`; `dev`/`prod` trocam 30 tasks por 1 `pipeline_task` | Databricks Asset Bundles |
| `docs/adr/006_lakeflow_migration.md` | ADR formal documentando a decisão de sobrescrever o ADR-03 | Markdown |

---

## Key Decisions

### Decision 1: API atual (`pyspark.pipelines as dp`), não o alias legado `dlt`

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-18 |

**Context:** A pesquisa encontrou que o módulo foi renomeado de `dlt` para
`pyspark.pipelines`, com o alias legado ainda funcionando.

**Choice:** Usar `from pyspark import pipelines as dp` e os decorators atuais (`@dp.table`,
`@dp.temporary_view`, `@dp.expect_all_or_drop`, etc.) em todo o código novo.

**Rationale:** É a API documentada e recomendada atualmente; usar o alias legado deliberadamente
criaria dívida técnica imediata num código que está sendo escrito do zero.

**Alternatives Rejected:**
1. Usar `import dlt` (legado) — rejeitado: funciona hoje, mas é exatamente o tipo de escolha
   que o projeto evitaria para qualquer outra dependência nova.

**Consequences:**
- Risco residual: a versão do runtime DBR do workspace real precisa suportar
  `pyspark.pipelines`. Não verificável sem acesso a um workspace ao vivo — ver Error Handling
  para o plano de contingência (fallback para `dlt`).

---

### Decision 2: `dp.create_auto_cdc_flow()` para o upsert por `merge_key`, em Bronze **e** Silver

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-18 |

**Context:** Hoje, Bronze faz `MERGE INTO ... WHEN NOT MATCHED THEN INSERT` (nunca
atualiza) e Silver faz `MERGE INTO ... WHEN MATCHED AND s.__source_ts_ms > t.__source_ts_ms
THEN UPDATE ... WHEN NOT MATCHED THEN INSERT`. O dataset de teste usa só `op='r'` (snapshot)
e `op='c'` (create) — nunca `op='u'` (update) — então, na prática, nenhum `merge_key` jamais
recebe uma segunda mensagem com timestamp diferente hoje.

**Choice:** Usar `create_auto_cdc_flow(target=..., source=..., keys=[merge_key],
sequence_by=col("__source_ts_ms"), stored_as_scd_type=1)` tanto para Bronze quanto para
Silver.

**Rationale:** Para o dataset atual, o comportamento observável é idêntico ao `MERGE`
manual de hoje (sem updates reais, então a branch `WHEN MATCHED AND newer THEN UPDATE` nunca
dispara). Mas é estritamente mais correto em geral — se uma atualização real de linha algum
dia chegar (um `op='u'` legítimo), `create_auto_cdc_flow` aplicaria a atualização
corretamente, enquanto o `WHEN NOT MATCHED THEN INSERT`-apenas de Bronze hoje **descartaria
silenciosamente** essa atualização. Isto não é uma mudança de escopo desta feature — é a
mesma garantia de idempotência de hoje, expressa de forma declarativa, com uma correção de
borda que não é exercitada pelo dataset de teste atual.

**Alternatives Rejected:**
1. Replicar literalmente o `WHEN NOT MATCHED THEN INSERT` de Bronze com um
   `@dp.table`/`dropDuplicates` simples, sem `create_auto_cdc_flow` — rejeitado: a
   deduplicação de um DataFrame de streaming via `dropDuplicates` tem o mesmo problema de
   estado vinculado ao ciclo de vida da query que o checkpoint de hoje (não sobrevive a um
   full refresh/reset do pipeline) — não é mais robusto, só mais código.

**Consequences:**
- Pequeno desvio de comportamento (estritamente mais correto, nunca observável no dataset
  atual) deve ser documentado explicitamente no ADR e no `CLAUDE.md` — não escondido

---

### Decision 3: Quarentena via `@dp.table` com predicado inverso, alimentada por uma `@dp.temporary_view` privada compartilhada

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-18 |

**Context:** Já decidido no brainstorm (par limpo+quarantine por domínio). Falta especificar
*como* evitar duplicar a lógica de filtro entre as duas tabelas.

**Choice:** Uma `@dp.temporary_view(name=f"{domain}_silver_candidate")` lê `bronze.<domain>`
uma única vez. `quarantine.<domain>` lê essa view e filtra pelo predicado de falha (igual a
`_rule_fail_expr` de hoje, traduzido para SQL/Column). O `create_auto_cdc_flow` de
`silver.<domain>` lê a MESMA view, já excluindo (via anti-join) as linhas que aparecem em
`quarantine.<domain>`.

**Rationale:** Evita duplicar a definição das regras de qualidade em dois lugares — a view
privada é o único ponto que lê Bronze; tanto quarentena quanto Silver derivam dela.

**Alternatives Rejected:**
1. `quarantine.<domain>` e `silver.<domain>` lendo `bronze.<domain>` diretamente, cada um
   recomputando o predicado — rejeitado: duplica a tradução do contrato em dois lugares,
   risco de divergência se um dos dois for editado sem o outro.

**Consequences:**
- `contracts/dlt_adapter.py` precisa expor o predicado de falha como uma única função
  reutilizável, não uma por tabela

---

### Decision 4: `check: unique` como *stream-static join* dentro da função da view, não como `@dp.expect`

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-18 |

**Context:** `@dp.expect*` só avalia expressões booleanas linha-a-linha; `check: unique`
precisa comparar contra o estado atual de `silver.<domain>` (anti-join), exatamente como
`_unique_violation_values()` faz hoje em `pipeline_silver.ipynb`.

**Choice:** Dentro da função da `@dp.temporary_view`/`@dp.table` de quarentena, fazer um
*stream-static join* explícito: `streaming_df.join(spark.read.table(silver_table), ...)`.
Joins stream-static no Spark Structured Streaming reavaliam o lado estático a cada
disparo de microbatch — a mesma propriedade que `spark.table(silver_table)` já tinha dentro
de `process_silver_batch()` hoje.

**Rationale:** Não é uma limitação do Lakeflow per se — é uma propriedade do motor Spark
Structured Streaming subjacente, disponível tanto dentro de uma função `foreachBatch` quanto
dentro de uma função decorada `@dp.table`. A lógica de `_unique_violation_values()` é
portável quase literalmente, só trocando o ponto de chamada.

**Alternatives Rejected:**
1. Tentar expressar unicidade via `@dp.expect` — rejeitado, não é uma expressão linha-a-linha
   (impossível de expressar nesse modelo)
2. Mover a checagem de unicidade para fora do Lakeflow (um job separado, pós-pipeline) —
   rejeitado: reintroduz uma janela onde dados duplicados ficam visíveis em Silver antes da
   checagem rodar, regressão de comportamento em relação a hoje

**Consequences:**
- **Não verificado ao vivo** — stream-static joins dentro de uma função decorada Lakeflow não
  foram testados contra um workspace real nesta sessão; é a peça de maior risco técnico do
  design. Ver Error Handling e Testing Strategy.

---

### Decision 5: Novo ADR (`docs/adr/006`) documentando a sobrescrita do ADR-03, não uma edição silenciosa

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-18 |

**Context:** ADR-03 rejeitou Lakeflow/DLT explicitamente. Esta feature reverte essa decisão
para o escopo Bronze+Silver (não para Gold/`silver_users`, que permanecem como o ADR-03
original já tinha decidido).

**Choice:** Criar `docs/adr/006_lakeflow_migration.md`, referenciando e contextualizando
ADR-03 (não editá-lo) — registrando por que a avaliação mudou (custo de lineage incorreto,
confirmado por auditoria, agora supera o argumento de "menos controle explícito").

**Rationale:** Mantém o histórico de decisões íntegro — qualquer leitor futuro vê os dois
ADRs e entende a evolução do raciocínio, em vez de uma reversão silenciosa que pareceria
inconsistente.

**Consequences:**
- Mais um arquivo de ADR a manter; aceito, é exatamente o propósito de ADRs

---

## File Manifest

| # | File | Action | Purpose | Agent | Dependencies |
|---|------|--------|---------|-------|---------------|
| 1 | `contracts/dlt_adapter.py` | Create | Traduz contrato → expectations/predicados DLT (sem mudar `contracts/*.yml`) | @data-quality-analyst | None |
| 2 | `pipelines/bronze_silver_dlt.py` | Create | Loop sobre `contracts/*.yml`, registra Bronze+Silver+quarantine via Lakeflow | @lakeflow-pipeline-builder | 1 |
| 3 | `databricks.yml` | Modify | `resources.pipelines.ubereats_bronze_silver`; `dev`/`prod` substituem 30 tasks por 1 `pipeline_task`; `free_edition` inalterado | @ci-cd-specialist | 2 |
| 4 | `docs/adr/006_lakeflow_migration.md` | Create | ADR formal — sobrescreve o escopo Bronze/Silver do ADR-03 | (direct) | 1, 2, 3 |
| 5 | `CLAUDE.md` | Modify | Documenta a nova arquitetura, a exceção do Free Edition, o desvio de comportamento da Decision 2 | (direct) | 1, 2, 3, 4 |
| 6 | `tests/test_dlt_adapter.py` | Create | Testes unitários de `contracts/dlt_adapter.py` — tradução pura, sem precisar de Spark | @data-quality-analyst | 1 |

**Total Files:** 6

---

## Agent Assignment Rationale

| Agent | Files Assigned | Why This Agent |
|-------|------------------|--------------------|
| @data-quality-analyst | 1, 6 | Tradução de regras de qualidade de contrato — mesma especialização usada em `GOLD_DIMENSION_JOIN_INTEGRITY` |
| @lakeflow-pipeline-builder | 2 | Descrição explícita: "Builds Databricks Lakeflow (DLT) pipelines... Uses KB + MCP validation for production-ready pipelines" |
| @ci-cd-specialist | 3 | Único agente cuja descrição cita explicitamente Databricks Asset Bundles — mesmo agente usado em `FREE_EDITION_BRONZE` para mudanças em `databricks.yml` |
| (direct) | 4, 5 | Documentação — não justifica um agente especializado, mesmo padrão das features anteriores |

> Nota para o Build: `@lakeflow-pipeline-builder` (ou `@lakeflow-architect`/`@lakeflow-expert`)
> deve validar as partes do Pattern 2 abaixo marcadas como "não verificado ao vivo" contra a
> documentação oficial mais recente antes de finalizar — esta sessão usou `WebSearch`/
> `WebFetch`, não um workspace real.

---

## Code Patterns

### Pattern 1: `contracts/dlt_adapter.py` — tradução contrato → DLT

```python
from __future__ import annotations


def to_reject_expectations(contract: dict, scope: str) -> dict[str, str]:
    """Regras on_failure=reject do scope dado → dict para @dp.expect_all_or_drop."""
    return {
        f"{r['field']}_{r['check']}": _condition_sql(r)
        for r in contract["quality"]["rules"]
        if scope in r["scope"] and r["on_failure"] == "reject"
    }


def to_warn_expectations(contract: dict, scope: str) -> dict[str, str]:
    """Regras on_failure=warn do scope dado → dict para @dp.expect_all (não bloqueia)."""
    return {
        f"{r['field']}_{r['check']}": _condition_sql(r)
        for r in contract["quality"]["rules"]
        if scope in r["scope"] and r["on_failure"] == "warn"
    }


def quarantine_row_level_predicate(contract: dict, scope: str) -> str | None:
    """SQL boolean: True quando a linha FALHA alguma regra quarantine que não seja 'unique'."""
    rules = [
        r for r in contract["quality"]["rules"]
        if scope in r["scope"] and r["on_failure"] == "quarantine" and r["check"] != "unique"
    ]
    if not rules:
        return None
    fail_conditions = [f"NOT ({_condition_sql(r)})" for r in rules]
    return " OR ".join(fail_conditions)


def unique_check_fields(contract: dict, scope: str) -> list[str]:
    """Campos com check=unique, on_failure=quarantine, no scope dado."""
    return [
        r["field"] for r in contract["quality"]["rules"]
        if scope in r["scope"] and r["on_failure"] == "quarantine" and r["check"] == "unique"
    ]


def _condition_sql(rule: dict) -> str:
    """SQL boolean: True quando a linha PASSA a regra (mesma semântica de _rule_fail_expr invertida)."""
    field, check = rule["field"], rule["check"]
    if check == "not_null":
        return f"{field} IS NOT NULL"
    if check == "allowed_values":
        values = ", ".join(f"'{v}'" for v in rule["values"])
        return f"{field} IS NULL OR {field} IN ({values})"
    if check == "not_future":
        return f"{field} IS NULL OR {field} <= current_timestamp()"
    if check == "unique":
        return "true"  # tratado separadamente via unique_check_fields, não aqui
    raise ValueError(f"unknown check type for DLT translation: {check!r}")
```

> Nota para o Build: validar se `@dp.expect_all_or_drop`/`@dp.expect_all` aceitam um `dict[str,
> str]` de `nome_da_regra -> condição SQL` (formato usado pelas variantes "_all_" do `dlt`
> legado) ou se a API atual (`dp`) mudou essa assinatura — não confirmado nesta sessão.

---

### Pattern 2: `pipelines/bronze_silver_dlt.py` — esqueleto do loop

```python
import sys
from pathlib import Path

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, expr

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from contracts.loader import load_contract
from contracts.dlt_adapter import (
    quarantine_row_level_predicate,
    to_reject_expectations,
    to_warn_expectations,
    unique_check_fields,
)

CATALOG = spark.conf.get("ubereats.catalog", "ubereats_dev")
CONTRACTS_DIR = Path(__file__).resolve().parent.parent / "contracts"


def _unique_violations(candidate_df, fields, merge_key, silver_table):
    """Stream-static join — mesma lógica de _unique_violation_values() em pipeline_silver.ipynb,
    só que expressa como transformação declarativa em vez de chamada dentro de foreachBatch."""
    bad = candidate_df.sparkSession.createDataFrame([], candidate_df.schema).limit(0)
    for field in fields:
        existing = spark.read.table(silver_table).select(field, merge_key).distinct()
        cross_batch = (
            candidate_df.select(field, merge_key).distinct().alias("i")
            .join(existing.alias("e"), field)
            .filter(col(f"i.{merge_key}") != col(f"e.{merge_key}"))
        )
        bad = bad.unionByName(
            candidate_df.join(cross_batch.select(field).distinct(), field, "left_semi")
        )
    return bad.distinct()


def register_domain(contract_path: Path) -> None:
    contract = load_contract(contract_path)
    domain = contract["table"]["name"]
    merge_key = contract["table"]["merge_key"]
    cluster_by = contract["storage"]["cluster_by"]
    kafka_topic = contract["table"]["kafka_topic"]

    bronze_table = f"{CATALOG}.bronze.{domain}"
    silver_table = f"{CATALOG}.silver.{domain}"
    quarantine_table = f"{CATALOG}.quarantine.{domain}"

    @dp.table(name=bronze_table, cluster_by=cluster_by, comment=f"Bronze: {domain}")
    @dp.expect_all_or_drop(to_reject_expectations(contract, scope="bronze"))
    def _bronze():
        # Avro decode via Schema Registry — mesma lógica de pipeline_bronze.ipynb hoje,
        # parametrizada por kafka_topic em vez de widget.
        return (
            spark.readStream.format("kafka")
            .option("subscribe", kafka_topic)
            .load()
            .select(expr("substring(value, 6)").alias("avro_bytes"))
            # .select(from_avro(...))  -- decode omitido aqui, igual ao notebook atual
            .withColumn("_ingested_at", current_timestamp())
        )

    @dp.temporary_view(name=f"{domain}_silver_candidate")
    @dp.expect_all(to_warn_expectations(contract, scope="silver"))
    def _candidate():
        return dp.read_stream(bronze_table)

    row_predicate = quarantine_row_level_predicate(contract, scope="silver")
    unique_fields = unique_check_fields(contract, scope="silver")

    @dp.table(name=quarantine_table, comment=f"Quarantine: {domain}")
    def _quarantine():
        candidate = dp.read_stream(f"{domain}_silver_candidate")
        rowlevel_bad = candidate.filter(row_predicate) if row_predicate else candidate.limit(0)
        unique_bad = (
            _unique_violations(candidate, unique_fields, merge_key, silver_table)
            if unique_fields else candidate.limit(0)
        )
        return rowlevel_bad.unionByName(unique_bad).distinct()

    @dp.temporary_view(name=f"{domain}_silver_clean")
    def _clean():
        candidate = dp.read_stream(f"{domain}_silver_candidate")
        bad = dp.read_stream(quarantine_table)
        return candidate.join(bad, merge_key, "left_anti")

    dp.create_streaming_table(name=silver_table, cluster_by=cluster_by)
    dp.create_auto_cdc_flow(
        target=silver_table,
        source=f"{domain}_silver_clean",
        keys=[merge_key],
        sequence_by=col("__source_ts_ms"),
        stored_as_scd_type=1,
    )


for _contract_path in sorted(CONTRACTS_DIR.glob("*.yml")):
    register_domain(_contract_path)
```

> **Nota para o Build — partes não verificadas ao vivo:**
> 1. Misturar `dp.read_stream()` de uma `@dp.temporary_view` dentro de outra `@dp.table`
>    (`_quarantine` lendo `_candidate`, `_clean` lendo `_candidate` E `quarantine_table`) —
>    confirmar se Lakeflow permite múltiplos consumidores da mesma view/stream sem
>    duplicar o processamento do Kafka subjacente.
> 2. `_unique_violations()` faz `spark.read.table(silver_table)` antes de `silver_table` ter
>    sido criado pela primeira vez — `dp.create_streaming_table()` precisa rodar/resolver
>    antes que isso seja lido; validar a ordem de inicialização do grafo Lakeflow.
> 3. O decode Avro via Schema Registry (omitido no esqueleto acima por brevidade) precisa ser
>    portado de `pipeline_bronze.ipynb::cell-schema-registry` palavra por palavra — não é uma
>    parte nova, só precisa ser colada dentro de `_bronze()`.

---

### Pattern 3: `databricks.yml` — novo `resources.pipelines` + `pipeline_task`

```yaml
resources:
  pipelines:
    ubereats_bronze_silver:
      name: ubereats_bronze_silver
      catalog: ${var.catalog}
      continuous: false              # triggered — preserva o custo "escala a zero"
      libraries:
        - file:
            path: ../pipelines/bronze_silver_dlt.py
      configuration:
        ubereats.catalog: ${var.catalog}

  jobs:
    ubereats_pipeline:
      # dev/prod: 30 tasks bronze_*/silver_* são REMOVIDOS daqui.
      # task_definitions (usado só por free_edition) permanece com os 37 originais.
      tasks:
        - task_key: bronze_silver_pipeline
          pipeline_task:
            pipeline_id: ${resources.pipelines.ubereats_bronze_silver.id}
            full_refresh: false

        - task_key: silver_users
          depends_on:
            - task_key: bronze_silver_pipeline
          <<: *silver_users_task   # notebook_task inalterado

        - task_key: gold_payments_by_status
          depends_on:
            - task_key: bronze_silver_pipeline   # antes: silver_payments
          <<: *gold_payments_by_status_task
        # ... os outros 5 gold_*, mesma troca de depends_on
```

> Nota para o Build: `free_edition` continua referenciando `*serverless_tasks` (os 37 tasks
> originais, inalterados) — esta mudança de `tasks:` só se aplica aos targets `dev`/`prod`.

---

## Data Flow

```text
1. databricks bundle deploy -t dev
   │
   ▼
2. resources.pipelines.ubereats_bronze_silver criado/atualizado no workspace
   │
   ▼
3. Job ubereats_pipeline roda: task bronze_silver_pipeline dispara o pipeline Lakeflow
   │
   ▼
4. Lakeflow resolve o DAG interno (31 domínios, Bronze→candidate→quarantine/clean→Silver)
   automaticamente — sem orquestração externa para essa parte
   │
   ▼
5. task silver_users roda (notebook, inalterado) após bronze_silver_pipeline completar
   │
   ▼
6. 6× task gold_* rodam (notebooks, inalterados) após bronze_silver_pipeline completar
   │
   ▼
7. Unity Catalog Lineage agora mostra 31 nós de tabela distintos para Bronze/Silver
```

---

## Integration Points

| External System | Integration Type | Authentication |
|------------------|----------------------|--------------------|
| Kafka local (`pg.public.*`) | `spark.readStream.format("kafka")`, dentro do pipeline Lakeflow | Nenhuma (mesmo modelo de hoje) |
| Schema Registry local | REST via `requests`, dentro de `_bronze()` | Nenhuma (mesmo modelo de hoje) |
| Unity Catalog | Tabelas Lakeflow nativas (`bronze.*`/`silver.*`/`quarantine.*`) | Já configurado — sem credencial nova |

---

## Testing Strategy

| Test Type | Scope | Files | Tools | Coverage Goal |
|-----------|-------|-------|-------|-----------------|
| Unit | Tradução contrato → expectations/predicados | `contracts/dlt_adapter.py` | `tests/test_dlt_adapter.py` (pytest, sem Spark) | Todos os 4 tipos de `check` + todos os `on_failure` |
| Syntax | `pipelines/bronze_silver_dlt.py` | mesmo arquivo | `python3 -c "import ast; ast.parse(...)"` (sem executar — precisa de runtime Spark/Lakeflow) | 100% sintaxe |
| Syntax | `databricks.yml` | mesmo arquivo | `python3 -c "import yaml; ..."`, `databricks bundle validate -t dev -o json` | Anchors resolvem, `pipeline_task` referencia `resources.pipelines` corretamente |
| Integration | Pipeline Lakeflow rodando de fato (Decisions 2 e 4) | `pipelines/bronze_silver_dlt.py` | **Não executável no `/build`** — requer workspace Databricks real com Lakeflow habilitado | Usuário valida manualmente — ver Acceptance Tests do DEFINE (AT-001 a AT-010) |
| Regression | `free_edition` inalterado | `databricks.yml` | `databricks bundle validate -t free_edition -o json`, comparar plano antes/depois | Plano de deploy idêntico |

---

## Error Handling

| Error Type | Handling Strategy | Retry? |
|------------|------------------------|------------|
| Workspace real não suporta `pyspark.pipelines` (DBR antigo) | Fallback documentado: trocar `from pyspark import pipelines as dp` por `import dlt as dp` no topo do arquivo — o resto do código não muda, porque os nomes dos decorators usados (`dp.table`, `dp.expect_all_or_drop`, etc.) têm equivalentes diretos no alias legado | No — decisão manual do usuário ao ver o erro de import |
| `create_auto_cdc_flow` não aceita a assinatura esperada | Validar contra a documentação da versão exata do DBR do workspace antes de generalizar para os 31 domínios — testar com 1 domínio primeiro | No |
| Stream-static join (Decision 4) não funciona como esperado dentro de `@dp.table` | Fallback: mover a checagem de unicidade para fora do Lakeflow, como um passo de validação separado pós-pipeline (perde a propriedade de "nunca deixar duplicata visível", mas não bloqueia o resto da migração) | No — decisão de design a ser revisitada se isso ocorrer |
| `databricks bundle validate` falha por causa do `pipeline_task`/`resources.pipelines` | Mesma resposta de sempre: revisar a sintaxe YAML contra a documentação oficial, sem chamadas ao vivo durante `/build` | No |

---

## Configuration

| Config Key | Type | Default | Description |
|------------|------|---------|-----------------|
| `resources.pipelines.ubereats_bronze_silver.configuration["ubereats.catalog"]` | string | `${var.catalog}` | Repassa o catalog (`ubereats_dev`/`ubereats_prod`) para dentro do pipeline, lido via `spark.conf.get(...)` |
| `resources.pipelines.ubereats_bronze_silver.continuous` | bool | `false` | Pipeline disparado, não always-on — preserva a propriedade de custo já valorizada |

---

## Security Considerations

- Nenhuma credencial nova — mesmo Kafka/Schema Registry locais sem autenticação já usados
  pelos notebooks atuais
- Nenhum dado PII adicional exposto — os mesmos campos que já trafegam hoje

---

## Observability

| Aspect | Implementation |
|--------|---------------------|
| Logging | Lakeflow tem seu próprio event log nativo (métricas de linhas processadas/rejeitadas por expectation) — substitui os `print()` de hoje; `quarantine.*` continua sendo a fonte de verdade para auditoria de dados, não só métricas |
| Metrics | Native Lakeflow pipeline event log — não estava disponível no modelo de notebooks; é um ganho, não documentado como meta nesta feature, mas vale registrar no CLAUDE.md |
| Tracing | N/A — fora de escopo |

---

## Pipeline Architecture

### DAG Diagram

Ver Architecture Overview acima.

### Schema Evolution Plan

| Change Type | Handling | Rollback |
|-------------|----------|--------------|
| Novo campo em `contracts/*.yml` | `dlt_adapter.py` deriva tudo do contrato — nenhuma mudança de código necessária | Reverter o contrato |
| Nova regra de qualidade | Se for um `check` já suportado (`not_null`/`allowed_values`/`not_future`/`unique`), nenhuma mudança de código; um `check` novo precisaria de uma branch nova em `_condition_sql()` | Remover a regra do contrato |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-06-18 | design-agent | Initial version, a partir de DEFINE_LAKEFLOW_MIGRATION.md; inclui pesquisa ao vivo das premissas A-001 a A-005 via WebSearch/WebFetch |
| 1.1 | 2026-06-18 | ship-agent | Shipped and archived — ver BUILD_REPORT_LAKEFLOW_MIGRATION.md e SHIPPED_2026-06-18.md |

---

## Next Step

**Ready for:** `/build .claude/sdd/features/DESIGN_LAKEFLOW_MIGRATION.md`
