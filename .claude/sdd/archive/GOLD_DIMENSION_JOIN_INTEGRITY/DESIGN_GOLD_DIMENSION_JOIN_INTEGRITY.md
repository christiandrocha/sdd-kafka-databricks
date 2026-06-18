# DESIGN: Gold Dimension Join Integrity — unicidade forçada no Silver + guard no Gold

> Decide e especifica a correção do padrão sistêmico encontrado na auditoria de linhagem
> dos 6 notebooks Gold: 3 deles fazem JOIN de dimensão Silver numa coluna que **não** é o
> `merge_key` declarado no contrato, o que pode disparar
> `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` em runtime (já aconteceu uma vez,
> em `gold_user_behavior`).

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | GOLD_DIMENSION_JOIN_INTEGRITY |
| **Date** | 2026-06-18 |
| **Author** | design-agent |
| **DEFINE** | Nenhum — ver Scope Note abaixo |
| **Status** | ✅ Shipped (2026-06-18) |

---

## Scope Note (sem DEFINE precedente)

Esta feature nasceu de uma auditoria (`/build` somente-leitura) e não passou por `/define`.
O problema já chegou bem delimitado — 3 notebooks, causa raiz identificada, duas abordagens
candidatas nomeadas pelo usuário (A e B) — então este `/design` pula direto para a decisão
arquitetural e a especificação de build, igual ao que `/define` produziria mas focado
inteiramente na pergunta feita: **qual abordagem corrigir, e por quê**.

---

## A pergunta, recapitulada

3 notebooks Gold fazem JOIN de uma tabela Silver numa coluna que não é o `merge_key`
daquela tabela:

| Notebook | JOIN em | `merge_key` real (contrato) | Risco |
|---|---|---|---|
| `gold_user_behavior` | `silver.users.user_id` | `cpf` | Confirmado em produção (DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE) |
| `gold_driver_performance` | `silver.drivers.driver_id` | `uuid` | Latente, não confirmado |
| `gold_revenue_per_restaurant` | `silver.restaurants.cnpj` | `uuid` | Latente, não confirmado |

Nada no contrato ou no pipeline garante que `user_id`, `driver_id` ou `cnpj` sejam únicos
dentro de `silver.*` — só o `merge_key` tem essa garantia (é a coluna do `MERGE INTO ... ON`).

**Abordagem A** (como proposta originalmente): trocar o `merge_key` do Silver para a coluna
que o Gold usa no JOIN.
**Abordagem B**: `row_number()` guard em cada Gold antes do MERGE (padrão já aplicado em
`gold_user_behavior`).

---

## Decisão

**Nenhuma das duas isoladamente. Híbrido: (1) manter `merge_key` como está — é a chave real
do CDC, não pode mudar — mas adicionar um novo tipo de regra de qualidade,
`check: unique`, que força unicidade da coluna que o Gold usa no JOIN, com quarantine em
caso de violação; (2) manter o `row_number()` guard em todo Gold que faz esse tipo de JOIN,
não só em `gold_user_behavior`, como rede de segurança — não como a correção.**

Ver "Key Decisions" abaixo para o raciocínio completo. Resumo de uma linha: **Abordagem A,
do jeito que foi proposta (trocar `merge_key`), está errada — quebraria ADR-04 e a semântica
de CDC. Mas o espírito da Abordagem A (garantir unicidade no Silver, não só mascarar no
Gold) está certo — só precisa de um mecanismo diferente de "trocar merge_key".**

---

## Architecture Overview

```text
┌──────────────────────────────────────────────────────────────────────────┐
│  SILVER (pipeline_silver.ipynb — já existe)                               │
│                                                                             │
│  Bronze (CDF) ──foreachBatch──→ apply_quality_rules(df, contract)         │
│                                        │                                   │
│                         ┌──────────────┴──────────────┐                  │
│                         ▼                              ▼                  │
│                  clean_df                       quarantine_df            │
│              (not_null, allowed_values,        (mesmo critério hoje +    │
│               not_future — já existem)           NOVO: check=unique)     │
│                         │                                                  │
│                         ▼                                                  │
│         MERGE INTO silver_table ON merge_key   (inalterado — uuid/cpf)   │
└──────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼  silver.drivers / silver.restaurants / silver.users
                              │  agora SEM duplicatas em driver_id / cnpj / user_id
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  GOLD (3 notebooks afetados)                                              │
│                                                                             │
│  JOIN com a dimensão Silver (driver_id / cnpj / user_id)                  │
│                              │                                            │
│                              ▼                                            │
│  row_number() guard antes do MERGE — JÁ EXISTE em gold_user_behavior,    │
│  RETROFIT em gold_driver_performance e gold_revenue_per_restaurant       │
│  (rede de segurança: cobre violações que escaparam da regra Silver,      │
│   dados já existentes antes da regra existir, e qualquer rule gap futuro)│
│                              │                                            │
│                              ▼                                            │
│  MERGE INTO gold_table ON gold_merge_key   (nunca mais deveria falhar)   │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Components

| Component | Purpose | Technology |
|-----------|---------|------------|
| `contracts/loader.py` | Reconhecer `check: unique` como tipo válido de regra | Python |
| `contracts/drivers.yml`, `contracts/restaurants.yml` | Declarar a nova regra `unique` no campo que o Gold usa (`driver_id`, `cnpj`) | YAML |
| `notebooks/pipeline_silver.ipynb` | Implementar a checagem `unique` via anti-join contra a tabela Silver já existente + dentro do próprio batch | PySpark |
| `notebooks/pipeline_users.ipynb` | Mesma checagem, à mão, para `user_id` (não tem contrato YAML — caso especial já documentado) | PySpark |
| `notebooks/cross_domain/gold_driver_performance.ipynb`, `gold_revenue_per_restaurant.ipynb` | Retrofit do `row_number()` guard (padrão de `gold_user_behavior`) | PySpark |
| `docs/adr/005_gold_dimension_join_integrity.md` | Registrar a decisão como ADR formal | Markdown |

---

## Key Decisions

### Decision 1: Rejeitar a Abordagem A literal (trocar `merge_key`)

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted (rejeitado como proposto, aceito em espírito — ver Decision 2) |
| **Date** | 2026-06-18 |

**Context:** A proposta original era alinhar o `merge_key` do Silver à coluna que o Gold usa
no JOIN (`driver_id`, `cnpj`, `user_id`).

**Choice:** Não trocar `merge_key` em nenhum dos 3 contratos/pipelines.

**Rationale:**
1. **ADR-04 é violado.** `cluster_by` precisa conter `merge_key` (`test_06_merge_key_in_cluster_by`,
   `contracts/loader.py::_validate_storage`) e a tabela de alinhamento do ADR-04 já fixa
   `silver.users` com `cluster_by=[cpf]`/`merge_key=cpf`. Trocar para `user_id` exige reescrever
   a Liquid Clustering física da tabela (rewrite, não é só editar YAML) e quebra a tabela de
   alinhamento documentada.
2. **`merge_key` é a identidade do CDC, não um atributo de negócio.** Em `pipeline_silver.ipynb`,
   o `MERGE INTO ... ON t.merge_key = s.merge_key` roda a cada micro-batch (`foreachBatch`,
   `trigger(availableNow=True)`) e depende de `merge_key` ser a chave primária estável vinda do
   Debezium (`uuid` para `drivers`/`restaurants` — é literalmente a PK do Postgres/MySQL de
   origem). Trocar para `driver_id`/`cnpj` redefine "qual update substitui qual linha" com base
   num atributo de negócio que pode, em tese, mudar — um risco novo, maior que o que está
   sendo corrigido.
3. **Para `silver.users`, `cpf` já é a chave correta por desenho.** CLAUDE.md documenta CPF
   como a FK canônica do hub `orders` para usuários (`user_key | CPF | users_mongo.cpf /
   users_mssql.cpf`). `user_id` é um atributo secundário carregado por `search_events`/
   `recommendations` (que não carregam CPF). Trocar `merge_key` para `user_id` inverteria a
   arquitetura de hub já decidida, não a corrigiria.

**Alternatives Rejected:**
1. Trocar `merge_key` para a coluna do JOIN do Gold em todos os 3 contratos — rejeitado pelos
   3 motivos acima.
2. Trocar `merge_key` só em `drivers`/`restaurants` (não em `users`, que já tem justificativa
   de hub) — ainda rejeitado: `uuid` continua sendo a PK real de CDC nesses dois casos também,
   o argumento 2 acima se aplica igualmente.

**Consequences:**
- O JOIN do Gold continua numa coluna que o contrato não garante única — por isso a Decision 2
  é necessária; rejeitar a Abordagem A por si só não resolve nada.

---

### Decision 2: Novo tipo de regra de qualidade `check: unique`, aplicada no Silver, com quarantine

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-18 |

**Context:** O espírito da Abordagem A — garantir unicidade *no Silver*, não só mascarar no
Gold — está certo. Precisa de um mecanismo que não seja "trocar `merge_key`".

**Choice:** Adicionar `unique` ao conjunto de checks válidos em `contracts/loader.py`
(`VALID_CHECKS`), e usá-lo em `contracts/drivers.yml` (`field: driver_id`) e
`contracts/restaurants.yml` (`field: cnpj`):

```yaml
quality:
  rules:
    - { field: driver_id, check: unique, on_failure: quarantine, severity: critical, scope: [silver] }
```

Em `pipeline_silver.ipynb`, `apply_quality_rules` ganha uma checagem adicional (não é uma
`_rule_fail_expr` linha-a-linha como as outras, porque unicidade é uma propriedade de
conjunto): para cada regra `check: unique`, faz um anti-join do batch entrante contra a
tabela Silver já existente (`spark.table(silver_table).select(field, merge_key).distinct()`)
e também contra o próprio batch, para achar valores de `field` que apareceriam associados a
mais de um `merge_key`. Esses registros vão para quarantine; o resto segue o fluxo normal.

Em `pipeline_users.ipynb`, que não tem contrato YAML (`users` é um caso especial — FULL OUTER
JOIN de `users_mongo` + `users_mssql` por CPF, DDL feita à mão), a mesma lógica é replicada
manualmente para `user_id`, espelhando o padrão já estabelecido ali para quarantine de CPF
ausente (`to_quarantine_shape`, `_quarantine_reason`).

**Rationale:**
- Mantém os contratos YAML como fonte única de verdade (consistente com a arquitetura já
  estabelecida) — a regra de unicidade fica declarada ao lado de `not_null`/`allowed_values`,
  não escondida em código Python ad-hoc.
- Segue o precedente já adotado no próprio projeto: "Quarantine users missing CPF instead of
  silently dropping them" (commit `158b2bd`). Uma violação de unicidade é o mesmo tipo de
  problema — dado que não deveria existir, mas existe — e a resposta correta já estabelecida
  pelo time é torná-lo visível via quarantine, não escondê-lo com uma escolha arbitrária de
  "qual linha vence" (que é exatamente o que um `row_number()` sozinho faria, silenciosamente).
- Custo de execução é aceitável na escala do projeto: a tabela existente lida no anti-join
  tem algumas centenas de linhas (`drivers`: 354, `restaurants`: 461) — nada perto do
  argumento de volume que justificaria pular essa checagem (CLAUDE.md já enquadra o dataset
  como "microcosmo arquitetural", não throughput).

**Alternatives Rejected:**
1. Checagem de unicidade só em CI (`test_contracts.py`), depois do fato — rejeitado: não
   impede a linha ruim de entrar no Silver, só avisa depois. O objetivo é nunca deixar o dado
   corrompido chegar ao Gold.
2. `dropDuplicates(subset=[field])` direto no batch, sem quarantine — rejeitado: descarta
   silenciosamente sem registro, repetindo exatamente o anti-padrão que o commit `158b2bd` já
   corrigiu para CPF ausente.

**Consequences:**
- `apply_quality_rules` deixa de ser puramente linha-a-linha; passa a precisar do nome da
  tabela Silver de destino (já disponível como `silver_table` no notebook) para o anti-join.
- Mais uma leitura por micro-batch (`spark.table(silver_table)...`) — overhead pequeno e
  aceitável na escala atual; se o projeto crescesse para volume real, valeria revisitar
  (ex.: cache ou Bloom filter), mas está fora do escopo desta correção.
- `users` precisa de uma cópia manual da lógica (sem contrato YAML) — débito técnico já
  existente, não criado por esta mudança; fica registrado para uma eventual extração de
  `users` para seu próprio contrato.

---

### Decision 3: manter (e retrofitar) o `row_number()` guard como rede de segurança, não como a correção

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-18 |

**Context:** `gold_user_behavior` já tem um `row_number()` guard antes do `MERGE`, adicionado
quando o erro aconteceu em runtime. A pergunta era se isso bastava (Abordagem B) ou se era só
parte da resposta.

**Choice:** Manter o guard em `gold_user_behavior` e adicionar o mesmo padrão em
`gold_driver_performance` e `gold_revenue_per_restaurant`, explicitamente como **defesa em
profundidade**, depois da Decision 2 já ter corrigido a causa raiz no Silver.

**Rationale:**
- A regra `unique` da Decision 2 só vale para dados que passam por ela a partir de agora.
  Linhas que já existem em `silver.drivers`/`silver.restaurants`/`silver.users` antes do
  deploy da regra não são revalidadas retroativamente (a regra atua em `foreachBatch`, não
  via um backfill). O guard no Gold é o que protege contra esse histórico.
- Mesmo com a regra no Silver, o guard custa pouco (uma `Window` + `row_number()`) e cobre
  qualquer gap futuro de regra/contrato sem exigir uma nova mudança de pipeline.
- Consistência: se o padrão é certo para `gold_user_behavior`, é inconsistente deixá-lo de
  fora dos outros dois Gold que têm exatamente o mesmo formato de risco.

**Alternatives Rejected:**
1. Confiar só na regra do Silver e remover o guard de `gold_user_behavior` — rejeitado: layer
   única de defesa para um erro que já aconteceu uma vez em produção é um risco desnecessário.
2. Aplicar o guard só nos dois notebooks novos e não revisar `gold_user_behavior` — já está
   correto, nenhuma mudança necessária lá além do que já existe.

**Consequences:**
- 3 notebooks Gold compartilham o mesmo padrão de guard — mais fácil de auditar/ensinar do que
  3 soluções diferentes.
- Mantém uma pequena redundância de defesa (Silver bloqueia a entrada, Gold garante a saída) —
  aceito deliberadamente, não é duplicação acidental.

---

## File Manifest

| # | File | Action | Purpose | Agent | Dependencies |
|---|------|--------|---------|-------|--------------|
| 1 | `contracts/loader.py` | Modify | Adiciona `"unique"` a `VALID_CHECKS` | @data-quality-analyst | None |
| 2 | `contracts/drivers.yml` | Modify | Nova regra `check: unique` em `driver_id` | @data-quality-analyst | 1 |
| 3 | `contracts/restaurants.yml` | Modify | Nova regra `check: unique` em `cnpj` | @data-quality-analyst | 1 |
| 4 | `notebooks/pipeline_silver.ipynb` | Modify | `apply_quality_rules` ganha checagem `unique` via anti-join contra `silver_table` + batch atual | @spark-engineer | 1, 2, 3 |
| 5 | `notebooks/pipeline_users.ipynb` | Modify | Mesma checagem de unicidade para `user_id`, à mão (sem contrato YAML), espelhando o padrão de quarantine de CPF já existente | @spark-engineer | None |
| 6 | `notebooks/cross_domain/gold_driver_performance.ipynb` | Modify | Retrofit do `row_number()` guard antes do `MERGE`, igual ao de `gold_user_behavior` | @spark-engineer | None |
| 7 | `notebooks/cross_domain/gold_revenue_per_restaurant.ipynb` | Modify | Retrofit do `row_number()` guard antes do `MERGE` (além da correção de viés já aplicada) | @spark-engineer | None |
| 8 | `docs/adr/005_gold_dimension_join_integrity.md` | Create | Registra a decisão (Decisions 1–3 acima) como ADR formal, no mesmo padrão dos ADRs 001–004 já existentes | (direct) | None |
| 9 | `CLAUDE.md` | Modify | Documenta o novo tipo de regra `unique` e o padrão de guard duplo (Silver + Gold) | (direct) | 1–8 |
| 10 | `tests/test_contracts.py` | Modify | `VALID_CHECKS` já é testado indiretamente via `load_contract` — adicionar 1 teste explícito de que `drivers.yml`/`restaurants.yml` carregam com a nova regra sem erro | @data-quality-analyst | 1, 2, 3 |

**Total Files:** 10

---

## Agent Assignment Rationale

| Agent | Files Assigned | Why This Agent |
|-------|-----------------|-------------------|
| @data-quality-analyst | 1, 2, 3, 10 | Descrição explícita: "data quality specialist... data contracts... investigando problemas de dados" — exatamente o tipo de regra sendo adicionada |
| @spark-engineer | 4, 5, 6, 7 | Transformações PySpark dentro de notebooks já existentes — mesmo agente que mantém a lógica de MERGE/quarantine atual |
| (direct) | 8, 9 | ADR e atualização de CLAUDE.md são edição direta de documentação, sem necessidade de um agente especializado (mesmo padrão usado em features anteriores deste projeto) |

---

## Code Patterns

### Pattern 1: `contracts/loader.py` — novo check válido

```python
VALID_CHECKS: frozenset[str] = frozenset({"not_null", "allowed_values", "not_future", "unique"})
```

### Pattern 2: contrato YAML — nova regra

```yaml
# contracts/drivers.yml (e analogamente contracts/restaurants.yml com field: cnpj)
quality:
  rules:
    - { field: uuid,                 check: not_null,   on_failure: reject,     severity: critical, scope: [bronze]         }
    - { field: driver_id,            check: not_null,   on_failure: quarantine, severity: critical, scope: [silver]         }
    - { field: driver_id,            check: unique,     on_failure: quarantine, severity: critical, scope: [silver]         }
    - { field: dt_current_timestamp, check: not_future, on_failure: warn,       severity: warning,  scope: [bronze, silver] }
```

### Pattern 3: `pipeline_silver.ipynb` — checagem `unique` via anti-join

```python
def _unique_violation_keys(df, field, merge_key, silver_table):
    """Valores de `field` que não podem entrar no Silver porque colidiriam
    com um `merge_key` diferente — já no Silver ou dentro do próprio batch."""
    existing = spark.table(silver_table).select(field, merge_key).distinct()
    incoming = df.select(field, merge_key).distinct()

    cross_batch = (
        incoming.alias("i")
        .join(existing.alias("e"), field)
        .filter(col(f"i.{merge_key}") != col(f"e.{merge_key}"))
        .select(field)
    )
    in_batch = incoming.groupBy(field).count().filter("count > 1").select(field)

    return cross_batch.unionByName(in_batch).distinct()


def apply_quality_rules(df, contract, silver_table):
    quarantine_rules = [
        r for r in contract["quality"]["rules"]
        if "silver" in r["scope"] and r["on_failure"] == "quarantine"
    ]
    if not quarantine_rules:
        return df, df.filter(lit(False))

    row_level_rules = [r for r in quarantine_rules if r["check"] != "unique"]
    unique_rules     = [r for r in quarantine_rules if r["check"] == "unique"]

    fail_expr = lit(False)
    for r in row_level_rules:
        fail_expr = fail_expr | _rule_fail_expr(r)

    for r in unique_rules:
        bad_keys = _unique_violation_keys(df, r["field"], contract["table"]["merge_key"], silver_table)
        fail_expr = fail_expr | col(r["field"]).isin([row[0] for row in bad_keys.collect()])

    return df.filter(~fail_expr), df.filter(fail_expr)
```

> Nota para o Build: `bad_keys.collect()` é aceitável na escala atual (poucas centenas de
> valores no máximo); se a lista crescer, trocar por um `left_anti` join direto no DataFrame
> em vez de materializar a lista em driver. Validar com `silver_table` vazio na primeira
> execução (não deve quebrar — `existing` será um DataFrame vazio, `cross_batch` também).

### Pattern 4: `pipeline_users.ipynb` — mesma ideia, à mão, para `user_id`

```python
# Mesmo padrão de to_quarantine_shape já usado para CPF ausente — agora para user_id duplicado
existing_user_ids = spark.table(silver_users_table).select("user_id", "cpf").filter(col("user_id").isNotNull())
dup_user_ids = (
    users_df.alias("i")
    .join(existing_user_ids.alias("e"), "user_id")
    .filter(col("i.cpf") != col("e.cpf"))
    .select("user_id")
    .distinct()
)
users_df, user_id_quarantine_df = (
    users_df.join(dup_user_ids, "user_id", "left_anti"),
    users_df.join(dup_user_ids, "user_id", "left_semi"),
)
# user_id_quarantine_df segue para quarantine_table com _quarantine_reason="duplicate_user_id"
```

### Pattern 5: guard `row_number()` no Gold — retrofit (mesmo padrão de `gold_user_behavior`)

```python
from pyspark.sql import Window
from pyspark.sql.functions import row_number, desc, col

w = Window.partitionBy("driver_id").orderBy(desc("_computed_at"))  # ou "restaurant_cnpj"
perf_df_deduped = (
    perf_df
    .withColumn("_rn", row_number().over(w))
    .filter(col("_rn") == 1)
    .drop("_rn")
)
perf_df_deduped.createOrReplaceTempView("gold_driver_performance_batch")
# ... MERGE como já existe, usando a view deduplicada
```

---

## Data Flow

```text
1. Bronze CDF chega em pipeline_silver.ipynb (foreachBatch, já existente)
   │
   ▼
2. apply_quality_rules(df, contract, silver_table) — NOVO: também checa `unique`
   │              │
   │              └─→ quarantine_df (inclui violações de unicidade) → quarantine_table
   ▼
3. clean_df → MERGE INTO silver_table ON merge_key (inalterado)
   │
   ▼
4. silver.drivers / silver.restaurants / silver.users agora sem duplicatas
   no campo que o Gold usa (driver_id / cnpj / user_id) — a partir do deploy da regra
   │
   ▼
5. Gold (3 notebooks) faz o JOIN como já fazia — agora com row_number() guard
   antes do MERGE em todos os 3, não só em gold_user_behavior
   │
   ▼
6. MERGE INTO gold_table — não deveria mais falhar com
   DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE
```

---

## Integration Points

Nenhum sistema novo — tudo dentro do Unity Catalog já existente (`ubereats_dev`/`ubereats_prod`).

---

## Testing Strategy

| Test Type | Scope | Files | Tools | Coverage Goal |
|-----------|-------|-------|-------|---------------|
| Syntax | Contratos carregam sem erro com a nova regra | `contracts/drivers.yml`, `contracts/restaurants.yml` | `load_contract()` via `tests/test_contracts.py` | 100% |
| Unit | `VALID_CHECKS` aceita `"unique"`, rejeita check desconhecido | `contracts/loader.py` | pytest | Caso novo + regressão dos checks existentes |
| Syntax | Notebooks seguem JSON válido após edição | `pipeline_silver.ipynb`, `pipeline_users.ipynb`, os 2 Gold | `python3 -c "import json; json.load(...)"` | 100% |
| Integration | Anti-join de unicidade contra Silver/Gold real | os 5 notebooks modificados | **Não executável no `/build`** — requer workspace Databricks real (mesma limitação documentada em features anteriores) | Usuário valida manualmente, incluindo: tabela Silver vazia (primeira execução), batch com duplicata interna, batch colidindo com linha já existente |
| Regression | `not_null`/`allowed_values`/`not_future` continuam funcionando após a refatoração de `apply_quality_rules` | `pipeline_silver.ipynb` | Revisão manual do diff — sem mudança de comportamento para os checks existentes | Nenhuma regressão |

---

## Error Handling

| Error Type | Handling Strategy | Retry? |
|------------|-------------------|--------|
| `silver_table` ainda não existe na primeira execução | `spark.table(silver_table)` funciona porque a DDL já é criada antes (`cell-contract`, `CREATE TABLE IF NOT EXISTS`) — `existing` resultará em DataFrame vazio, sem erro | No |
| Check `"unique"` referenciando campo inexistente no schema | Já coberto por `test_03_quality_rules_reference_existing_fields` (genérico, não precisa de teste novo) | No |
| Volume de violações de unicidade inesperadamente alto | `quarantine_df` cresce normalmente — mesmo tratamento dos outros critérios de quarantine, sem limite especial | No |
| Gold MERGE ainda recebe duplicata (gap entre o deploy da regra Silver e dados históricos) | Coberto pelo guard `row_number()` (Decision 3) — não deveria mais propagar para erro de runtime | No |

---

## Configuration

Nenhuma configuração nova — a regra de unicidade é declarada inteiramente dentro do contrato
YAML já existente (`quality.rules`), sem novos widgets ou variáveis do DABs.

---

## Security Considerations

- Nenhuma superfície nova — a checagem de unicidade lê apenas tabelas Silver já dentro do
  mesmo Unity Catalog, sem credenciais ou acesso novo.

---

## Observability

| Aspect | Implementation |
|--------|----------------|
| Logging | `process_silver_batch` já imprime contagem de quarantine por batch — passa a incluir as violações de unicidade no mesmo total (sem distinguir motivo no print; o motivo fica registrado na própria linha de quarantine, mesmo padrão de `_quarantine_reason` usado para CPF ausente) |
| Metrics | Nenhuma nova — mesmo padrão de `print()` já usado no resto do projeto |
| Tracing | N/A — fora de escopo (projeto de demonstração) |

---

## Pipeline Architecture

### Schema Evolution Plan

| Change Type | Handling | Rollback |
|-------------|----------|----------|
| Nova regra `quality.rules` (`check: unique`) | Não altera `schema:` nem `storage:` — é aditivo e não quebra `schema_evolution: new_fields: allowed` / `removed_fields: forbidden` já declarado | Remover a regra do YAML; nenhuma migração de dado necessária |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-06-18 | design-agent | Versão inicial, decidindo entre Abordagem A e B da auditoria Gold — resultado: híbrido (Decisions 1–3) |
| 1.1 | 2026-06-18 | ship-agent | Shipped and archived |

---

## Next Step

**Ready for:** `/build .claude/sdd/features/DESIGN_GOLD_DIMENSION_JOIN_INTEGRITY.md`
