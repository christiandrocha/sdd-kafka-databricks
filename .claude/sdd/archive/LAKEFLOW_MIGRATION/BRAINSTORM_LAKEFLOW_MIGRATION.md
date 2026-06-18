# BRAINSTORM: v1.1.0 — Migração Bronze+Silver para Databricks Lakeflow (DLT)

> Exploratory session to clarify intent and approach before requirements capture

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | LAKEFLOW_MIGRATION |
| **Date** | 2026-06-18 |
| **Author** | brainstorm-agent |
| **Status** | Ready for Define |

---

## Initial Idea

**Raw Input:** Migrar `pipeline_bronze.ipynb` + `pipeline_silver.ipynb` para Databricks
Lakeflow Declarative Pipelines (DLT), mantendo a mesma lógica de MERGE/quality rules, os
mesmos 20 domínios Bronze + 11 Silver, os contratos YAML como fonte única de verdade, e
ADR-04 (`cluster_by = merge_key`). Motivação: Unity Catalog Lineage não distingue execuções
parametrizadas do mesmo notebook — agrupa todas as 20 execuções de `pipeline_bronze.ipynb`
(e as 11 de `pipeline_silver.ipynb`) como se fossem uma coisa só.

**Context Gathered:**
- `.claude/03_design.md` (ADR-03) já avaliou e **rejeitou explicitamente** Lakeflow/DLT como
  alternativa às 2 notebooks parametrizadas: `"Lakeflow DLT pipelines (rejected — different
  abstraction, less explicit control)"`. Esta feature reabre essa decisão deliberadamente —
  não é uma lacuna que passou batido.
- O `/build` anterior (auditoria de "Bronze lendo Bronze") não encontrou nenhum bug de código
  nos 3 lugares investigados (`pipeline_bronze.ipynb`, `pipeline_silver.ipynb`,
  `databricks.yml`) — a hipótese mais provável é que o sintoma é um artefato de como a UI/
  sistema de lineage do Unity Catalog atribui nós por *caminho de notebook*, não por
  *execução parametrizada*. Migrar para Lakeflow ataca essa causa raiz diretamente, porque
  cada `@dlt.table` é seu próprio nó de lineage — não há "um notebook reusado 20x" no modelo
  DLT.
- O modelo de `quality.rules` dos contratos (`not_null`, `allowed_values`, `not_future`, e o
  `unique` recém-adicionado em `GOLD_DIMENSION_JOIN_INTEGRITY`) não mapeia 1:1 para
  `@dlt.expect*`: DLT não tem uma ação nativa de "quarantine para outra tabela" (só
  warn/drop/fail-pipeline), e `check: unique` é uma checagem cross-row/cross-batch via
  anti-join — `@dlt.expect` só avalia expressões booleanas linha-a-linha.

**Technical Context Observed (for Define):**

| Aspect | Observation | Implication |
|--------|-------------|--------------|
| Likely Location | `pipelines/` (novo) + `databricks.yml` (modificado) | Novo diretório para o código DLT; `notebooks/pipeline_bronze.ipynb`/`pipeline_silver.ipynb` continuam existindo (uso exclusivo do `free_edition`) |
| Relevant KB Domains | `databricks` (`.claude/kb/databricks.md`), agentes `lakeflow-architect`/`lakeflow-expert`/`lakeflow-pipeline-builder`/`lakeflow-specialist` disponíveis para o `/build` | Especialistas já existem no projeto para a fase de implementação |
| IaC Patterns | `databricks.yml` (DABs) já existente — ganha um novo bloco `resources.pipelines` | Primeira vez que este projeto usa `resources.pipelines` (hoje só usa `resources.jobs`) |

---

## Discovery Questions & Answers

| # | Question | Answer | Impact |
|---|----------|--------|--------|
| 1 | ADR-03 já rejeitou DLT por "menos controle explícito" — como abordar isso agora? | Sobrescrever ADR-03 deliberadamente: o ganho de lineage correto compensa a perda de controle explícito | A migração segue como uma decisão consciente, não uma reversão silenciosa — precisa de um novo ADR documentando a mudança de avaliação |
| 2 | DLT não tem "quarantine" nativo — como preservar isso? | Dois `@dlt.table` por domínio (limpo + quarantine), mesmo predicado em direções opostas | Dobra a contagem de funções para os domínios com regras de quarantine (~14 dos 20+11); preserva 100% do comportamento atual, sem regressão da feature de CPF/unicidade recém-enviada |
| 3 | Migrar Free Edition também, ou só dev/prod? | Só dev/prod no v1.1.0; Free Edition continua nos notebooks atuais até um v1.1.1 separado verificar suporte a Lakeflow serverless | Evita bloquear a migração toda numa restrição de plataforma ainda não verificada; mas quebra a simetria do ADR-06 (dev/prod/free_edition compartilhando os mesmos 37 task bodies) — dev/prod e free_edition passam a rodar códigos diferentes para Bronze/Silver, não só configs de compute diferentes |
| 4 | Os 6 notebooks Gold migram também? | Não — ficam como notebooks | O problema de lineage relatado não existe em Gold (1 notebook : 1 execução, já corretamente atribuído hoje); a lógica de Gold (agregação em 2 estágios, guard `row_number()`) é exatamente o tipo de controle imperativo que ADR-03 já dizia que DLT não oferece |
| 5 | Existe alguma referência de pipeline Lakeflow/DLT para se basear? | Nenhuma — design do zero | O design usa padrões gerais de Lakeflow (`@dlt.table` gerado em loop, `dlt.read_stream`, `dlt.apply_changes`), sem um exemplo prévio para validar contra |
| 6 | `pipeline_users.ipynb` (task `silver_users`) também migra? | Não — fica como notebook, mesma razão do Gold | É 1 task só (não parametrizado), sem o bug de lineage relatado; seu `FULL OUTER JOIN` + full-refresh (`mode("overwrite")`) não se encaixa no modelo incremental/streaming do DLT |

**Minimum Questions:** 3 — atendido (6 perguntas, incluindo a de amostras)

---

## Sample Data Inventory

| Type | Location | Count | Notes |
|------|----------|-------|-------|
| Input files | N/A | 0 | Nenhum exemplo de pipeline Lakeflow/DLT disponível no projeto ou apontado pelo usuário |
| Output examples | N/A | 0 | — |
| Ground truth | N/A | 0 | — |
| Related code | `notebooks/pipeline_bronze.ipynb`, `notebooks/pipeline_silver.ipynb`, `contracts/*.yml`, `contracts/loader.py`, `contracts/spark_schema.py` | 5 | Lógica e contratos atuais — fonte de verdade para a tradução, não um exemplo de DLT em si |

**How samples will be used:**

- A lógica atual (`apply_quality_rules`, `to_create_table_ddl`, `merge_to_bronze`) é o
  comportamento de referência que o design DLT precisa reproduzir exatamente — qualquer
  desvio precisa ser justificado explicitamente no `/design`, não introduzido por acidente.

---

## Approaches Explored

### Approach A: Um pipeline Lakeflow combinado, gerado por loop ⭐ Recommended

**Description:** Um único arquivo Python itera sobre `contracts/*.yml` e registra
dinamicamente, por domínio: `@dlt.table(name=f"bronze.{domain}")` (lê Kafka) →
`@dlt.table(name=f"silver.{domain}")` + par de quarantine (lê a tabela Bronze via
`dlt.read_stream(...)`). A dependência Bronze→Silver é resolvida automaticamente pelo DAG do
próprio Lakeflow — sem precisar de `depends_on` no `databricks.yml` para essa parte.

**Pros:**
- Um pipeline, um grafo de lineage — cada domínio ainda tem seu próprio nó de tabela
  (resolve o problema relatado)
- `dlt.apply_changes()` (CDC helper nativo) substitui o `MERGE INTO ... WHEN MATCHED AND
  newer THEN UPDATE` escrito à mão hoje — mesma semântica (keep-latest-by-sequence,
  upsert-by-key), só que declarativa
- `cluster_by` mapeia direto para `@dlt.table(..., cluster_by=[...])` — ADR-04 não precisa de
  tratamento especial

**Cons:**
- `check: unique` ainda precisa do mesmo anti-join de hoje — não é coberto por
  `dlt.apply_changes()` nem por `@dlt.expect`, precisa virar uma `@dlt.view` própria antes da
  tabela DLT
- Quarantine dobra a contagem de funções `@dlt.table` (dois por domínio, nos domínios que
  têm regra de quarantine)

**Why Recommended:** Preserva o espírito DRY do ADR-03 original (1 arquivo, 1 loop, em vez
de 31 pipelines), resolve o problema de lineage relatado diretamente na causa raiz, e ganha
de graça uma simplificação real (`apply_changes`) na lógica de upsert por `merge_key`.

---

### Approach B: Pipeline Bronze separado do pipeline Silver

**Description:** Dois arquivos, dois pipelines Lakeflow — mais próximo do modelo mental
atual de "2 notebooks".

**Pros:**
- Mapeamento mais direto e familiar do estado atual (1 pipeline Bronze, 1 pipeline Silver)

**Cons:**
- Perde a resolução automática de dependência intra-pipeline do DLT — Silver passaria a ler
  Bronze via `spark.readStream.table(...)` em vez de `dlt.read_stream(...)`
- `databricks.yml` precisaria de uma dependência explícita de job entre os dois pipelines —
  reintroduz exatamente o tipo de wiring de orquestração que o DLT deveria remover

---

### Approach C: Um pipeline Lakeflow por domínio (31 pipelines)

**Description:** Cada um dos 20 domínios Bronze + 11 Silver ganha seu próprio pipeline
Lakeflow dedicado.

**Pros:**
- Isolamento total por domínio

**Cons:**
- Recria o anti-padrão que o próprio ADR-03 original já rejeitou ("60 notebooks estáticos"),
  agora em formato de pipeline — 31 configs para tocar em qualquer mudança de lógica
- Rejeitado sem ambiguidade; incluído aqui só para registrar que foi considerado

---

## Data Engineering Context

### Source Systems

| Source | Type | Volume Estimate | Current Freshness |
|--------|------|------------------|--------------------|
| Kafka (`pg.public.*`, 20 tópicos) | Streaming (Debezium CDC) | 129.353 registros totais, `order_items` = 110.001 (85%) | `trigger(availableNow=True)` — batch disparado, não contínuo |

### Data Flow Sketch

```text
[Kafka pg.public.* (20 tópicos)] → [Lakeflow pipeline: bronze.* (20 @dlt.table)]
                                            │  dlt.read_stream (nativo, sem DABs depends_on)
                                            ▼
                                  [silver.* (11 @dlt.table) + quarantine.* pares]
                                            │
                          (job-level depends_on, via pipeline_task)
                                            ▼
                    [silver_users (notebook, inalterado)] → [6× gold_* (notebooks, inalterados)]
```

### Key Data Questions Explored

| # | Question | Answer | Impact |
|---|----------|--------|--------|
| 1 | O pipeline deve ser contínuo ou disparado? | Disparado (`triggered`), não `continuous` | Preserva a propriedade "escala a zero" já valorizada nos ADRs existentes — sem custo de cluster sempre ligado |
| 2 | `free_edition` precisa do mesmo pipeline? | Não neste v1.1.0 — fica nos notebooks atuais | Evita bloquear a migração numa restrição de Free Edition ainda não verificada (suporte a Lakeflow serverless) |
| 3 | Quem mais lê `bronze.*`/`silver.*` além deste pipeline? | Gold (6 notebooks) e `pipeline_users.ipynb` — ambos continuam lendo via `spark.table()` normal, sem mudança | Lakeflow só precisa expor `bronze.*`/`silver.*` como tabelas Unity Catalog normais — consumidores externos ao pipeline não percebem diferença |

---

## Selected Approach

| Attribute | Value |
|-----------|-------|
| **Chosen** | Approach A — um pipeline Lakeflow combinado, gerado por loop sobre `contracts/*.yml` |
| **User Confirmation** | 2026-06-18, via `AskUserQuestion` (Pipeline structure) |
| **Reasoning** | Resolve a causa raiz do problema de lineage relatado, preserva o espírito DRY do ADR-03 original, e ganha uma simplificação real via `dlt.apply_changes()` para o upsert por `merge_key` |

---

## Key Decisions Made

| # | Decision | Rationale | Alternative Rejected |
|---|----------|-----------|------------------------|
| 1 | Sobrescrever ADR-03 deliberadamente (DLT agora é aceito para Bronze+Silver) | Lineage correto por domínio é um requisito real, confirmado por uma auditoria anterior sem causa de código encontrada — o trade-off original ("menos controle explícito") agora pesa menos que o custo de um lineage ilegível | Manter os notebooks parametrizados e aceitar o lineage agrupado como limitação permanente |
| 2 | Quarantine via 2 `@dlt.table` por domínio, nunca via `expect_or_drop` puro | `expect_or_drop` descartaria silenciosamente — reverteria o trabalho já enviado de "nunca esconder dados ruins, sempre quarentenar" (CPF ausente, `check: unique`) | Aceitar `expect_or_drop` e perder a quarentena |
| 3 | `check: unique` implementado como `@dlt.view` (anti-join) antes da tabela DLT, não como `@dlt.expect` | `@dlt.expect` só avalia expressões linha-a-linha; unicidade é uma propriedade de conjunto, como já era em `pipeline_silver.ipynb` | Tentar forçar a checagem dentro de um `@dlt.expect` (não é expressível) |
| 4 | `dlt.apply_changes()` substitui o `MERGE INTO` manual para a lógica de upsert por `merge_key` | É o helper nativo do Lakeflow para exatamente esse padrão (keep-latest-by-sequence, upsert-by-key) — simplificação real, não cosmética | Continuar escrevendo `MERGE INTO` manual dentro de uma função `@dlt.table` |
| 5 | Um pipeline combinado (Bronze+Silver), não dois separados | Resolução automática de dependência via `dlt.read_stream`, sem wiring extra no `databricks.yml` | Approach B (2 pipelines) |
| 6 | `free_edition` fora de escopo neste v1.1.0 | Suporte a Lakeflow serverless lá é desconhecido — não bloquear a migração numa verificação pendente | Migrar os 3 targets juntos |
| 7 | Gold (6 notebooks) fora de escopo | Já tem lineage correto hoje (1 notebook : 1 execução); lógica imperativa recém-corrigida (agregação 2 estágios, guard `row_number()`) não se encaixa no modelo declarativo | Migrar Gold também, por "consistência" |
| 8 | `pipeline_users.ipynb` fora de escopo | Mesma razão do Gold — 1 task só, sem o bug relatado; `FULL OUTER JOIN` + full-refresh não se encaixa no modelo incremental do DLT | Migrar `silver_users` também |
| 9 | Pipeline `triggered`, não `continuous` | Preserva a propriedade "escala a zero" já documentada como positiva nos ADRs existentes | Pipeline contínuo (always-on) |

---

## Features Removed (YAGNI)

| Feature Suggested | Reason Removed | Can Add Later? |
|--------------------|------------------|-------------------|
| Migração do `free_edition` para Lakeflow | Suporte a serverless DLT lá não foi verificado; não bloquear o v1.1.0 nisso | Yes — v1.1.1 dedicado, depois de verificar |
| Migração dos 6 notebooks Gold | Sem o bug de lineage relatado; lógica imperativa não se encaixa no modelo declarativo | Yes, se uma necessidade real surgir — não há pressão hoje |
| Migração de `pipeline_users.ipynb` | Mesma razão do Gold — sem bug, lógica não se encaixa | Yes, mesma condição do item acima |
| Pipeline `continuous` (always-on) | Seria uma regressão de custo (cluster sempre ligado) sem ganho de requisito — nenhum SLA de freshness em tempo real foi pedido | Yes, se um requisito de freshness em tempo real aparecer |
| Approach C (1 pipeline por domínio) | Recria o anti-padrão de 60 notebooks que o ADR-03 original já rejeitou | No — seria preciso um motivo novo e forte para reconsiderar |
| Approach B (Bronze/Silver separados) | Perde resolução automática de dependência, reintroduz wiring de orquestração | No, a menos que surja uma razão operacional para cadências diferentes entre Bronze e Silver |

---

## Incremental Validations

| Section | Presented | User Feedback | Adjusted? |
|---------|-----------|------------------|--------------|
| Approaches de estrutura do pipeline (A/B/C) | ✅ | Confirmou Approach A | No |
| Impacto no `databricks.yml` + exceção de `pipeline_users.ipynb` | ✅ | Confirmou que `pipeline_users.ipynb` também fica como notebook | No |

**Minimum Validations:** 2 — atendido

---

## Suggested Requirements for /define

### Problem Statement (Draft)

Unity Catalog Lineage não distingue as 20 execuções parametrizadas de
`pipeline_bronze.ipynb` (nem as 11 de `pipeline_silver.ipynb`) entre si — atribui lineage por
caminho de notebook, não por execução/domínio — tornando o grafo de lineage de Bronze/Silver
inútil para auditar qual tabela de domínio alimentou qual outra.

### Target Users (Draft)

| User | Pain Point |
|------|--------------|
| Data engineer auditando o pipeline | Não consegue confiar no grafo de lineage do Unity Catalog para Bronze/Silver — todas as 20/11 execuções aparecem misturadas |
| Recrutador/revisor técnico avaliando o projeto | A história de "Data Contracts como diferencial" perde força se o lineage automático do Unity Catalog não reflete os contratos corretamente |

### Success Criteria (Draft)

- [ ] Cada um dos 20 domínios Bronze e 11 domínios Silver aparece como nó de lineage
      distinto no Unity Catalog, atribuído ao seu próprio contrato/fonte
- [ ] As 4 regras de qualidade (`not_null`, `allowed_values`, `not_future`, `unique`)
      reproduzem exatamente o mesmo comportamento de pass/fail/quarantine que hoje —
      zero regressão
- [ ] ADR-04 (`cluster_by = merge_key`) vale para toda tabela migrada
- [ ] `dev`/`prod`: pipeline Lakeflow substitui os 30 tasks `bronze_*`/`silver_*`; Gold (6) +
      `silver_users` (1) continuam como notebooks, conectados via um task `pipeline_task`
- [ ] `free_edition`: funcionalmente inalterado (`pipeline_bronze.ipynb`/
      `pipeline_silver.ipynb`, `source_mode=volume`)
- [ ] Pipeline roda em modo `triggered`, não `continuous`

### Constraints Identified

- Suporte a Lakeflow serverless no Free Edition não foi verificado — fora de escopo deste
  v1.1.0, propositalmente
- `contracts/*.yml` continuam como fonte única de verdade — esta migração não muda o formato
  do contrato, só adiciona um adaptador contrato→expectations DLT
- ADR-03 está sendo deliberadamente sobrescrito — precisa de um novo ADR documentando a
  mudança de avaliação, não uma edição silenciosa do ADR-03 original
- `check: unique` (`GOLD_DIMENSION_JOIN_INTEGRITY`, já em produção) precisa de um caminho de
  implementação que não seja uma `@dlt.expect` nativa

### Out of Scope (Confirmed)

- Migração do `free_edition` para Lakeflow
- Migração dos 6 notebooks Gold
- Migração de `pipeline_users.ipynb`
- Modo `continuous` (always-on)
- Approach C (1 pipeline por domínio) e Approach B (Bronze/Silver separados)

---

## Session Summary

| Metric | Value |
|--------|-------|
| Questions Asked | 6 |
| Approaches Explored | 3 |
| Features Removed (YAGNI) | 6 |
| Validations Completed | 2 |
| Duration | ~1 sessão |

---

## Next Step

**Ready for:** `/define .claude/sdd/features/BRAINSTORM_LAKEFLOW_MIGRATION.md`
