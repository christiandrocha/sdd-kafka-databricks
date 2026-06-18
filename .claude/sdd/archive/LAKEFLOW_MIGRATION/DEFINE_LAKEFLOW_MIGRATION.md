# DEFINE: v1.1.0 — Migração Bronze+Silver para Databricks Lakeflow (DLT)

> Migrar a ingestão Bronze (20 domínios) e a limpeza Silver (11 domínios) dos 2 notebooks
> parametrizados atuais para um pipeline Databricks Lakeflow Declarative Pipelines, para que
> cada domínio apareça como nó de lineage distinto no Unity Catalog

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | LAKEFLOW_MIGRATION |
| **Date** | 2026-06-18 |
| **Author** | define-agent |
| **Status** | ✅ Shipped (2026-06-18) |
| **Clarity Score** | 14/15 |
| **Predecessor** | [BRAINSTORM_LAKEFLOW_MIGRATION.md](./BRAINSTORM_LAKEFLOW_MIGRATION.md) |

---

## Problem Statement

Unity Catalog Lineage atribui lineage por *caminho de notebook*, não por *execução
parametrizada* — então as 20 execuções de `pipeline_bronze.ipynb` (uma por domínio) e as 11
execuções de `pipeline_silver.ipynb` aparecem agrupadas como se fossem uma coisa só, tornando
o grafo de lineage de Bronze/Silver inútil para auditar qual tabela de domínio alimentou qual
outra. Uma auditoria de código anterior (`/build` "Bronze lendo Bronze") não encontrou nenhum
bug nos notebooks ou no `databricks.yml` — a causa é estrutural ao modelo de notebooks
parametrizados via DABs, não corrigível sem mudar o modelo de execução.

---

## Target Users

| User | Role | Pain Point |
|------|------|------------|
| Data engineer auditando o pipeline | Mantém/depura o pipeline Bronze/Silver | Não consegue confiar no grafo de lineage do Unity Catalog para os 20+11 domínios — todas as execuções aparecem misturadas sob o mesmo notebook |
| Recrutador/revisor técnico avaliando o projeto | Avalia a maturidade de engenharia do projeto | A história de "Data Contracts como diferencial" perde força se o lineage automático do Unity Catalog não reflete os contratos corretamente, por domínio |

---

## Goals

| Priority | Goal |
|----------|------|
| **MUST** | Cada um dos 20 domínios Bronze e 11 domínios Silver aparece como `@dlt.table` próprio — nó de lineage distinto no Unity Catalog |
| **MUST** | As 4 regras de qualidade do contrato (`not_null`, `allowed_values`, `not_future`, `unique`) produzem exatamente o mesmo resultado de pass/fail/quarantine que produzem hoje — zero regressão de dados |
| **MUST** | ADR-04 (`cluster_by = merge_key`) vale para toda tabela migrada, sem exceção |
| **MUST** | `contracts/*.yml` continuam como única fonte de verdade — nenhuma duplicação de schema/regras fora do YAML |
| **SHOULD** | A lógica de upsert por `merge_key` usa `dlt.apply_changes()` em vez de `MERGE INTO` manual, simplificando o código mantido |
| **SHOULD** | `databricks.yml` (`dev`/`prod`) substitui os 30 tasks `bronze_*`/`silver_*` por um único `resources.pipelines` + 1 task `pipeline_task`, mantendo Gold (6) e `silver_users` (1) como notebooks dependentes |
| **COULD** | Documentar a mudança como um novo ADR, explicitamente superando a rejeição original de DLT no ADR-03 |

---

## Success Criteria

- [ ] 20 tabelas `bronze.*` + 11 tabelas `silver.*` aparecem como nós de lineage individuais no Unity Catalog, cada um atribuído ao seu próprio contrato — 0 agrupamento entre domínios
- [ ] Rodar o pipeline Lakeflow duas vezes sobre o mesmo lote de dados produz zero registros duplicados em qualquer tabela Bronze ou Silver (idempotência via `dlt.apply_changes`)
- [ ] Para cada um dos 31 domínios migrados (20 Bronze + 11 Silver), a contagem de linhas em `clean`/`quarantine` é idêntica, registro a registro, ao resultado produzido pelos notebooks atuais para o mesmo conjunto de dados de teste
- [ ] `databricks bundle validate -t dev` e `-t prod` não apresentam erros após a migração
- [ ] `databricks bundle validate -t free_edition` continua passando, sem nenhuma mudança de comportamento (mesmos 37 tasks de hoje)
- [ ] Pipeline Lakeflow configurado como `continuous: false` (triggered) — confirmado via `databricks bundle validate -o json`

---

## Acceptance Tests

| ID | Scenario | Given | When | Then |
|----|----------|-------|------|------|
| AT-001 | Lineage por domínio, não agrupado | Pipeline Lakeflow `ubereats_bronze_silver` implantado em `dev` | O pipeline roda uma vez, populando os 20 domínios Bronze + 11 Silver | A aba de Lineage do Unity Catalog mostra 31 nós de tabela distintos, cada um com sua fonte/destino correta — nenhum domínio aparece misturado com outro |
| AT-002 | Paridade de `not_null` (Bronze) | Um registro com `merge_key` nulo chega via Kafka para um domínio Bronze | O pipeline processa o lote | O registro é rejeitado (não inserido em `bronze.*`), mesmo comportamento de `clean_df = batch_df.filter(col(merge_key).isNotNull())` de hoje |
| AT-003 | Paridade de `allowed_values`/`not_future`/quarantine (Silver) | Um registro viola uma regra `scope: [silver]`, `on_failure: quarantine` (ex.: `event_name` fora de `allowed_values`) | O pipeline processa o lote | O registro aparece em `quarantine.<domain>`, não em `silver.<domain>` — mesmo resultado que `apply_quality_rules()` produz hoje |
| AT-004 | Paridade de `check: unique` (`driver_id`/`cnpj`) | Dois registros Bronze com `merge_key` (uuid) diferentes, mas o mesmo `driver_id` ou `cnpj` | O pipeline processa o lote completo via Silver | Apenas um dos dois chega a `silver.drivers`/`silver.restaurants`; o outro é quarentenado — mesmo comportamento do anti-join em `pipeline_silver.ipynb` hoje |
| AT-005 | `ADR-04` vale para toda tabela migrada | Contrato de qualquer domínio migrado, com `storage.cluster_by` definido | A tabela Lakeflow correspondente é criada | `DESCRIBE TABLE EXTENDED` mostra `Clustering Columns` idêntico ao `cluster_by` do contrato |
| AT-006 | Idempotência do upsert por `merge_key` | Pipeline já rodou uma vez para um domínio | O mesmo lote de dados é reprocessado (ex.: re-trigger manual) | Contagem de linhas em `bronze.*`/`silver.*` não muda — zero duplicatas, via `dlt.apply_changes` |
| AT-007 | `databricks.yml` — Gold continua dependendo corretamente de Silver | Pipeline Lakeflow + os 6 tasks `gold_*` + `silver_users` no mesmo job `ubereats_pipeline` (`dev`/`prod`) | `databricks bundle validate -t dev -o json` é executado | Os 6 tasks `gold_*` têm `depends_on` apontando para o novo task `pipeline_task` (ou para `silver_users`, conforme o domínio), nunca para um `task_key` `bronze_*`/`silver_*` que não existe mais |
| AT-008 | `free_edition` inalterado | Target `free_edition` no `databricks.yml`, antes e depois da migração | `databricks bundle validate -t free_edition -o json` é executado nos dois momentos | O plano de deploy é idêntico — ainda 37 tasks, ainda `pipeline_bronze.ipynb`/`pipeline_silver.ipynb`, ainda `source_mode=volume` |
| AT-009 | Pipeline não é `continuous` | Definição do pipeline em `databricks.yml`/`resources.pipelines` | `databricks bundle validate -o json` é executado | O campo de modo do pipeline indica execução disparada (`continuous: false` ou equivalente), não always-on |
| AT-010 | Gold e `silver_users` continuam funcionando sem alteração | `silver.*` populado pelo novo pipeline Lakeflow | `gold_*` (6 notebooks) e `silver_users` (`pipeline_users.ipynb`) rodam normalmente | Leem `silver.*`/`bronze.*` via `spark.table()` como sempre fizeram — nenhuma mudança de código nesses notebooks é necessária para eles funcionarem |

---

## Out of Scope

- Migração do target `free_edition` para Lakeflow — suporte a pipelines serverless lá não foi
  verificado; permanece nos notebooks atuais (`source_mode=volume`) até um v1.1.1 dedicado
- Migração dos 6 notebooks Gold — já têm lineage correto hoje (1 notebook : 1 execução); a
  lógica imperativa recém-corrigida (agregação em 2 estágios, guard `row_number()`) não se
  encaixa no modelo declarativo
- Migração de `pipeline_users.ipynb` (task `silver_users`) — mesma razão do Gold: 1 task só,
  sem o bug de lineage relatado, e seu `FULL OUTER JOIN` + full-refresh não se encaixa no
  modelo incremental do DLT
- Modo `continuous` (always-on) do pipeline Lakeflow — sem requisito de freshness em tempo
  real; manteria um cluster sempre ligado sem ganho correspondente
- Mudança no formato dos contratos YAML (`contracts/*.yml`) — a migração usa um adaptador
  contrato→expectations DLT, não um novo formato de contrato
- Resolver a inconsistência de numeração de ADRs já flagada em `GOLD_DIMENSION_JOIN_INTEGRITY`
  (`.claude/03_design.md` vs. `docs/adr/`) — seguir referenciando por caminho de arquivo, como
  já decidido naquela feature

---

## Constraints

| Type | Constraint | Impact |
|------|------------|--------|
| Technical | Suporte a Lakeflow serverless no Databricks Free Edition não foi verificado | Decisão consciente de excluir `free_edition` do escopo, em vez de bloquear a migração numa verificação pendente |
| Technical | `@dlt.expect*` não tem ação nativa de "quarantine para outra tabela" (só warn/drop/fail-pipeline) | Cada domínio com regra de quarantine precisa de 2 `@dlt.table` (limpo + quarantine), não 1 |
| Technical | `@dlt.expect*` avalia expressões linha-a-linha; `check: unique` é uma checagem cross-row/cross-batch via anti-join | `check: unique` precisa ser uma `@dlt.view` própria antes da tabela DLT, não uma `@dlt.expect` |
| Technical | ADR-04 (`cluster_by = merge_key`) é validado automaticamente por `test_contracts.py` | A tradução contrato→DLT precisa propagar `cluster_by` para o parâmetro nativo de clustering do `@dlt.table` |
| Architectural | ADR-03 já rejeitou Lakeflow/DLT explicitamente ("less explicit control") | Esta migração é uma decisão consciente de sobrescrever essa avaliação — precisa de um novo registro de decisão, não uma edição silenciosa do ADR-03 |
| Process | Mesma restrição de features anteriores: sem chamadas ao vivo contra um workspace Databricks real durante `/build` | Validação E2E completa (lineage real no Unity Catalog, `dlt.apply_changes` rodando de fato) fica para o usuário confirmar manualmente após o deploy |

---

## Technical Context

| Aspect | Value | Notes |
|--------|-------|-------|
| **Deployment Location** | `pipelines/bronze_silver_dlt.py` (novo) + `databricks.yml` (modificado, novo bloco `resources.pipelines`) | Primeiro uso de `resources.pipelines` neste projeto — até agora só `resources.jobs` |
| **KB Domains** | `databricks` (`.claude/kb/databricks.md`) — não existe ainda um domínio KB dedicado a Lakeflow/DLT | Considerar usar o agente `kb-architect` para criar um domínio `lakeflow` antes ou durante o `/build`, já que os agentes especializados (`lakeflow-architect`, `lakeflow-expert`, `lakeflow-pipeline-builder`, `lakeflow-specialist`) existem mas não têm uma KB própria para validar contra |
| **IaC Impact** | Novo recurso DABs: `resources.pipelines.ubereats_bronze_silver`. Job `ubereats_pipeline` (`dev`/`prod`) perde 30 tasks (`bronze_*`/`silver_*`) e ganha 1 task `pipeline_task` | `free_edition` não é afetado — continua com os 37 tasks originais via `task_definitions`/`serverless_tasks` |

---

## Data Contract

### Source Inventory

| Source | Type | Volume | Freshness | Owner |
|--------|------|--------|-----------|-------|
| `contracts/*.yml` (20 contratos com `layers: [bronze, silver]`) | YAML, já existente | 20 domínios, 129.353 registros no dataset de teste | Sem mudança de SLA — mesmo `trigger(availableNow=True)` em espírito, agora `continuous: false` no Lakeflow | Esta migração não altera nenhum contrato |

### Schema Contract

> Inalterado — `contracts/*.yml` continua sendo a fonte única de verdade para `schema`,
> `quality.rules`, `storage.cluster_by` e `table.merge_key`. Esta migração adiciona apenas um
> adaptador (contrato → `@dlt.table`/`@dlt.expect`/`dlt.apply_changes`), sem mudar o formato
> YAML em si.

### Freshness SLAs

| Layer | Target | Measurement |
|-------|--------|--------------|
| Bronze/Silver (Lakeflow, `dev`/`prod`) | Disparado (`triggered`), mesma cadência de hoje — sem SLA de tempo real | Execução do pipeline, não contínua |
| Bronze/Silver (`free_edition`, inalterado) | Mesma SLA já validada em v1.0.1/FREE_EDITION_BRONZE | Idêntica |

### Completeness Metrics

- 100% das regras de qualidade dos 20 contratos `[bronze, silver]` precisam ter uma tradução
  equivalente em DLT (`@dlt.expect_or_drop` ou par de `@dlt.table` para quarantine)
- Zero diferença na contagem de linhas `clean` vs. `quarantine` por domínio, comparando o
  pipeline Lakeflow contra os notebooks atuais, para o mesmo conjunto de dados de teste

### Lineage Requirements

- Cada domínio Bronze e Silver deve aparecer como nó de lineage individual no Unity Catalog
  — este é o requisito que motiva toda a feature

---

## Assumptions

| ID | Assumption | If Wrong, Impact | Validated? |
|----|------------|---------------------|------------|
| A-001 | `dlt.apply_changes()` aceita `sequence_by=col("__source_ts_ms")` para reproduzir exatamente a semântica de `WHEN MATCHED AND s.__source_ts_ms > t.__source_ts_ms THEN UPDATE` | Precisaria manter um `MERGE INTO` manual dentro de uma função `@dlt.table`, perdendo parte da simplificação prevista | [ ] |
| A-002 | `@dlt.table(..., cluster_by=[...])` é o parâmetro correto para Liquid Clustering em tabelas Lakeflow (paridade com `CLUSTER BY` do DDL atual) | Precisaria de um passo pós-criação (`ALTER TABLE ... CLUSTER BY`) fora do decorator | [ ] |
| A-003 | Lakeflow permite múltiplos `@dlt.table`/`@dlt.view` no mesmo arquivo Python gerados dinamicamente em loop (não precisam ser declarados estaticamente um a um no código-fonte) | Precisaria de 31 funções escritas à mão em vez de geradas por loop sobre `contracts/*.yml` — quebraria o objetivo DRY do Approach A | [ ] |
| A-004 | Um pipeline Lakeflow pode ser referenciado como dependência (`pipeline_task`) por um Job comum do DABs, permitindo que os 6 tasks `gold_*` + `silver_users` continuem no mesmo job `ubereats_pipeline` | Precisaria desacoplar Gold/`silver_users` para um job separado, com algum outro mecanismo de espera pela conclusão do pipeline | [ ] |
| A-005 | `databricks bundle validate` valida sintaticamente um bloco `resources.pipelines` sem precisar de execução real contra um workspace | Validação ficaria limitada a `yaml.safe_load`, sem confirmação de que o DABs aceita a estrutura proposta | [ ] |

**Note:** A-001 a A-004 são específicas da API do Lakeflow e não foram confirmadas contra
documentação oficial nesta sessão — recomenda-se validá-las explicitamente no início do
`/design`, antes de comprometer o file manifest a uma estrutura de código que dependa delas.

---

## Clarity Score Breakdown

| Element | Score (0-3) | Notes |
|---------|-------------|-------|
| Problem | 3 | Causa confirmada por auditoria de código anterior (sem bug encontrado), com explicação estrutural clara (lineage por caminho de notebook, não por execução) |
| Users | 3 | Dois personas com pain points distintos e não-conflitantes, herdados diretamente do brainstorm |
| Goals | 3 | MUST/SHOULD/COULD bem priorizados, todos derivados das 6 decisões registradas no brainstorm |
| Success | 3 | Critérios mensuráveis (contagem de nós de lineage, paridade registro-a-registro, idempotência) |
| Scope | 2 | Out of scope explícito e bem justificado, mas 5 das premissas técnicas (A-001 a A-005) ainda não foram validadas contra a API real do Lakeflow — a Goals/Success criteria dependem delas sem confirmação |
| **Total** | **14/15** | |

**Minimum to proceed: 12/15** — atendido

---

## Open Questions

- Nenhuma pergunta de negócio pendente — todas as decisões de escopo já foram validadas no
  brainstorm. As únicas pendências são técnicas (A-001 a A-005 acima), a serem resolvidas no
  início do `/design`, idealmente com uma consulta direta à documentação do Lakeflow antes de
  fixar o file manifest.

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-06-18 | define-agent | Initial version, extraído de BRAINSTORM_LAKEFLOW_MIGRATION.md |
| 1.1 | 2026-06-18 | ship-agent | Shipped and archived — ver BUILD_REPORT_LAKEFLOW_MIGRATION.md e SHIPPED_2026-06-18.md |

---

## Next Step

**Ready for:** `/design .claude/sdd/features/DEFINE_LAKEFLOW_MIGRATION.md`
