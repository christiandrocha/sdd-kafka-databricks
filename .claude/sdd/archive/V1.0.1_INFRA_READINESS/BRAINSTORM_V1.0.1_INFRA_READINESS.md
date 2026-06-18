# BRAINSTORM: v1.0.1 Infra Readiness — gaps reais antes do primeiro `make up`

> Exploratory session to clarify intent and approach before requirements capture

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | V1.0.1_INFRA_READINESS |
| **Date** | 2026-06-17 |
| **Author** | brainstorm-agent |
| **Status** | ✅ Shipped (2026-06-17) |

---

## Initial Idea

**Raw Input:** "v1.0.1 — resolver os 3 TDs de alta prioridade antes do primeiro make up: TD-01 (docker-compose.yml JMX wiring), TD-06 (scripts/init.sql e register_connectors.sh), TD-07 (Volume paths Unity Catalog). Contexto: .claude/06_retrospective.md" — refinado depois para "2 gaps reais" quando a investigação mostrou que o item de JMX já estava implementado.

**Context Gathered:**
- Os rótulos de TD usados no pedido (TD-01/TD-06/TD-07) **não correspondem** à numeração atual de `.claude/06_retrospective.md` (que tem TD-01 a TD-07, com TD-05 já resolvido na sessão anterior). Mapeamento real:
  | Rótulo do usuário | TD real no arquivo | Estado real no código |
  |---|---|---|
  | TD-01 (JMX) | TD-06 | ✅ já implementado |
  | TD-06 (init.sql/connectors) | sem TD dedicado — gap real | ⚠️ confirmado |
  | TD-07 (Volumes UC) | TD-04 | ⚠️ confirmado, escopo maior que o relatado |
- `docker-compose.yml` já tem os sidecars `jmx-kafka` (porta 9101) e `jmx-kafka-connect` (porta 9404) corretamente wireados (`KAFKA_JMX_PORT=9999/9998` + bitnami/jmx-exporter). `05_implementation_log.md` já registra isso como resolvido (sessão "Agent 1 — infra-base"). O texto do TD-06 em `06_retrospective.md` está desatualizado (ainda marca como pendente).
- `sql/init.sql` tem apenas 14 linhas: `CREATE EXTENSION pgcrypto`, replication slot e `CREATE PUBLICATION debezium_publication FOR ALL TABLES`. **Nenhuma das 20 tabelas é criada.** `tests/load_to_postgres.py` faz `INSERT INTO {table}` assumindo que as tabelas já existem — `make produce-initial` falharia com "relation does not exist".
- Não existe `scripts/` nem `connectors/` no projeto. `connectors/debezium.json` e `scripts/register_connectors.sh` nunca foram criados — `make up` sobe os containers mas o conector CDC nunca é registrado.
- O projeto irmão `/home/christian/Documents/sdd-kafka-snowflake` tem `scripts/init.sql` (468 linhas, 20 `CREATE TABLE`), `connectors/debezium.json` e `scripts/register_connectors.sh` prontos, com os mesmos 20 domínios e nomes de tabela. Verificação campo-a-campo contra os 19 contratos YAML deste projeto (tipos, nullability, casos especiais como `order_status.status_id INTEGER`, `receipts`/`search_events`/`inventory` sem `dt_current_timestamp`) — alinhamento total, reuso direto viável.
- **Achado crítico:** o `debezium.json` de referência usa a SMT `ExtractNewRecordState`. A ADR-02 *escrita* em `CLAUDE.md`/`03_design.md`/`02_define.md` diz "Bronze = envelope raw, SMT NÃO é usada, unwrap no Silver". Mas o código real (`pipeline_bronze.ipynb` comenta literalmente *"Post-SMT Avro schema... (ADR-002)"`; `pipeline_silver.ipynb` não tem nenhuma lógica de unwrap; os contratos têm schema flat com `__op`/`__source_ts_ms`) implementa exatamente o oposto: **SMT é usada**. A origem da divergência: `06_retrospective.md` (seção "v1.0.0 — Design complete") narra que a decisão "sem SMT" foi identificada como regressão e corrigida para "usa SMT" — mas o bloco formal da ADR-02 nunca foi atualizado para refletir essa correção.
- Nada no repositório cria o Catalog (`ubereats_dev`/`ubereats_prod`), os 4 Schemas (`bronze`/`silver`/`gold`/`quarantine`) ou os Volumes de checkpoint. Os notebooks só fazem `CREATE TABLE IF NOT EXISTS catalog.schema.table`, que falha sem catalog/schema existentes — o gap é maior que "só Volumes".
- `databricks.yml` `resources:` só tem `jobs:` — sem `volumes:`/`schemas:` declarativos. Mecanismo escolhido: script imperativo via Databricks CLI, não bundle resource.
- `03_design.md` `file_manifest.infrastructure` lista `scripts/init.sql` (path errado — o real é `sql/init.sql`, montado em `docker-compose.yml`) e `scripts/set_compatibility.sh` (nunca criado — será dispensado via YAGNI, dobrado dentro de `register_connectors.sh`, como no projeto de referência).

**Technical Context Observed (for Define):**

| Aspect | Observation | Implication |
|--------|-------------|--------------|
| Likely Location | `sql/init.sql`, `connectors/debezium.json`, `scripts/register_connectors.sh`, `scripts/preflight_unity_catalog.sh` | Paths confirmados contra docker-compose.yml e file_manifest |
| Relevant KB Domains | data-engineering (CDC/Debezium), spark (contracts↔DDL) | Padrões de schema e SMT |
| IaC Patterns | Reuso adaptado de `sdd-kafka-snowflake` (mesmo domain set) | Reduz risco de erro de schema |

---

## Discovery Questions & Answers

| # | Question | Answer | Impact |
|---|----------|--------|--------|
| 1 | A ADR-02 escrita diz "sem SMT" mas o código usa SMT — corrigir a doc junto? | Sim, corrigir ADR-02 (CLAUDE.md + 03_design.md + 02_define.md) | Doc passa a refletir a arquitetura real antes de criar o novo debezium.json |
| 2 | Gap 2 deve cobrir só Volumes, ou também Catalog + Schemas (nada cria nenhum dos três hoje)? | Catalog + Schemas + Volumes juntos | Pre-flight cobre as 3 dependências reais do Unity Catalog |
| 3 | Mecanismo do pre-flight (script bash vs notebook)? | Script bash com Databricks CLI | Roda uma vez fora do ciclo de notebooks parametrizados |
| 4 | Manter `debezium_publication FOR ALL TABLES` ou trocar para lista explícita como a referência? | Manter `FOR ALL TABLES` (nome já em uso) | Não precisa editar a publication se um domínio futuro for adicionado |
| 5 | register_connectors.sh deve registrar só o Debezium connector (sem sinks)? | Sim, só `debezium-postgres-cdc` | Script reduzido — Databricks lê Kafka direto via Structured Streaming (ADR-01) |

**Minimum Questions:** 3 ✅ (5 perguntas feitas)

---

## Sample Data Inventory

| Type | Location | Count | Notes |
|------|----------|-------|-------|
| Reference init.sql | `sdd-kafka-snowflake/scripts/init.sql` | 20 tabelas | Verificado campo-a-campo contra os 19 contratos YAML — tipos e casos especiais alinhados |
| Reference debezium.json | `sdd-kafka-snowflake/connectors/debezium.json` | 1 connector | Precisa manter `transforms.unwrap` (SMT) — já confirmado consistente com o código real |
| Reference register_connectors.sh | `sdd-kafka-snowflake/scripts/register_connectors.sh` | 3 connectors no original | Reduzir para 1 (sem sinks Snowflake) |
| Contratos deste projeto | `contracts/*.yml` | 19 arquivos | Fonte de verdade para nomes/tipos de coluna — usados para validar o init.sql adaptado |
| `.env.example` | raiz do projeto | — | Já tem `POSTGRES_USER`/`PASSWORD`/`DB`, `DATABRICKS_HOST`/`TOKEN` — reusável pelo register_connectors.sh sem mudanças |

**How samples will be used:**
- `sql/init.sql` completo será adaptado linha-a-linha do reference, com nomes de coluna e tipos cruzados contra os contratos YAML (já verificado nesta sessão para os casos especiais: `order_status`, `search_events`, `receipts`, `inventory`).
- `connectors/debezium.json` adaptado removendo apenas o que não se aplica (nada — a SMT é mantida), ajustando só `topic.prefix`/`table.include.list` se necessário (já idênticos).
- `scripts/register_connectors.sh` adaptado removendo o registro dos 2 Sink connectors e mantendo a lógica de espera + compatibilidade BACKWARD inline.

---

## Approaches Explored

### Approach A: Reuso adaptado de `sdd-kafka-snowflake` + correção de ADR-02 ⭐ Recommended

**Description:** Adaptar os 3 arquivos de infraestrutura (`init.sql`, `debezium.json`, `register_connectors.sh`) do projeto de referência, mantendo a SMT (alinhada ao código real), reduzindo o connector script para 1 conector, e corrigir a ADR-02 escrita para bater com a implementação. Criar `scripts/preflight_unity_catalog.sh` novo (sem referência direta, já que `sdd-kafka-snowflake` não usa Databricks/Unity Catalog) cobrindo Catalog + Schemas + Volumes via Databricks CLI.

**Pros:**
- Reuso de schema já validado em produção (mesmo domain set, 100% dos tipos confirmados contra os contratos)
- Resolve a contradição de documentação (ADR-02) antes que ela contamine o próximo notebook ou contrato
- Pre-flight cobre a causa raiz completa (catalog/schema/volume), não só o sintoma relatado (volume)

**Cons:**
- Toca 5 documentos de design (CLAUDE.md, 02_define.md, 03_design.md, 06_retrospective.md, 05_implementation_log.md) além dos arquivos de infra — mudança maior que "só 2 arquivos novos"
- `scripts/preflight_unity_catalog.sh` não tem equivalente para copiar — precisa ser escrito do zero no /build

**Why Recommended:** É a única abordagem que deixa o `make up` → `make produce-initial` → `databricks bundle deploy` funcionando ponta a ponta sem mais surpresas de doc/código divergentes — exatamente o objetivo declarado ("antes do primeiro make up").

---

### Approach B: Copiar os 3 arquivos de referência sem alterar ADR-02

**Description:** Criar `init.sql`/`debezium.json`/`register_connectors.sh` mantendo a SMT (porque o código real precisa), mas sem tocar na documentação da ADR-02 — tratando a contradição textual como um problema separado.

**Pros:**
- Menor blast radius nesta iteração — só 3 arquivos novos + o pre-flight

**Cons:**
- Cria um `debezium.json` que contradiz a ADR-02 escrita no próprio `CLAUDE.md` no mesmo commit — exatamente o tipo de divergência que `CLAUDE.md` instrui a sinalizar para `/design`
- Próxima sessão (humana ou agente) que ler a ADR-02 vai presumir "sem SMT" e pode tentar adicionar lógica de unwrap no Silver que já não é necessária

**Why not recommended:** Resolve o gap funcional mas deixa uma armadilha documental ativa — descartado pelo usuário na validação desta sessão.

---

## Data Engineering Context

### Source Systems
| Source | Type | Volume Estimate | Current Freshness |
|--------|------|-----------------|--------------------|
| PostgreSQL (20 tabelas, WAL logical) | CDC source | 129,353 registros / 100 arquivos JSON | Batch via `load_to_postgres.py`, capturado em near-real-time pelo Debezium |

### Data Flow Sketch
```text
tests/data/*.json → load_to_postgres.py → PostgreSQL (sql/init.sql cria as 20 tabelas)
  → Debezium (connectors/debezium.json, SMT ExtractNewRecordState)
  → Kafka (20 tópicos pg.public.*)
  → Databricks Structured Streaming (pipeline_bronze.ipynb, espera schema pós-SMT)
  → Unity Catalog (catalog/schemas/volumes criados por scripts/preflight_unity_catalog.sh)
```

### Key Data Questions Explored
| # | Question | Answer | Impact |
|---|----------|--------|--------|
| 1 | O `init.sql` adaptado cobre os 20 domínios com schema correto? | Sim — verificado campo-a-campo contra os 19 contratos YAML nesta sessão | Reduz risco de retrabalho no /build |
| 2 | Quantos Kafka Connectors são necessários? | 1 (`debezium-postgres-cdc`) | Databricks lê Kafka direto, sem Sink Connector |
| 3 | O pre-flight do Unity Catalog precisa rodar antes de cada deploy ou só uma vez? | Uma vez, antes do primeiro `databricks bundle deploy` por target (dev/prod) | Script idempotente, não entra no ciclo do DABs job |

---

## Selected Approach

| Attribute | Value |
|-----------|-------|
| **Chosen** | Approach A |
| **User Confirmation** | 2026-06-17, validado em 2 checkpoints incrementais |
| **Reasoning** | Resolve os 2 gaps reais + a contradição de documentação da ADR-02 que o Gap 1 expõe, deixando o caminho `make up` → primeiro `bundle deploy` livre de surpresas |

---

## Key Decisions Made

| # | Decision | Rationale | Alternative Rejected |
|---|----------|-----------|----------------------|
| 1 | Manter a SMT `ExtractNewRecordState` no novo `debezium.json` | Código real (`pipeline_bronze.ipynb`, `pipeline_silver.ipynb`, contratos) já assume dados pós-SMT | Envelope raw sem SMT (texto atual da ADR-02, nunca implementado) |
| 2 | Corrigir o texto da ADR-02 em CLAUDE.md/03_design.md/02_define.md | Documentação deve refletir o código, não a versão pré-correção da decisão | Deixar a contradição para um brainstorm futuro |
| 3 | `register_connectors.sh` registra só 1 connector | ADR-01: Databricks lê Kafka direto via Structured Streaming, sem Sink Connector | Copiar os 3 connectors do projeto de referência (2 seriam mortos) |
| 4 | Manter `debezium_publication FOR ALL TABLES` | Já é o nome em uso no `init.sql` parcial existente; evita editar lista a cada novo domínio | `dbz_publication FOR TABLE <lista explícita>` (padrão da referência) |
| 5 | Pre-flight cobre Catalog + Schemas + Volumes, não só Volumes | Nenhum dos três existe hoje; só Volumes deixaria o primeiro `CREATE TABLE` falhar mesmo assim | Escopo restrito só a Volumes (conforme pedido original) |
| 6 | Pre-flight é script bash com Databricks CLI, não notebook nem bundle resource | Simplicidade, idempotência fácil de checar via CLI antes de criar; `databricks.yml` não usa `resources.volumes` hoje | Notebook `.ipynb` dedicado / declarar como DABs resource |

---

## Features Removed (YAGNI)

| Feature Suggested | Reason Removed | Can Add Later? |
|-------------------|-----------------|-----------------|
| `scripts/set_compatibility.sh` separado (listado no `file_manifest` de `03_design.md`) | A única ação (PUT BACKWARD compatibility) cabe em 4 linhas dentro de `register_connectors.sh`, como no projeto de referência — arquivo dedicado seria over-engineering | Sim |
| 2 Sink Connectors (Snowflake) no `register_connectors.sh` | Databricks lê Kafka direto via Structured Streaming (ADR-01) — não há destino Kafka Connect Sink neste projeto | Não (incompatível com a arquitetura) |
| `dbz_publication FOR TABLE <lista explícita>` | `FOR ALL TABLES` já está em uso e é mais simples de manter | Sim, se um motivo concreto de segurança/produção aparecer |
| Validação end-to-end do JMX (Prometheus realmente raspando) | Fora de escopo deste brainstorm — é teste de infraestrutura, não gap de implementação | Sim, como item futuro de observability |

---

## Incremental Validations

| Section | Presented | User Feedback | Adjusted? |
|---------|-----------|----------------|-----------|
| Seção 1 — Gap 1 (init.sql + connectors + ADR-02) | ✅ | "De acordo, seguir" | Não |
| Seção 2 — Gap 2 (pre-flight UC) + fechamento TD-01/TD-04 | ✅ | "De acordo, escrever o BRAINSTORM" | Não |

**Minimum Validations:** 2 ✅

---

## Suggested Requirements for /define

### Problem Statement (Draft)
Antes do primeiro `make up` + `databricks bundle deploy`, o pipeline tem 2 gaps de infraestrutura que impedem qualquer execução ponta a ponta: PostgreSQL nunca recebe o schema das 20 tabelas e o Debezium nunca é registrado; e o Unity Catalog (catalog/schemas/volumes) nunca é provisionado. Além disso, a ADR-02 documentada contradiz o comportamento real do código (SMT).

### Target Users (Draft)
| User | Pain Point |
|------|------------|
| Engenheiro de dados rodando o stack localmente | `make up` sobe containers mas `make produce-initial` falha (tabelas não existem) e o Kafka nunca recebe CDC (connector não registrado) |
| Quem for fazer o primeiro `databricks bundle deploy` | Primeira `CREATE TABLE` falha por catalog/schema/volume inexistentes |
| Próximo agente/revisor que ler a ADR-02 | Doc diz "sem SMT" mas o código usa SMT — risco de retrabalho incorreto |

### Success Criteria (Draft)
- [ ] `sql/init.sql` cria as 20 tabelas com schema validado contra os 19 contratos YAML
- [ ] `connectors/debezium.json` registra com sucesso via `scripts/register_connectors.sh` (1 connector, SMT ativa)
- [ ] `scripts/preflight_unity_catalog.sh` cria catalog + 4 schemas + volume para dev e prod, idempotente
- [ ] ADR-02 (CLAUDE.md, 03_design.md, 02_define.md) reflete "usa SMT" com a justificativa real
- [ ] TD-01(JMX)/TD-06 e TD-04 fechados em `06_retrospective.md` apontando para os artefatos corretos

### Constraints Identified
- Manter `debezium_publication FOR ALL TABLES` (não migrar para lista explícita)
- `register_connectors.sh` registra só `debezium-postgres-cdc` (sem sinks)
- Pre-flight é script imperativo via Databricks CLI, não bundle resource declarativo

### Out of Scope (Confirmed)
- Validação end-to-end do JMX/Prometheus (TD-01 do usuário fica só como correção documental)
- `scripts/set_compatibility.sh` como arquivo separado
- Qualquer Sink Connector

---

## Session Summary

| Metric | Value |
|--------|-------|
| Questions Asked | 5 |
| Approaches Explored | 2 |
| Features Removed (YAGNI) | 4 |
| Validations Completed | 2 |
| Duration | ~1 sessão |

---

## Next Step

**Ready for:** `/define .claude/sdd/features/BRAINSTORM_V1.0.1_INFRA_READINESS.md`
