# BRAINSTORM: Data Contracts — 20 domínios YAML

> Exploratory session — Agent 4 do 04_build.delegation.md

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | DATA_CONTRACTS |
| **Date** | 2026-06-16 |
| **Author** | brainstorm-agent |
| **Status** | Ready for Define |

---

## Initial Idea

**Raw Input:** Construir os 20 contratos YAML de dados para sdd-kafka-databricks. Domínios no CLAUDE.md. Agent 4 do 04_build.delegation.md.

**Context Gathered:**
- 20 domínios com schemas reais extraídos de `tests/data/*.json` (1 record por domínio)
- ADR-04: cluster_by MUST equal merge_key (validado por test_contracts.py)
- SMT ExtractNewRecordState ativo (Agent 3 correction) — Bronze recebe registros flat com `__op` e `__source_ts_ms`
- Quarantine mirrors apenas os 12 domínios Silver (não bronze-only)
- `payment_current_state` na silver_list é view derivada de payment_events — sem contrato próprio

**Technical Context Observed:**

| Aspect | Observation | Implication |
|--------|-------------|-------------|
| Likely Location | `contracts/{table}.yml` | Diretório já existe |
| Relevant KB Domains | data-quality, spark | Regras de qualidade + StructType |
| Special Cases | JSONB (2 tabelas), sem dt_current_timestamp (3 tabelas), rating_id INTEGER | Tratamento por tabela |

---

## Discovery Questions & Answers

| # | Question | Answer | Impact |
|---|----------|--------|--------|
| 1 | Abordagem de validação antes de gerar todos os 20? | 3 contratos representativos (1 JSONB, 1 hub entity, 1 bronze-only) | Valida formato antes de escalar |
| 2 | JSONB fields (payment_events.event, order_status.status) — tipo no Bronze? | `string` — Bronze armazena raw JSONB, Silver faz o parse | `event` e `status` como `type: string` |
| 3 | `payment_current_state` na silver_list — o que é? | View Silver derivada de payment_events (latest event_name por payment_id) | Sem contrato próprio — 20 contratos para 20 domínios com source data |

---

## Sample Data Inventory

| Type | Location | Count | Notes |
|------|----------|-------|-------|
| Input files | `tests/data/*.json` | 100 | JSON por linha, 1 linha = 1 record |
| Schemas inferidos | `tests/data/*.json` | 20 domínios | 1 record de amostra por domínio |
| ADR-04 alignment table | `.claude/03_design.md` | 6 exemplos | Expandido para todos os 20 |

---

## Approaches Explored

### Approach A: 3 representativos → aprovação → 17 restantes ⭐ Escolhido

**Pros:** Valida formato cedo; detecta problemas antes de escalar; user aprova o padrão.
**Cons:** Um passo adicional antes do lote completo.

### Approach B: Todos os 20 de uma vez

**Por que não escolhido:** Sem validação de formato; erros de estrutura se repetem nos 20.

### Approach C: Script Python de auto-geração

**Por que não escolhido (YAGNI):** Schemas já mapeados pelos JSON reais; quality rules exigem julgamento de negócio que um script não faz.

---

## Key Decisions Made

| # | Decision | Rationale | Alternative Rejected |
|---|----------|-----------|----------------------|
| 1 | `layers: [bronze, silver]` em vez de `layer: silver` | Reflete que o contrato é usado em ambas as camadas | `layer: silver` (singular) |
| 2 | JSONB como `type: string` no Bronze | Bronze preserva raw; Silver faz parse via PARSE_JSON no notebook | Campos explodidos em Bronze |
| 3 | `on_failure: reject` apenas no PK em Bronze | Sem quarantine para bronze-only; outras violações são `warn` | Reject em todos os campos FK |
| 4 | `delta.enableChangeDataFeed: false` para bronze-only | CDF desnecessário sem Silver lendo via CDF | CDF habilitado universalmente |
| 5 | `allowed_values` apenas onde há certeza (ratings.rating, payments.status, etc.) | Evitar valores inventados que quebrem test_contracts.py | Sem allowed_values |
| 6 | `scope: [bronze, silver]` como lista (não string) | Formato esperado por loader.py e test_contracts.py | `scope: bronze` (string) |
| 7 | Delta properties como booleans Python (sem aspas) | Formato esperado por spark_schema.py | `"true"` (string) |
| 8 | `_ingested_date` fora do schema | Campo derivado gerado pelo notebook — não pertence ao contrato | Incluir no schema |

---

## Features Removed (YAGNI)

| Feature Sugerida | Razão Removida | Pode Voltar? |
|------------------|----------------|--------------|
| Contrato para `payment_current_state` | View Silver derivada, sem Kafka topic/source próprio | Sim, como 21º contrato separado |
| Script Python de auto-geração de contratos | YAGNI — sample data + ADR-04 já definem tudo | Sim |
| `schema_evolution` por campo | Regras idênticas para todos — defaults universais suficientes | Sim |

---

## Data Engineering Context

### Casos especiais por domínio

| Domínio | Caso Especial | Tratamento no Contrato |
|---------|---------------|------------------------|
| `payment_events` | `event` é JSONB | `type: string, nullable: true` |
| `order_status` | `status` é JSONB; PK é INTEGER | `type: string`; `merge_key: status_id (integer)` |
| `receipts` | Sem `dt_current_timestamp` | `not_future` em `receipt_generated_at` |
| `search_events` | Sem `dt_current_timestamp` | `not_future` em `timestamp` |
| `inventory` | Sem `dt_current_timestamp` | `not_future` em `last_updated` |
| `ratings` | `rating_id` é INTEGER (não UUID) | `type: integer`; `merge_key: uuid` |
| `order_items` | 85% do volume (110k records) | `maxOffsetsPerTrigger: 5000` no DABs (não no contrato) |
| `users_mongo` + `users_mssql` | CPF como chave de merge Silver | `cpf: quarantine, [silver]` em ambos |

### ADR-04 Alignment — cluster_by = merge_key (todos os 20)

| Domínio | merge_key | cluster_by | ADR-04 ✓ |
|---------|-----------|------------|----------|
| payment_events | event_id | [event_id] | ✓ |
| gps_events | gps_id | [gps_id] | ✓ |
| orders | order_id | [order_id] | ✓ |
| payments | payment_id | [payment_id] | ✓ |
| order_status | status_id | [status_id] | ✓ |
| order_items | order_item_id | [order_item_id] | ✓ |
| search_events | search_id | [search_id] | ✓ |
| recommendations | event_id | [event_id] | ✓ |
| driver_shifts | shift_id | [shift_id] | ✓ |
| routes | route_id | [route_id] | ✓ |
| receipts | receipt_id | [receipt_id] | ✓ |
| support_tickets | ticket_id | [ticket_id] | ✓ |
| users_mongo | uuid | [uuid] | ✓ |
| users_mssql | uuid | [uuid] | ✓ |
| restaurants | uuid | [uuid] | ✓ |
| drivers | uuid | [uuid] | ✓ |
| products | product_id | [product_id] | ✓ |
| menu_sections | menu_section_id | [menu_section_id] | ✓ |
| ratings | uuid | [uuid] | ✓ |
| inventory | stock_id | [stock_id] | ✓ |

---

## Selected Approach

| Attribute | Value |
|-----------|-------|
| **Chosen** | Approach A |
| **User Confirmation** | 2026-06-16 |
| **Reasoning** | 3 representativos validados antes do lote completo; ajuste de formato aplicado (layers, scope como lista, booleans sem aspas, sem _ingested_date) |

---

## Incremental Validations

| Section | Presented | User Feedback | Adjusted? |
|---------|-----------|---------------|-----------|
| Formato YAML completo (payment_events draft) | ✅ | Ajustar 3 pontos (layers, scope, _ingested_date) | Sim |
| 3 contratos representativos gerados | ✅ | Aprovado com layers corrigido | Sim — layer → layers antes do lote |

---

## Suggested Requirements for /define

### Problem Statement
Gerar 20 contratos YAML válidos — um por domínio — que sirvam de fonte de verdade para StructType, TBLPROPERTIES, Pydantic models e quality rules do pipeline sdd-kafka-databricks.

### Success Criteria
- [ ] 20 arquivos `contracts/{table}.yml` existem e são YAML sintaticamente válidos
- [ ] Todos os quality.rules referenciam campos existentes no schema
- [ ] `cluster_by` é subset dos campos do schema
- [ ] `merge_key` está em `cluster_by` (ADR-04)
- [ ] `allowed_values` são listas não-vazias onde presentes
- [ ] `_ingested_date` ausente de todos os schemas (campo derivado)
- [ ] `layers: [bronze, silver]` para Silver domains; `layers: [bronze]` para bronze-only
- [ ] `delta.enableChangeDataFeed: false` para bronze-only
- [ ] test_contracts.py passa sem erros

### Constraints Identified
- JSONB fields (`event`, `status`) devem ser `type: string` no contrato Bronze
- bronze-only domains (8) não têm quarantine — `on_failure` não pode ser `quarantine` em scope [bronze]
- `payment_current_state` é view derivada — sem contrato próprio nesta iteração

### Out of Scope
- Contrato para `payment_current_state` (view Silver derivada)
- Script de auto-geração de contratos
- `schema_evolution` por campo (defaults universais suficientes)

---

## Session Summary

| Metric | Value |
|--------|-------|
| Questions Asked | 3 |
| Approaches Explored | 3 |
| Features Removed (YAGNI) | 3 |
| Validations Completed | 2 |
| Contratos gerados | 20 |

---

## Artifacts Produced

| Artifact | Status |
|----------|--------|
| `contracts/payment_events.yml` | ✅ |
| `contracts/gps_events.yml` | ✅ |
| `contracts/orders.yml` | ✅ |
| `contracts/payments.yml` | ✅ |
| `contracts/users_mongo.yml` | ✅ |
| `contracts/users_mssql.yml` | ✅ |
| `contracts/drivers.yml` | ✅ |
| `contracts/order_items.yml` | ✅ |
| `contracts/driver_shifts.yml` | ✅ |
| `contracts/restaurants.yml` | ✅ |
| `contracts/order_status.yml` | ✅ |
| `contracts/search_events.yml` | ✅ |
| `contracts/recommendations.yml` | ✅ |
| `contracts/routes.yml` | ✅ |
| `contracts/receipts.yml` | ✅ |
| `contracts/support_tickets.yml` | ✅ |
| `contracts/products.yml` | ✅ |
| `contracts/menu_sections.yml` | ✅ |
| `contracts/ratings.yml` | ✅ |
| `contracts/inventory.yml` | ✅ |

---

## Next Step

**Próximo passo do Agent 4:** `/build` — implementar `contracts/loader.py`, `contracts/spark_schema.py`, `contracts/pydantic_models.py` e `tests/test_contracts.py`
