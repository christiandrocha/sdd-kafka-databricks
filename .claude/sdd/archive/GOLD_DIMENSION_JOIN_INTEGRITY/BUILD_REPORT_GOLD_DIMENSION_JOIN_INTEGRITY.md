# BUILD REPORT: Gold Dimension Join Integrity — unicidade forçada no Silver + guard no Gold

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | GOLD_DIMENSION_JOIN_INTEGRITY |
| **Date** | 2026-06-18 |
| **Author** | build-agent |
| **DEFINE** | Nenhum (ver Scope Note do DESIGN) |
| **DESIGN** | [DESIGN_GOLD_DIMENSION_JOIN_INTEGRITY.md](../features/DESIGN_GOLD_DIMENSION_JOIN_INTEGRITY.md) |
| **Status** | Complete |

---

## Summary

| Metric | Value |
|--------|-------|
| **Tasks Completed** | 10/10 |
| **Files in DESIGN manifest** | 10/10 created/modified |
| **Files added mid-build (não previstos no manifest)** | 0 |
| **Desvios do code pattern do DESIGN (aprovados implicitamente, justificados abaixo)** | 1 (`pipeline_users.ipynb` — checagem simplificada por ser full-refresh, não incremental) |
| **Contract tests** | 163 passed (141 → 163; +22 com os novos testes e a regra `unique`) |

---

## Task Execution

| # | Task | Agent | Status | Notes |
|---|------|-------|--------|-------|
| 1 | `contracts/loader.py` — `"unique"` em `VALID_CHECKS` | (direct) | ✅ Complete | 1 linha |
| 2 | `contracts/drivers.yml` — regra `unique` em `driver_id` | (direct) | ✅ Complete | — |
| 3 | `contracts/restaurants.yml` — regra `unique` em `cnpj` | (direct) | ✅ Complete | — |
| 4 | `pipeline_silver.ipynb` — checagem `unique` via anti-join | (direct) | ✅ Complete | `apply_quality_rules` ganhou parâmetro `silver_table`; call site atualizado |
| 5 | `pipeline_users.ipynb` — quarantine de `user_id` duplicado | (direct) | ✅ Complete | Implementação adaptada — ver Deviations |
| 6 | `gold_driver_performance.ipynb` — guard `row_number()` | (direct) | ✅ Complete | Mesmo padrão de `gold_user_behavior` |
| 7 | `gold_revenue_per_restaurant.ipynb` — guard `row_number()` | (direct) | ✅ Complete | Aplicado sobre o `revenue_df` já corrigido (viés de média, sessão anterior) |
| 8 | `docs/adr/005_gold_dimension_join_integrity.md` | (direct) | ✅ Complete | — |
| 9 | `CLAUDE.md` — documenta `check: unique` + guard duplo | (direct) | ✅ Complete | — |
| 10 | `tests/test_contracts.py` — testes da nova regra | (direct) | ✅ Complete | 3 testes novos (`test_08`–`test_10`) |

**Legend:** ✅ Complete | 🔄 In Progress | ⏳ Pending | ❌ Blocked

Nenhum agente especializado foi de fato invocado via `Task`/`Agent` — todas as edições foram
suficientemente diretas (YAML, 1 linha em Python, edições de notebook bem delimitadas) para
serem feitas diretamente, mesmo onde o DESIGN sugeria `@data-quality-analyst`/`@spark-engineer`.

---

## Files Created / Modified

| File | Action | Verified |
|------|--------|----------|
| `contracts/loader.py` | Modified | ✅ `pytest` passa, `"unique"` aceito |
| `contracts/drivers.yml` | Modified | ✅ `load_contract()` sem erro, `test_09` passa |
| `contracts/restaurants.yml` | Modified | ✅ `load_contract()` sem erro, `test_09` passa |
| `notebooks/pipeline_silver.ipynb` | Modified | ✅ JSON válido |
| `notebooks/pipeline_users.ipynb` | Modified | ✅ JSON válido |
| `notebooks/cross_domain/gold_driver_performance.ipynb` | Modified | ✅ JSON válido |
| `notebooks/cross_domain/gold_revenue_per_restaurant.ipynb` | Modified | ✅ JSON válido |
| `docs/adr/005_gold_dimension_join_integrity.md` | Created | ✅ revisado |
| `CLAUDE.md` | Modified | ✅ revisado |
| `tests/test_contracts.py` | Modified | ✅ 163/163 passam |

---

## Verification Results

### Lint / Syntax

```text
ruff check .                                                → All checks passed!
python3 -c "import json; ...pipeline_silver.ipynb"          → valid JSON
python3 -c "import json; ...pipeline_users.ipynb"            → valid JSON
python3 -c "import json; ...gold_driver_performance.ipynb"   → valid JSON
python3 -c "import json; ...gold_revenue_per_restaurant.ipynb" → valid JSON
```

**Status:** ✅ Pass

`make lint` também roda `yamllint contracts/`, que falhou com `yamllint: No such file or
directory` — `yamllint` não está instalado neste ambiente. Pré-existente, não relacionado a
esta mudança (não há como ter regredido algo que nunca rodou nesta sessão). Os 2 contratos
modificados (`drivers.yml`, `restaurants.yml`) foram validados via `load_contract()` (que faz
`yaml.safe_load` internamente) dentro de `pytest` — equivalente em cobertura de sintaxe.

### Contract Tests

```text
python3 -m pytest tests/test_contracts.py -q   → 163 passed
```

**Status:** ✅ Pass (141 → 163: +20 do crescimento normal de `test_09` parametrizado por
contrato, +2 dos testes não-parametrizados `test_08`/`test_10`; nenhuma regressão nos 7 testes
já existentes)

### Não executável neste ambiente (mesma limitação de features anteriores)

| Test | Motivo |
|------|--------|
| `apply_quality_rules` com `check: unique` contra um Spark real (anti-join, `_unique_violation_values`) | Requer runtime Spark/Databricks — não disponível no `/build`. Lógica revisada por inspeção: `existing`/`incoming` corretamente escopados a `(field, merge_key)`, `cross_batch` via join+filter de desigualdade, `in_batch` via `groupBy().count()` — sem dependência de estado externo além do `silver_table` já lido pela própria função |
| `row_number()` guard nos 2 Gold modificados, contra dado real | Mesma limitação — revisado por inspeção, mesmo padrão já em produção em `gold_user_behavior` |
| `pipeline_users.ipynb` — quarantine de `user_id` duplicado, contra dado real | Mesma limitação |

---

## Deviations from Design

| Deviation | Reason | Impact |
|-----------|--------|--------|
| `pipeline_users.ipynb`: a checagem de unicidade implementada é **só dentro do batch atual** (`groupBy("user_id").count() > 1`), não um anti-join contra uma tabela "existente" como o Pattern 4 do DESIGN sugeria | Ao reler `cell-write` durante o build, confirmei que `silver.users` faz **full refresh** (`mode("overwrite")`) a cada execução, não `MERGE` incremental como o restante do Silver. Não existe um "estado anterior" significativo para fazer anti-join contra — cada execução recomputa tudo do zero a partir do Bronze atual. O Pattern 4 do DESIGN foi escrito por analogia ao padrão incremental de `pipeline_silver.ipynb`, mas essa analogia não se aplica aqui; a versão simplificada (checagem só intra-batch) é a correta para este notebook especificamente | Nenhum — o resultado é o mesmo objetivo (linhas com `user_id` duplicado nunca chegam a `silver.users`), só que com menos código do que o sketch original previa. O DESIGN já antecipava essa possibilidade ("nota para o Build: validar/ajustar") |

Nenhum arquivo fora do manifesto de 10 foi tocado.

---

## Acceptance Verification (mapeado às Decisions do DESIGN)

| Decision | Verificação | Status |
|----------|-------------|--------|
| Decision 1 — `merge_key` não foi alterado em nenhum contrato | `git diff contracts/drivers.yml contracts/restaurants.yml` — só a linha de regra `unique` foi adicionada; `table.merge_key` e `storage.cluster_by` inalterados | ✅ |
| Decision 2 — `check: unique` declarado e implementado, com quarantine | `contracts/drivers.yml`/`restaurants.yml` têm a regra; `pipeline_silver.ipynb` implementa via anti-join; `pipeline_users.ipynb` tem o equivalente manual; ambos roteiam para `quarantine_table`, nunca descartam silenciosamente | ✅ |
| Decision 3 — guard `row_number()` em todos os 3 Gold afetados | `gold_user_behavior` (já existia), `gold_driver_performance` (adicionado), `gold_revenue_per_restaurant` (adicionado) — todos imediatamente antes do `MERGE` | ✅ |

---

## Final Status

### Overall: ✅ COMPLETE

**Completion Checklist:**

- [x] Todos os 10 arquivos do manifesto do DESIGN criados/modificados
- [x] `ruff check .` limpo
- [x] Todos os notebooks tocados são JSON válido
- [x] 163/163 testes de contrato passam (0 regressões)
- [x] Nenhum arquivo fora do manifesto foi tocado
- [ ] Validação live contra um workspace Databricks real — não executável neste ambiente, mesma limitação já documentada em features anteriores
- [x] Pronto para `/ship`

---

## Next Step

**Ready for:** `/ship .claude/sdd/features/DESIGN_GOLD_DIMENSION_JOIN_INTEGRITY.md`

**Antes do primeiro uso real:**
1. Rodar `pipeline_silver.ipynb` para `drivers`/`restaurants` num workspace real e confirmar
   que a regra `unique` não quebra com `silver_table` vazio (primeira execução).
2. Rodar `pipeline_users.ipynb` e confirmar a contagem impressa de
   `[users] quarantined (duplicate user_id): N` (esperado `N=0` nos dados atuais, já que o
   bug nunca foi confirmado para `drivers`/`restaurants`/`users` — só para o JOIN do Gold).
3. Rodar os 3 Gold notebooks afetados e confirmar que o `MERGE` final não falha mais com
   `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`.
