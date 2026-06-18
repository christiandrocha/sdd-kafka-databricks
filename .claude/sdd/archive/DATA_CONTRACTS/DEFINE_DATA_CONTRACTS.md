# DEFINE: Data Contracts — Python Implementation (Agent 4)

> Implementar os 4 arquivos Python que tornam os 20 contratos YAML operacionais no pipeline

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | DATA_CONTRACTS |
| **Date** | 2026-06-16 |
| **Author** | define-agent |
| **Status** | Ready for Design |
| **Clarity Score** | 15/15 |
| **Predecessor** | BRAINSTORM_DATA_CONTRACTS.md (20 YAMLs aprovados) |

---

## Problem Statement

Os 20 contratos YAML estão aprovados em `contracts/` mas são inertes sem os 4 arquivos Python que os consomem. Sem `loader.py`, `spark_schema.py`, `pydantic_models.py` e `test_contracts.py`, os notebooks Bronze/Silver não têm como gerar DDL, aplicar quality rules ou validar consistência — e o CI não tem como garantir ADR-04 antes do deploy.

---

## Target Users

| User | Role | Pain Point |
|------|------|------------|
| Agent 5 (spark-bronze) | Notebook Bronze | Precisa de StructType + DDL TBLPROPERTIES gerados do contrato |
| Agent 6 (spark-silver) | Notebook Silver | Precisa de quality rules carregadas do contrato para MERGE + quarantine |
| `load_to_postgres.py` | Loader de dados | Precisa de modelos Pydantic v2 para validar registros antes do INSERT |
| Agent 9 (CI/CD) | GitHub Actions CI | Precisa de `pytest tests/test_contracts.py` que falhe antes do deploy se ADR-04 for violado |

---

## Goals

| Priority | Goal |
|----------|------|
| **MUST** | `loader.py` — parse + validação semântica dos YAMLs, zero deps além de PyYAML |
| **MUST** | `spark_schema.py` — gera StructType dict, TBLPROPERTIES string e cláusula CLUSTER BY |
| **MUST** | `pydantic_models.py` — gera modelos Pydantic v2 para os 20 domínios |
| **MUST** | `tests/test_contracts.py` — valida consistência dos 20 YAMLs (ADR-04 + quality rules) |
| **SHOULD** | `loader.py` levanta erros descritivos com `table.name` + campo problemático |
| **SHOULD** | `spark_schema.py` importável sem Spark instalado (deferred import ou string output) |

---

## Success Criteria

- [ ] `python -c "from contracts.loader import load_contract; load_contract('contracts/payment_events.yml')"` sem erro
- [ ] `spark_schema.py` mapeia todos os 7 tipos YAML (`string`, `integer`, `long`, `double`, `boolean`, `timestamp`, `date`) para tipos PySpark corretos
- [ ] TBLPROPERTIES serializa booleans como `'true'`/`'false'` (string lowercase) e inteiros como string — nunca Python `True`/`False` ou int
- [ ] Chaves com ponto em TBLPROPERTIES ficam entre aspas simples: `'delta.enableChangeDataFeed'`
- [ ] `cluster_by: []` ou ausente → cláusula CLUSTER BY omitida (não gera string vazia)
- [ ] `pytest tests/test_contracts.py` passa nos 20 contratos sem erros
- [ ] `test_contracts.py` falha se `merge_key` não está em `cluster_by` (ADR-04)
- [ ] `test_contracts.py` falha se quality rule referencia campo ausente no schema
- [ ] `test_contracts.py` falha se `allowed_values` é lista vazia
- [ ] `nullable` em todos os contratos é `bool` Python — `test_contracts.py` falha se encontrar string

---

## Acceptance Tests

| ID | Scenario | Given | When | Then |
|----|----------|-------|------|------|
| AT-001 | Load válido | `contracts/payment_events.yml` existe | `load_contract(path)` | Retorna dict com keys `[table, schema, quality, storage, schema_evolution]` |
| AT-002 | Load inválido — campo inexistente | `quality.rule.field = "nonexistent"` | `load_contract(path)` | Levanta `ValueError` com nome da tabela e campo |
| AT-003 | Tipo INTEGER | `order_status.status_id` type `integer` | `to_struct_type(contract)` | Campo gera `IntegerType()` |
| AT-004 | Bool em TBLPROPERTIES | `delta.enableChangeDataFeed: true` | `to_tblproperties(contract)` | Produz `'delta.enableChangeDataFeed': 'true'` |
| AT-005 | CLUSTER BY presente | `cluster_by: [event_id]` | `to_cluster_by_sql(contract)` | Retorna `"CLUSTER BY (event_id)"` |
| AT-006 | CLUSTER BY ausente | `cluster_by: []` ou campo ausente | `to_cluster_by_sql(contract)` | Retorna `""` (string vazia — omitido pelo notebook) |
| AT-007 | ADR-04 violado | `merge_key: event_id`, `cluster_by: [payment_id]` | `pytest test_contracts.py` | Falha com mensagem de erro identificando a tabela |
| AT-008 | allowed_values vazio | `values: []` em qualquer regra | `pytest test_contracts.py` | Falha |
| AT-009 | Pydantic model | `users_mongo.yml` com `uuid: string, nullable: false` | `generate_model("users_mongo", contract)` | `uuid: str` como campo obrigatório no modelo |
| AT-010 | Suite completa | 20 YAMLs em `contracts/` | `pytest tests/test_contracts.py` | 0 failures, 0 errors |

---

## Out of Scope

- Contrato para `payment_current_state` (view Silver derivada — sem Kafka topic)
- `loader.py` fazer file-watching ou reload automático de contratos
- `spark_schema.py` gerar código Scala ou suportar outros dialetos
- Geração automática de contratos a partir dos JSONs de `tests/data/`
- Validação de tipos de dados nos registros reais (responsabilidade do notebook Silver)
- Suporte a formatos além de YAML (JSON, TOML)

---

## Constraints

| Type | Constraint | Impact |
|------|------------|--------|
| Technical | `loader.py`: zero deps externas além de `PyYAML` | Sem `pydantic`, `jsonschema` ou similares em `loader.py` |
| Technical | `spark_schema.py`: booleans → `str(v).lower()`, inteiros → `str(v)` | TBLPROPERTIES nunca contém tipos Python nativos |
| Technical | Chaves com ponto em TBLPROPERTIES entre aspas simples | `'delta.enableChangeDataFeed'` não `delta.enableChangeDataFeed` |
| Technical | `nullable` deve ser `bool` Python no YAML, nunca string | `test_contracts.py` valida com `isinstance(field["nullable"], bool)` |
| Technical | `cluster_by` vazio → omitir CLUSTER BY | Notebooks verificam se string retornada é não-vazia |
| Technical | `pydantic_models.py`: Pydantic v2 (`model_validator`, `field_validator`) | Não usar v1 API (`@validator`, `@root_validator`) |
| Design | `merge_key` deve estar em `cluster_by` (ADR-04) | Validado em `test_contracts.py`, não em runtime |

---

## Technical Context

| Aspect | Value | Notes |
|--------|-------|-------|
| **Deployment Location** | `contracts/` (3 Python files) + `tests/` (1 test file) | Sem subpastas novas |
| **KB Domains** | `data-quality`, `spark`, `databricks` | Tipos StructType + TBLPROPERTIES Delta |
| **IaC Impact** | None | Arquivos Python locais — sem infra |
| **Imports proibidos em loader.py** | `pydantic`, `pyspark`, `jsonschema` | Apenas `yaml`, `pathlib`, `typing`, stdlib |
| **Imports em spark_schema.py** | `pyspark` deferred (dentro de função) | Importável em test sem Spark instalado |

---

## Data Contract

### Mapa de tipos YAML → PySpark

| YAML type | PySpark type | Python stdlib equivalent |
|-----------|-------------|--------------------------|
| `string` | `StringType()` | `str` |
| `integer` | `IntegerType()` | `int` |
| `long` | `LongType()` | `int` |
| `double` | `DoubleType()` | `float` |
| `boolean` | `BooleanType()` | `bool` |
| `timestamp` | `TimestampType()` | `datetime` |
| `date` | `DateType()` | `date` |

### Mapa de tipos YAML → Pydantic v2

| YAML type | Pydantic / Python type | nullable=true → |
|-----------|----------------------|-----------------|
| `string` | `str` | `Optional[str] = None` |
| `integer` | `int` | `Optional[int] = None` |
| `long` | `int` | `Optional[int] = None` |
| `double` | `float` | `Optional[float] = None` |
| `boolean` | `bool` | `Optional[bool] = None` |
| `timestamp` | `datetime` | `Optional[datetime] = None` |
| `date` | `date` | `Optional[date] = None` |

### Interfaces públicas dos 3 módulos

```python
# contracts/loader.py
def load_contract(path: str | Path) -> dict:
    """Carrega e valida semanticamente um contrato YAML. Levanta ValueError se inválido."""

# contracts/spark_schema.py
def to_struct_type(contract: dict) -> "StructType":
    """Gera StructType PySpark a partir do schema do contrato."""

def to_tblproperties(contract: dict) -> dict[str, str]:
    """Gera dict de TBLPROPERTIES com todos os valores como string."""

def to_cluster_by_sql(contract: dict) -> str:
    """Retorna 'CLUSTER BY (col1, col2)' ou '' se cluster_by vazio."""

# contracts/pydantic_models.py
def get_model(table_name: str) -> type[BaseModel]:
    """Retorna modelo Pydantic v2 para o domínio especificado."""
```

---

## Assumptions

| ID | Assumption | If Wrong, Impact | Validated? |
|----|------------|------------------|------------|
| A-001 | PyYAML está disponível no ambiente (`pip install pyyaml`) | `loader.py` não importa | [x] `pyproject.toml` confirmar |
| A-002 | Pydantic v2 está disponível (`pydantic>=2.0`) | `pydantic_models.py` quebra | [x] `pyproject.toml` confirmar |
| A-003 | `nullable` em todos os 20 YAMLs é `bool` Python (não string) | `test_contracts.py` falha imediatamente | [x] Gerado assim no brainstorm |
| A-004 | `cluster_by` está presente em todos os 20 contratos como lista (mesmo que vazia não ocorra) | `to_cluster_by_sql` lança KeyError | [x] Todos os 20 têm `cluster_by` não-vazio |

---

## Clarity Score Breakdown

| Element | Score | Notes |
|---------|-------|-------|
| Problem | 3 | 4 arquivos nomeados, responsabilidade de cada um explícita |
| Users | 3 | Agent 5, Agent 6, load_to_postgres.py, Agent 9 identificados com pain point |
| Goals | 3 | MUST/SHOULD com regras técnicas obrigatórias detalhadas |
| Success | 3 | 10 critérios mensuráveis e testáveis |
| Scope | 3 | Out of scope explícito (payment_current_state, file-watching, Scala, etc.) |
| **Total** | **15/15** | |

---

## Open Questions

Nenhuma — pronto para Design.

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-06-16 | define-agent | Initial version |

---

## Next Step

**Ready for:** `/design .claude/sdd/features/DEFINE_DATA_CONTRACTS.md`
