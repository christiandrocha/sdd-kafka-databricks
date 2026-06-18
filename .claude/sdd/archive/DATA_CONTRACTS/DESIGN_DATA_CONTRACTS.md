# DESIGN: Data Contracts — Python Implementation (Agent 4)

> Especificação técnica dos 4 arquivos Python que tornam os 20 contratos YAML operacionais

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | DATA_CONTRACTS |
| **Date** | 2026-06-16 |
| **Author** | design-agent |
| **DEFINE** | [DEFINE_DATA_CONTRACTS.md](./DEFINE_DATA_CONTRACTS.md) |
| **Status** | Ready for Build |

---

## Architecture Overview

```text
┌─────────────────────────────────────────────────────────────────────┐
│                    contracts/ — fonte de verdade                     │
│                                                                      │
│   contracts/*.yml (20 arquivos)                                      │
│          │                                                           │
│          ▼                                                           │
│   ┌─────────────┐   valida     ┌──────────────────────┐             │
│   │  loader.py  │ ──────────→  │  test_contracts.py   │ (CI gate)   │
│   │  load_contract()           └──────────────────────┘             │
│   └──────┬──────┘                                                   │
│          │ dict                                                      │
│    ┌─────┴──────────────────────────────────┐                       │
│    ▼                                         ▼                      │
│  spark_schema.py                    pydantic_models.py              │
│  to_struct_type()                   get_model()                     │
│  to_tblproperties()                      │                          │
│  to_cluster_by_sql()                     │                          │
│    │                                      │                         │
│    ▼                                      ▼                         │
│  pipeline_bronze.ipynb           load_to_postgres.py                │
│  pipeline_silver.ipynb           (validação pré-INSERT)             │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Components

| Component | Purpose | Tecnologia |
|-----------|---------|------------|
| `contracts/loader.py` | Parse + validação semântica dos YAMLs | Python 3.11 stdlib + PyYAML |
| `contracts/spark_schema.py` | Gera StructType, TBLPROPERTIES e CLUSTER BY | Python 3.11 + PySpark (deferred import) |
| `contracts/pydantic_models.py` | Modelos Pydantic v2 por domínio | Python 3.11 + Pydantic v2 |
| `tests/test_contracts.py` | Valida consistência dos 20 YAMLs (CI gate) | pytest 8.0 |

---

## Key Decisions

### Decision 1: loader.py sem dependências além de PyYAML

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-16 |

**Context:** `loader.py` é o bootstrap de toda a cadeia — se ele tiver deps externas, o CI precisa de um ambiente mais pesado só para validar YAML.

**Choice:** Usar apenas `yaml` (PyYAML), `pathlib.Path`, `typing` e stdlib.

**Alternatives Rejected:**
1. `jsonschema` — adicionaria validação de schema JSON, mas requer instalação extra e é overkill para contratos simples
2. `pydantic` — contraditório usar o validador em si para validar o esquema que gera os validadores

**Consequences:** Validações são imperativos Python explícitos (mais verboso, mas zero deps transitivas).

---

### Decision 2: spark_schema.py com deferred import de PySpark

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-16 |

**Context:** `test_contracts.py` roda em CI sem cluster Databricks. Se `spark_schema.py` importar PySpark no topo, `import contracts.spark_schema` falha onde não há Spark instalado.

**Choice:** `from pyspark.sql.types import ...` fica **dentro** de `to_struct_type()`. As funções `to_tblproperties()` e `to_cluster_by_sql()` não importam PySpark — operam apenas sobre dicts e strings.

**Alternatives Rejected:**
1. Import condicional no topo (`try/except ImportError`) — mascara erros reais em ambientes com Spark parcialmente instalado
2. Módulo separado `spark_schema_spark.py` — fragmenta a interface pública sem benefício real

**Consequences:** `to_struct_type()` só pode ser testada com Spark disponível. `to_tblproperties()` e `to_cluster_by_sql()` são testáveis em qualquer ambiente.

---

### Decision 3: pydantic_models.py com geração dinâmica via create_model()

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-16 |

**Context:** 20 modelos estáticos = 20 classes = 600+ linhas repetitivas. Qualquer mudança num contrato YAML exige edição manual do modelo correspondente.

**Choice:** Usar `pydantic.create_model()` para gerar as 20 classes dinamicamente na importação do módulo, cacheadas em `_MODELS: dict[str, type[BaseModel]]`.

**Alternatives Rejected:**
1. 20 classes estáticas — viola DRY, sincronização manual entre YAML e Python
2. Metaclasse customizada — complexidade desnecessária quando `create_model()` resolve

**Consequences:** Os modelos não aparecem na IDE com autocompletar estático. Trade-off aceito para manter DRY.

---

### Decision 4: Pydantic models excluem campos Bronze metadata

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-16 |

**Context:** Os campos `__op`, `__source_ts_ms` e `_ingested_at` são adicionados pelo Debezium/Bronze notebook — **não existem** no JSON original que `load_to_postgres.py` processa. Pydantic v2 não aceita nomes de campos com `__` prefix sem aliases explícitos.

**Choice:** `_contract_to_model()` filtra campos onde `name.startswith("_")` antes de criar o modelo. Os modelos Pydantic cobrem apenas o schema de negócio.

**Alternatives Rejected:**
1. Incluir com `Field(alias="__op")` — os campos não existem no input de `load_to_postgres.py`, então incluí-los só adicionaria `Optional` com default `None` sem utilidade
2. Dois modelos por tabela (bronze + business) — YAGNI

---

### Decision 5: test_contracts.py usa parametrize sobre os 20 arquivos

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-16 |

**Context:** 7 testes × 20 contratos = 140 casos possíveis. Um loop `for` num único teste dá uma falha genérica; `pytest.mark.parametrize` dá falha precisa por arquivo.

**Choice:** `@pytest.mark.parametrize("contract_path", sorted(Path("contracts").glob("*.yml")))` para testes que validam todos os 20. Testes de spark_schema/loader específicos usam fixtures fixas.

---

## File Manifest

| # | File | Action | Depende de | Propósito |
|---|------|--------|------------|-----------|
| 1 | `contracts/loader.py` | Create | PyYAML (já em pyproject.toml) | Parse + validação semântica |
| 2 | `contracts/spark_schema.py` | Create | #1, PySpark (deferred) | StructType + DDL + TBLPROPERTIES |
| 3 | `contracts/pydantic_models.py` | Create | #1, Pydantic v2 (já em pyproject.toml) | Modelos de validação para load_to_postgres |
| 4 | `tests/test_contracts.py` | Create | #1, #2, #3 | CI gate — 7 testes sobre os 20 YAMLs |
| 5 | `contracts/__init__.py` | Create | — | Torna contracts/ um package importável |

**Total Files:** 5 (4 principais + 1 `__init__.py`)

---

## Estrutura detalhada: contracts/loader.py

```text
contracts/loader.py
│
├── CONSTANTS
│   ├── VALID_TYPES: frozenset  — {"string", "integer", "long", "double",
│   │                               "boolean", "timestamp", "date"}
│   ├── VALID_CHECKS: frozenset — {"not_null", "allowed_values", "not_future"}
│   └── VALID_SCOPES: frozenset — {"bronze", "silver"}
│
├── PRIVATE VALIDATORS  (levantam ValueError com msg descritiva)
│   ├── _validate_schema(table_name: str, schema: list[dict]) -> None
│   │     • cada field tem "name" (str) e "type" (in VALID_TYPES)
│   │     • "nullable" é bool Python — isinstance check, não apenas truthy
│   │     • sem campos duplicados
│   │
│   ├── _validate_quality(table_name: str, field_names: set[str],
│   │                     quality: dict) -> None
│   │     • cada rule.field existe em field_names
│   │     • rule.check está em VALID_CHECKS
│   │     • se check == "allowed_values": values é lista não-vazia
│   │     • rule.scope é lista de strings em VALID_SCOPES
│   │
│   └── _validate_storage(table_name: str, field_names: set[str],
│                         storage: dict) -> None
│         • cluster_by é lista
│         • cada coluna em cluster_by existe em field_names
│         • merge_key (de table.merge_key) está em cluster_by  ← ADR-04
│
└── PUBLIC API
    └── load_contract(path: str | Path) -> dict
          1. Path(path).read_text()
          2. yaml.safe_load(text)
          3. valida keys obrigatórias: [table, schema, quality, storage,
                                        schema_evolution]
          4. table_name = contract["table"]["name"]
          5. field_names = {f["name"] for f in contract["schema"]}
          6. _validate_schema(table_name, contract["schema"])
          7. _validate_quality(table_name, field_names, contract["quality"])
          8. _validate_storage(table_name, field_names, contract["storage"])
          9. return contract
```

**Assinatura pública:**
```python
def load_contract(path: str | Path) -> dict:
    """
    Carrega e valida semanticamente um contrato YAML.

    Raises:
        FileNotFoundError: se o arquivo não existe
        ValueError: se o contrato é semanticamente inválido,
                    com mensagem incluindo table.name e campo problemático
    """
```

---

## Estrutura detalhada: contracts/spark_schema.py

```text
contracts/spark_schema.py
│
├── CONSTANTS
│   └── _PYSPARK_TYPE_NAMES: dict[str, str]
│         "string"    → "StringType"
│         "integer"   → "IntegerType"
│         "long"      → "LongType"
│         "double"    → "DoubleType"
│         "boolean"   → "BooleanType"
│         "timestamp" → "TimestampType"
│         "date"      → "DateType"
│
├── PRIVATE HELPERS
│   └── _serialize_property_value(v: Any) -> str
│         isinstance(v, bool) → str(v).lower()   # True  → "true"
│         isinstance(v, int)  → str(v)            # 100   → "100"
│         default             → str(v)
│
└── PUBLIC API
    │
    ├── to_struct_type(contract: dict) -> "StructType"
    │     • deferred import: from pyspark.sql.types import ...
    │     • itera contract["schema"]
    │     • cada field → StructField(name, <Type>(), nullable)
    │     • retorna StructType([...])
    │
    ├── to_tblproperties(contract: dict) -> dict[str, str]
    │     • itera contract["storage"]["properties"]
    │     • chaves ficam como estão (strings Python)
    │     • valores → _serialize_property_value(v)
    │     • retorna {"delta.enableChangeDataFeed": "true", ...}
    │
    ├── to_cluster_by_sql(contract: dict) -> str
    │     • cols = contract["storage"].get("cluster_by", [])
    │     • len(cols) == 0 → return ""
    │     • return f"CLUSTER BY ({', '.join(cols)})"
    │
    └── to_create_table_ddl(contract: dict, table_fqn: str) -> str
          • chama to_tblproperties() para formatar TBLPROPERTIES
          • chama to_cluster_by_sql() para CLUSTER BY (omitido se "")
          • retorna string DDL completa:
            CREATE TABLE IF NOT EXISTS {table_fqn} (...)
            USING DELTA
            CLUSTER BY (...)          ← omitido se cluster_by vazio
            TBLPROPERTIES (
              'delta.enableChangeDataFeed' = 'true',
              ...
            )
```

**Assinaturas públicas:**
```python
def to_struct_type(contract: dict) -> "StructType":
    """Gera StructType PySpark. Requer PySpark instalado no ambiente."""

def to_tblproperties(contract: dict) -> dict[str, str]:
    """Gera TBLPROPERTIES como dict[str, str]. Não requer PySpark."""

def to_cluster_by_sql(contract: dict) -> str:
    """Retorna 'CLUSTER BY (col1, col2)' ou '' se cluster_by vazio. Não requer PySpark."""

def to_create_table_ddl(contract: dict, table_fqn: str) -> str:
    """DDL completo CREATE TABLE IF NOT EXISTS. Não requer PySpark."""
```

---

## Estrutura detalhada: contracts/pydantic_models.py

```text
contracts/pydantic_models.py
│
├── IMPORTS
│   ├── from datetime import date, datetime
│   ├── from pathlib import Path
│   ├── from typing import Optional
│   └── from pydantic import BaseModel, create_model
│       from contracts.loader import load_contract
│
├── CONSTANTS
│   └── _CONTRACTS_DIR: Path = Path(__file__).parent
│
├── TYPE MAPPING
│   └── _YAML_TO_PYTHON: dict[str, type]
│         "string"    → str
│         "integer"   → int
│         "long"      → int
│         "double"    → float
│         "boolean"   → bool
│         "timestamp" → datetime
│         "date"      → date
│
├── PRIVATE HELPERS
│   │
│   ├── _yaml_type_to_annotation(yaml_type: str, nullable: bool) -> type
│   │     • nullable=False → tipo direto (str, int, float, ...)
│   │     • nullable=True  → Optional[tipo] (com default None)
│   │
│   ├── _contract_to_model(contract: dict) -> type[BaseModel]
│   │     • table_name = contract["table"]["name"]
│   │     • filtra campos: skip se name.startswith("_")
│   │       (exclui __op, __source_ts_ms, _ingested_at — Bronze metadata)
│   │     • constrói fields_kwargs para create_model():
│   │         nullable=False: (type, ...)           ← campo obrigatório
│   │         nullable=True:  (Optional[type], None) ← campo opcional
│   │     • return create_model(table_name, **fields_kwargs)
│   │
│   └── _load_all_models() -> dict[str, type[BaseModel]]
│         • glob contracts/*.yml
│         • load_contract(path) para cada um
│         • _contract_to_model(contract) para cada um
│         • return {table_name: ModelClass, ...}
│
├── MODULE-LEVEL CACHE
│   └── _MODELS: dict[str, type[BaseModel]] = _load_all_models()
│       (executado uma vez na importação do módulo)
│
└── PUBLIC API
    └── get_model(table_name: str) -> type[BaseModel]
          • table_name não encontrado → KeyError descritivo
          • return _MODELS[table_name]
```

**Assinatura pública:**
```python
def get_model(table_name: str) -> type[BaseModel]:
    """
    Retorna modelo Pydantic v2 para o domínio especificado.

    Args:
        table_name: nome da tabela (ex: "payment_events", "orders")

    Raises:
        KeyError: se table_name não corresponde a nenhum contrato YAML

    Example:
        Model = get_model("payment_events")
        record = Model(event_id="abc", payment_id="xyz", ...)
    """
```

---

## Estrutura detalhada: tests/test_contracts.py

```text
tests/test_contracts.py
│
├── IMPORTS + FIXTURES
│   ├── import pytest
│   ├── from pathlib import Path
│   ├── from contracts.loader import load_contract
│   └── from contracts.spark_schema import to_tblproperties, to_cluster_by_sql
│
│   CONTRACTS_DIR = Path("contracts")
│   ALL_CONTRACTS  = sorted(CONTRACTS_DIR.glob("*.yml"))
│   # 20 arquivos — parametrize sobre esta lista
│
└── 7 TESTES
    │
    ├── test_01_all_contracts_load_without_error          (AT-001)
    │   @pytest.mark.parametrize("contract_path", ALL_CONTRACTS)
    │   • load_contract(contract_path) → dict
    │   • assert set(result.keys()) >= {"table", "schema", "quality",
    │                                    "storage", "schema_evolution"}
    │   Verifica: todos os 20 carregam sem exceção
    │
    ├── test_02_nullable_is_bool_not_string               (AT-001 / Success Criteria)
    │   @pytest.mark.parametrize("contract_path", ALL_CONTRACTS)
    │   • para cada field em contract["schema"]:
    │       assert isinstance(field["nullable"], bool)
    │   Verifica: nullable nunca é "true"/"false" string
    │
    ├── test_03_quality_rules_reference_existing_fields   (AT-002)
    │   @pytest.mark.parametrize("contract_path", ALL_CONTRACTS)
    │   • field_names = {f["name"] for f in contract["schema"]}
    │   • para cada rule em contract["quality"]["rules"]:
    │       assert rule["field"] in field_names
    │   Verifica: quality.rule.field existe no schema
    │
    ├── test_04_allowed_values_are_non_empty              (AT-008 / Success Criteria)
    │   @pytest.mark.parametrize("contract_path", ALL_CONTRACTS)
    │   • para cada rule onde check == "allowed_values":
    │       assert isinstance(rule["values"], list)
    │       assert len(rule["values"]) > 0
    │   Verifica: nenhuma lista de valores vazia
    │
    ├── test_05_cluster_by_is_subset_of_schema_fields     (Success Criteria)
    │   @pytest.mark.parametrize("contract_path", ALL_CONTRACTS)
    │   • field_names = {f["name"] for f in contract["schema"]}
    │   • for col in contract["storage"]["cluster_by"]:
    │       assert col in field_names
    │   Verifica: cluster_by só referencia colunas existentes
    │
    ├── test_06_merge_key_in_cluster_by                   (AT-007 / ADR-04)
    │   @pytest.mark.parametrize("contract_path", ALL_CONTRACTS)
    │   • merge_key = contract["table"]["merge_key"]
    │   • cluster_by = contract["storage"]["cluster_by"]
    │   • assert merge_key in cluster_by
    │   Verifica: ADR-04 — merge_key deve estar em cluster_by
    │
    └── test_07_tblproperties_values_are_strings          (AT-004)
        @pytest.mark.parametrize("contract_path", ALL_CONTRACTS)
        • props = to_tblproperties(contract)
        • for k, v in props.items():
            assert isinstance(v, str), f"{k}: {v!r} deve ser str"
            assert v in ("true", "false") or v.isdigit() or isinstance(v, str)
        Verifica: TBLPROPERTIES nunca contém bool ou int Python nativos
```

---

## Data Flow

```text
1. CI executa: pytest tests/test_contracts.py
   │
   ▼
2. test_contracts.py chama load_contract() para cada um dos 20 YAMLs
   │   └── loader.py: yaml.safe_load → _validate_* → dict
   ▼
3. Testes 1-6 validam consistência YAML (sem Spark, sem Pydantic)
   │
   ▼
4. Teste 7 chama to_tblproperties() → valida serialização de valores
   │
   ▼
5. CI gate: se 0 failures → pipeline pode avançar para Agent 5 (spark-bronze)

Em runtime (notebooks Databricks):
   load_contract(contract_path)
        │
        ├──→ to_struct_type(contract)     ← Agent 5/6: CREATE TABLE DDL
        ├──→ to_tblproperties(contract)  ← Agent 5/6: TBLPROPERTIES
        └──→ to_cluster_by_sql(contract) ← Agent 5/6: CLUSTER BY

Em load_to_postgres.py:
   get_model("payment_events")
        └──→ Model(event_id=..., payment_id=..., ...)  ← validação pré-INSERT
```

---

## Testing Strategy

| Test | Escopo | Arquivo | Requer Spark? | Parametrizado? |
|------|--------|---------|---------------|----------------|
| test_01 — load sem erro | 20 contratos | test_contracts.py | Não | Sim (20×) |
| test_02 — nullable is bool | 20 contratos | test_contracts.py | Não | Sim (20×) |
| test_03 — fields existem | 20 contratos | test_contracts.py | Não | Sim (20×) |
| test_04 — allowed_values | 20 contratos | test_contracts.py | Não | Sim (20×) |
| test_05 — cluster_by subset | 20 contratos | test_contracts.py | Não | Sim (20×) |
| test_06 — ADR-04 | 20 contratos | test_contracts.py | Não | Sim (20×) |
| test_07 — TBLPROPERTIES str | 20 contratos | test_contracts.py | Não | Sim (20×) |

**Todos os 7 testes rodam sem Spark instalado.** `to_struct_type()` (requer Spark) não é coberta em CI — é testada manualmente no notebook ou em ambiente com Spark.

---

## Error Handling

| Erro | Origem | Handling |
|------|--------|----------|
| `FileNotFoundError` | `load_contract()` — arquivo não encontrado | Propaga com path incluído |
| `ValueError: "orders: merge_key 'order_id' not in cluster_by"` | `_validate_storage()` | Levanta com `table.name` + campo |
| `ValueError: "payments: quality rule field 'nonexistent' not in schema"` | `_validate_quality()` | Levanta com `table.name` + field |
| `ValueError: "inventory: nullable must be bool, got str for field 'stock_id'"` | `_validate_schema()` | Levanta com `table.name` + field name |
| `KeyError: "unknown_table not found in models"` | `get_model()` | Levanta com table_name + lista de disponíveis |

---

## Integration Points

| Sistema | Integração | Sentido |
|---------|------------|---------|
| `pipeline_bronze.ipynb` | `from contracts.spark_schema import to_struct_type, to_create_table_ddl` | Consome |
| `pipeline_silver.ipynb` | `from contracts.loader import load_contract` | Consome |
| `tests/load_to_postgres.py` | `from contracts.pydantic_models import get_model` | Consome |
| GitHub Actions CI | `pytest tests/test_contracts.py` | Executa |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-06-16 | design-agent | Initial version |

---

## Next Step

**Ready for:** `/build .claude/sdd/features/DESIGN_DATA_CONTRACTS.md`
