# KB: Data Contracts & Quality Rules — sdd-kafka-databricks patterns
# contracts/*.yml, contracts/loader.py, contracts/dlt_adapter.py

## Contract anatomy

One YAML per domain (20 total, `contracts/<domain>.yml`), 5 required top-level keys
(`tests/test_contracts.py::test_01` asserts all 5 are present):

```yaml
table:
  name: drivers
  layers: [bronze, silver]      # Bronze-only domains omit "silver"
  source: postgres_drivers
  kafka_topic: pg.public.drivers
  merge_key: uuid                # real CDC identity — never the Gold join column if they differ

schema:
  - { name: uuid, type: string, nullable: false }
  - { name: driver_id, type: string, nullable: false }
  # ...

quality:
  rules:
    - { field: driver_id, check: unique, on_failure: quarantine, severity: critical, scope: [silver] }

storage:
  cluster_by: [uuid]             # MUST contain merge_key (ADR-04, test_06 enforces it)
  compression: zstd

schema_evolution:
  new_fields: allowed
  removed_fields: forbidden
  type_changes: forbidden
```

## quality.rules fields

| Field | Meaning |
|---|---|
| `field` | Column the rule applies to — must exist in `schema` (`test_03`) |
| `check` | One of `VALID_CHECKS` (`contracts/loader.py`): `not_null`, `allowed_values`, `not_future`, `unique` |
| `on_failure` | `reject` (Bronze, drop the row) / `quarantine` (Silver, route to `quarantine.<domain>`) / `warn` (Silver, log only, row stays) — not validated by `loader.py`, only consumed by `dlt_adapter.py` |
| `severity` | `critical` / `warning` — descriptive only, not read by `dlt_adapter.py` |
| `scope` | List of `bronze` / `silver` — which layer's expectations the rule feeds |

`allowed_values` rules additionally require a non-empty `values: [...]` list
(`test_04`).

## How a rule becomes a DLT expectation (contracts/dlt_adapter.py)

```python
to_reject_expectations(contract, scope="bronze")   # -> @dp.expect_all_or_drop dict
to_warn_expectations(contract, scope="silver")      # -> @dp.expect_all dict (never drops)
quarantine_row_level_predicate(contract, scope="silver")  # -> SQL string for quarantine/clean split
```

`_condition_sql()` translates each check into a boolean that is **true when the row
passes**:

```python
not_null:       "{field} IS NOT NULL"
allowed_values: "{field} IS NULL OR {field} IN ({values})"
not_future:     "{field} IS NULL OR {field} <= current_timestamp()"
unique:         "true"      # always passes — see "check: unique gap" below
```

## check: unique — declared in the contract, NOT enforced pre-merge

This is the one rule type that does not get a row-level SQL condition.
`quarantine_row_level_predicate()` explicitly excludes it:

```python
rules = [
    r for r in contract["quality"]["rules"]
    if scope in r["scope"] and r["on_failure"] == "quarantine" and r["check"] != "unique"
]
```

**Why:** uniqueness needs a cross-row aggregation (`COUNT(DISTINCT merge_key)` per
field value), which Structured Streaming rejects without a watermark in `kafka`
mode (`"COUNT(DISTINCT ...) not supported in streaming"`). Architecturally, the
project treats uniqueness as a Silver-**merge-time** property (the `create_auto_cdc_flow`
upsert already dedupes by `merge_key`), not something the pre-merge Bronze→Silver
quarantine gate should police — a row can look "duplicate" pre-merge and still be
a perfectly valid update.

**What actually protects the 3 affected Gold joins today:** the `row_number()`
window-dedup guard inside each Gold table function (`register_gold_driver_performance`,
`register_gold_revenue_per_restaurant`, `register_gold_user_behavior`) — see
`kb/medallion.md`. This silently picks the latest row per join key; it does **not**
quarantine or log the duplicate the way a real `check: unique` enforcement would.

**Known doc/code gap, flagged for `/design` review:** `CLAUDE.md`'s "Gold dimension
join integrity" section describes `check: unique` as "enforced in Silver via
anti-join against the existing table" — that describes the *intended* design from
`docs/adr/005_gold_dimension_join_integrity.md`, not the current implementation. As
of `pipelines/ubereats_pipeline.py` + `contracts/dlt_adapter.py` today, the contract
rule is structural/declarative only (`tests/test_contracts.py::test_09` checks the
rule is *declared* for `drivers.driver_id` and `restaurants.cnpj`, not that it's
enforced at runtime). Don't assume quarantine rows ever get created for `check: unique`
violations — they currently never do.

## What tests/test_contracts.py actually validates

10 structural/static tests, parametrized over all 20 contracts — they check the
**shape** of the YAML (keys present, types correct, `cluster_by` ⊇ `merge_key`,
`allowed_values` non-empty, unknown `check` rejected), not runtime DLT behavior.
There is no test that proves a quarantine table actually receives a bad row in a
live pipeline run — that would need an integration test against a running pipeline.

## Anti-patterns

| Never do | Why | Instead |
|---|---|---|
| Assume `check: unique` quarantines duplicates | It's excluded from `quarantine_row_level_predicate` on purpose (streaming limitation) | Keep the `row_number()` guard at every Gold join on a non-`merge_key` column |
| Change `merge_key` to whatever column Gold happens to join on | `merge_key` is the real CDC identity; changing it breaks `create_auto_cdc_flow` semantics | Add a `check: unique` rule on the join column instead (declarative intent + future enforcement target) |
| Add a new `check` type without updating `VALID_CHECKS` and `_condition_sql()` | `loader.py` raises `ValueError` on unknown checks (`test_10`) | Add to both, plus a contracts test for the new check |
| Trust `severity`/`on_failure` validation at load time | Neither is checked by `loader.py` — a typo like `on_failure: qurantine` silently does nothing | Cross-check new contracts against `tests/test_contracts.py` and a real pipeline run, not just `load_contract()` succeeding |
