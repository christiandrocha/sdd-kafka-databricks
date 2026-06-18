# ADR 004 — Liquid Clustering Aligned with MERGE Key

**Status:** Accepted
**Date:** 2026-06-16

## Context

Delta Lake 3.1 (DBR 14.1+) introduces Liquid Clustering as the successor to ZORDER BY.
The critical non-obvious constraint: clustering only accelerates MERGE when the
cluster_by columns match the MERGE ON columns.

## Decision

Enforce that `storage.cluster_by` in every contract MUST contain `table.merge_key`.
This is validated automatically by `test_contracts.py` before any build runs.

## Rationale

**How Databricks executes MERGE with Liquid Clustering:**
1. Query planner examines MERGE ON condition: `ON target.event_id = source.event_id`
2. Checks if `event_id` is a clustering column → YES → uses file statistics to skip irrelevant files
3. Result: file pruning during MERGE — only files containing matching `event_id` values are scanned

**Without alignment:** Full table scan on every MERGE → clustering provides no benefit.

## Alignment table

| Table | cluster_by | merge_key | Aligned |
|---|---|---|---|
| silver.payment_events | [event_id, event_ts] | event_id | ✅ |
| silver.orders | [order_id] | order_id | ✅ |
| silver.users | [cpf] | cpf | ✅ |
| silver.payments | [payment_id] | payment_id | ✅ |
| gold.payment_lifecycle | [payment_id] | payment_id | ✅ |
| gold.driver_performance | [driver_id] | driver_id | ✅ |
| gold.revenue_per_restaurant | [restaurant_cnpj] | restaurant_cnpj | ✅ |

## Automated validation

```python
# tests/test_contracts.py
def test_cluster_by_aligns_with_merge_key():
    for yml_file in Path("contracts").glob("*.yml"):
        contract = yaml.safe_load(yml_file.read_text())
        merge_key = contract.get("table", {}).get("merge_key")
        cluster_by = contract.get("storage", {}).get("cluster_by", [])
        if merge_key and cluster_by:
            assert merge_key in cluster_by, \
                f"{yml_file.name}: merge_key '{merge_key}' not in cluster_by {cluster_by}"
```

## Alternatives considered

- **ZORDER BY** — requires full OPTIMIZE rewrite; not incremental; Liquid Clustering supersedes it
- **No clustering** — acceptable for small tables but wrong for Silver/Gold at scale

## Consequences

**Positive:** MERGE performance scales with data volume; validated by CI before deployment
**Negative:** cluster_by and merge_key must be kept in sync; test_contracts.py enforces this
