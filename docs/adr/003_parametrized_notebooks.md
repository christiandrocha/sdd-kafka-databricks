# ADR 003 — 2 Parametrized Notebooks Instead of 60 Static

**Status:** Accepted
**Date:** 2026-06-16

## Context

The pipeline processes 20 domains (Bronze) and 12 domains (Silver).
The naive approach: create one notebook per domain per layer = 60 notebooks.

## Decision

Create 2 parametrized notebooks (pipeline_bronze.ipynb + pipeline_silver.ipynb)
that receive domain-specific configuration via dbutils.widgets.
DABs orchestrates each notebook 20x (bronze) and 12x (silver) with different parameters.

## Rationale

**DRY principle applied to data engineering:**

```python
# pipeline_bronze.ipynb — runs 20 times with different params
dbutils.widgets.text("table_name",   "payment_events")  # → "orders" → "payments"...
dbutils.widgets.text("kafka_topic",  "pg.public.payment_events")
dbutils.widgets.text("bronze_table", "ubereats_dev.bronze.payment_events")
dbutils.widgets.text("max_offsets",  "1000")  # order_items uses "5000"
```

If Bronze read logic changes (e.g., new Kafka option), edit 1 file, not 20.
This demonstrates software engineering maturity in a data engineering context.

## Special cases handled via widgets

- `order_items`: max_offsets=5000 (85% of volume — larger buffer)
- `users`: separate notebook (FULL OUTER JOIN of users_mongo + users_mssql — not parametrizable)

## Alternatives considered

- **60 static notebooks** — rejected: DRY violation, 20x maintenance burden
- **Lakeflow DLT** — different abstraction; less explicit control over MERGE logic

## Consequences

**Positive:** Single change point; cleaner DABs yaml; easier to test
**Negative:** Special cases (users, order_items) need conditional logic or separate notebooks
