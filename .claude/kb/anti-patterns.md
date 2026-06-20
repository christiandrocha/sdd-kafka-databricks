# KB: Anti-Patterns — Centralized Reference
# Cross-domain anti-patterns with severity, mapped to where each one actually
# applies (or has already been fixed) in sdd-kafka-databricks.

## Severity

| Severity | Action | Criteria |
|---|---|---|
| **CRITICAL** | Block immediately, don't proceed | Data loss risk, PII exposure, irreversible destruction |
| **HIGH** | Warn user, require explicit confirmation | Severe performance degradation, silent failure, runaway cost |
| **MEDIUM** | Log a warning, suggest a fix | Bad practice, tech debt, naming inconsistency |

## CRITICAL

| ID | Anti-pattern | Status in this project |
|---|---|---|
| C01 | `SELECT *` / full scan without WHERE/LIMIT on a production table | Gold tables use `dp.read(silver_*)` full recompute by design (`kb/medallion.md`) — that's intentional batch recompute, not an accidental full scan. Don't add a `WHERE` to "fix" this; it would break the Gold contract. |
| C02 | `DROP TABLE`/`DROP DATABASE` without a verified backup | No automated DROP anywhere in `pipelines/ubereats_pipeline.py` or `scripts/`. Keep it that way — any manual `DROP` needs a Delta `RESTORE TABLE ... TO VERSION AS OF N` plan first. |
| C03 | PII unmasked in queries/outputs | **Open gap** — `cpf`/`cnpj`/`email`/`license_number` flow unmasked Bronze→Silver→Gold (`gold.user_behavior.cpf` included). See `kb/governance.md` for the Unity Catalog masking patterns to close this. |
| C04 | Hardcoded secrets/tokens in code or notebooks | `pipelines/ubereats_pipeline.py` reads `spark.conf.get(...)` for every credential-adjacent value (`KAFKA_BOOTSTRAP`, `SCHEMA_REGISTRY_URL`); `.env` is git-ignored and CI's `env-guard` job fails the build if `.env` is ever tracked. |
| C05 | `TRUNCATE` on a silver/gold table without a checkpoint | Not used — Silver uses `create_auto_cdc_flow` (incremental upsert), Gold is `@dp.table` full recompute (idempotent by construction, not by truncation). |
| C06 | Writing straight to a production table without staging | Every domain goes Bronze→Silver(→Gold); the quarantine pair (`kb/medallion.md`) is itself a staging gate before a row reaches Silver clean. |
| C07 | `collect()` on a large dataset (driver OOM) | Not used anywhere in `pipelines/ubereats_pipeline.py` — `order_items` (110k rows, 85% of volume) is always handled via DataFrame ops, never collected to the driver. Watch for this if anyone adds ad-hoc debugging code. |
| C08 | `MERGE`/CDC flow without explicit delete handling, fed by a CDC source with `REPLICA IDENTITY DEFAULT` | **Confirmed via live test against the local stack (2026-06-20), not just inferred.** No table sets `REPLICA IDENTITY FULL`, so Postgres only logs PK columns in the before-image of a DELETE (verified in Debezium's own log: `"UPDATE and DELETE events will contain previous values only for PK columns"`). `delete.handling.mode=rewrite` turns that into a Kafka record with the merge key populated and **every other field `NULL`** (`__deleted="true"` confirmed present in the registered Avro schema). `register_silver()` doesn't filter on `__op`/`__deleted` for the 10 generic domains, so `create_auto_cdc_flow()` (no `apply_as_deletes`) applies it as `WHEN MATCHED THEN UPDATE SET *` — **the Postgres DELETE doesn't remove the Silver/Gold row, it NULLs out every non-key column on it, permanently.** Never observed in a real run because `tests/load_to_postgres.py` never issues a `DELETE` — real gap, just unexercised. See `CLAUDE.md`'s matching entry for the full trace and the two candidate fixes (`REPLICA IDENTITY FULL` + `apply_as_deletes`, or quarantine on `__deleted='true'`). No `/design` decision made yet. |

## HIGH

| ID | Anti-pattern | Status in this project |
|---|---|---|
| H01 | JOIN without a predicate (implicit cross join) | All Gold joins in `register_gold_*` specify explicit `on=`/`join(..., col1 == col2, ...)`. |
| H02 | Full table scan on a partitioned table without a partition filter | Liquid Clustering (not partition-by) is the storage strategy here — `cluster_by` must equal `merge_key` (ADR-04, enforced by `tests/test_contracts.py::test_06`). |
| H03 | Schema without Delta/versioning | Every table in this project is Delta by default (Lakeflow-managed); no raw Parquet output table exists outside the `landing` Volume (which is a snapshot input, not an output). |
| H04 | Pipeline without data-quality tests | Every contract declares `quality.rules`; see `kb/data-quality.md` for what's actually enforced vs. declarative-only (`check: unique` gap). |
| H05 | Python UDF where native SQL/DataFrame functions would do | `pipelines/ubereats_pipeline.py` uses `get_json_object`, `coalesce`, `regexp_replace`, window functions — no Python UDFs anywhere in the pipeline. |
| H06 | High-cardinality partition key (>10k distinct values) | N/A in the literal partition sense (Liquid Clustering, not `PARTITION BY`) — but the same risk shape applies to `cluster_by` choice. Don't cluster by a column with near-row-level cardinality on a small table (e.g. don't cluster `order_items` by `order_item_id` alone without checking file-count impact at 110k rows). |
| H07 | Pipeline without idempotency | Explicitly designed for — `volume` mode's full materialized-view recompute and `kafka` mode's streaming-table model both make re-running side-effect-free (`docs/adr/007_pipeline_unification.md`'s 2026-06-19 addendum). |
| H08 | (duplicate of H07 in source material — kept as alias) | See H07. |
| H11 | Databricks `MERGE`/CDC flow without explicit delete handling | **Promoted to C08 (CRITICAL)** — confirmed via live test, not just a theoretical gap. See the CRITICAL table above. |
| H12 | Reading Unity Catalog without `catalog.schema.table` | Every table reference in `pipelines/ubereats_pipeline.py` is built via f-string with `CATALOG` (`f"{CATALOG}.bronze.{domain}"`) — no bare table name reads. |

## MEDIUM

| ID | Anti-pattern | Status in this project |
|---|---|---|
| M01 | Inconsistent naming (camelCase vs snake_case) | snake_case throughout contracts/schema/Python — no known violations. |
| M02 | Outdated or misleading comments | Caught one real instance this session — `CLAUDE.md`'s `check: unique` description claimed Silver-level anti-join enforcement the code doesn't do; corrected. Re-check comments against code whenever touching `dlt_adapter.py` or `register_silver()`. |
| M03 | KB without versioning/freshness markers | This KB (`.claude/kb/`) has no `updated_at`/`version` frontmatter on any file — low risk at current size (8 files), worth adding if the KB grows past ~15 files. |
| M04 | Magic numbers without a named constant | `MAX_OFFSETS_OVERRIDES = {"order_items": 5000}` and `DEFAULT_MAX_OFFSETS = 1000` are named module-level constants, not inline magic numbers — good. |
| M05 | `OPTIMIZE`/`VACUUM` without duration monitoring | Not yet run anywhere in this project (129k rows doesn't need it yet per "Dataset framing" in `CLAUDE.md`) — revisit if/when this stops being a microcosm-scale dataset. |
| M06 | Delta table without `TBLPROPERTIES` retention | `delta.enableChangeDataFeed`/`delta.autoOptimize.*` are set per-contract (`storage.properties`); explicit `delta.logRetentionDuration` is not set anywhere — acceptable at this scale, flag before any production cutover. |

## How to use this file

Check here before recommending a fix in this codebase — several of the "HIGH"/"CRITICAL"
items above describe patterns that look risky in isolation but are *intentional*
given this project's specific architecture (full Gold recompute, Liquid Clustering
instead of partitioning, batch-vs-stream split for `users`). Don't "fix" C01/H02/H06
without re-reading `kb/medallion.md` first — the obvious-looking fix is usually
wrong here.
