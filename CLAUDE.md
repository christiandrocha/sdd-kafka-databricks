# sdd-kafka-databricks v1.2.0

**Platform:** Uber Eats food delivery (Brazilian market)
**Pipeline:** JSON exports → PostgreSQL → Debezium → Kafka → Databricks → Unity Catalog → DABs

## Platform at a glance

| Metric | Value |
|---|---|
| Source systems | 4 (Kafka, MongoDB, MySQL, PostgreSQL/MSSQL) |
| Domains | 20 |
| JSON files | 100 |
| Total records | 129,353 |
| Largest table | order_items (110,001 — 85% of volume) |
| Hub table | orders (links all via CPF, CNPJ, driver_id, UUID) |
| Unity Catalog | ubereats_dev / ubereats_prod |
| Pipeline execution | One Lakeflow pipeline (`ubereats_pipeline`), all 3 targets (dev/prod/free_edition) — see below |
| Notebooks | 0 — all 8 legacy notebooks retired (v1.2.0); logic ported into `pipelines/ubereats_pipeline.py` |
| Silver domains | 11 (payment_current_state dropped — covered by gold_payment_lifecycle) |

## What changed from sdd-kafka-snowflake

| Component | sdd-kafka-snowflake | sdd-kafka-databricks |
|---|---|---|
| Destination | Snowflake Sink Connector | Databricks Structured Streaming |
| Transformation | dbt | Lakeflow Declarative Pipelines (one pipeline, all 3 targets) |
| Orchestration | Dagster | Databricks Asset Bundles (DABs) |
| Storage | Snowflake VARIANT | Delta Lake 3.1 + Liquid Clustering |
| Catalog | Snowflake schemas | Unity Catalog (ubereats_dev/prod) |
| IDE | Claude Code (basic) | Claude Code (AgentSpec full) |
| Data contracts | None | YAML per table (differentiator) |

## Critical architecture decisions

**Bronze = flat records via SMT (ExtractNewRecordState)**
The Debezium ExtractNewRecordState SMT IS used (`connectors/debezium.json`).
Bronze receives flat business fields + `__op` + `__source_ts_ms`, not the raw
Debezium envelope — there is no unwrap step in Silver. Topology is
unidirectional (JSON exports → Postgres → Debezium → Kafka), so the
audit-trail argument for skipping the SMT does not apply here. See ADR-02.

**DELETE handling — fix implemented 2026-06-20, verified live end-to-end (Postgres + Databricks) 2026-06-22**
Originally a confirmed NULL-corruption gap (C08): no table set `REPLICA
IDENTITY FULL`, so Postgres only logged primary-key columns in the
before-image of an `UPDATE`/`DELETE`, and `register_silver()` had no
`apply_as_deletes`, so a delete-rewrite row landed as `WHEN MATCHED THEN
UPDATE SET *` — NULLing every non-key column instead of removing the row.
Full design: `.claude/sdd/features/DESIGN_DELETE_HANDLING.md`. Fix shipped in
3 parts: (1) `sql/init.sql` now sets `REPLICA IDENTITY FULL` on all 20 source
tables (+ `scripts/migrate_replica_identity.sh` for already-running Postgres
instances) — **verified live** against the local stack: a test `DELETE` on
`payments` now produces a Kafka record with every real field populated
(`method`, `status`, `amount`, `currency`, `country`, ...), not just the merge
key, matching the preceding INSERT event's values exactly. (2)
`register_silver()`'s `create_auto_cdc_flow()` now passes
`apply_as_deletes=expr("__op = 'd'")`, so a delete-rewrite row actually
removes the target Silver row instead of upserting it. (3)
`register_silver_users()`'s `_prepped_users()`/`_dedup_by_cpf()` now keep
delete rows through the dedup window and exclude a `cpf_key` from
`silver.users` only if its *latest* event is a delete — previously the delete
row was dropped before dedup, so the user's last live state persisted forever
instead of being removed. **Verified live 2026-06-22** against the real `dev`
Databricks workspace: yes, `apply_as_deletes` needs `full_refresh=true` to
correctly reprocess already-ingested Bronze history (Databricks' public docs
never confirmed this either way) — after deploying the bundle, running the
pipeline once, then triggering `databricks pipelines start-update --full-refresh`,
`silver.payments` correctly shows 0 rows for the AT-001 test `payment_id`
(`bronze.payments` retains both its INSERT and DELETE events, each with full
real field values) and `gold.payments_by_status` sums to exactly the live
Postgres row count — confirming the delete propagates to Silver and Gold with
no NULL-corruption and no leaked row. `silver.orders` also converges to the
correct deduped count despite `bronze.orders` holding 2x raw events from an
unrelated historical re-snapshot, confirming ordinary updates still merge
correctly post-fix. AT-002 (non-key quarantine domain) and AT-003 (`users`
single-source delete) remain unexecuted — no test delete was performed against
`drivers` or `users_mongo`/`users_mssql` in this run. Separately, this run
surfaced that `source_mode=volume`'s Auto Loader tracks ingested files by path,
so re-running `scripts/export_kafka_to_volume.py` + `databricks fs cp --overwrite`
on the *same* `data.parquet` path is silently ignored on incremental runs —
only a `full_refresh` (or a uniquely-named file per export) picks up the new
content; flagged for `/design` review. See `.claude/kb/anti-patterns.md` (C08) and
`.claude/sdd/reports/BUILD_REPORT_DELETE_HANDLING.md` for the full trace.

**One Lakeflow pipeline for everything, all 3 targets (v1.2.0)**
`pipelines/ubereats_pipeline.py` (renamed from `bronze_silver_dlt.py`) loops
over `contracts/*.yml` and registers one `@dp.table` per Bronze domain (20)
and one Silver `@dp.table` + quarantine pair per generic Silver domain (10 of
the 11), then adds `silver_users`/`quarantine.users` (FULL OUTER JOIN of
`bronze.users_mongo`+`bronze.users_mssql`, ported from the retired
`pipeline_users.ipynb`) and all 6 Gold tables (`@dp.table` + `dp.read(silver_*)`,
ported from the retired `notebooks/cross_domain/gold_*.ipynb`) — 37 tables, one
DAG. `dev`, `prod`, and `free_edition` all reference the same pipeline resource
and the same 1-task Job, differing only by `variables:` (`catalog`,
`bronze_source_mode`, `landing_base`). `dlt.create_auto_cdc_flow()` (the
renamed `apply_changes()`) replaces the hand-written `MERGE INTO` for the 10
generic Silver domains' `merge_key` upsert; Gold/`silver_users` are full
batch-recompute `@dp.table` materialized views instead (no MERGE — a full
recompute is already what their old `MERGE INTO ... WHEN MATCHED UPDATE SET *`
amounted to, since each run re-aggregates over the complete Silver table).
`check: unique` (see below) is declared in the contract but excluded from
`contracts/dlt_adapter.py`'s quarantine predicate for the 10 generic Silver
domains — Structured Streaming can't express the needed cross-row aggregation
without a watermark. `register_silver_users()`'s hand-written `user_id`
duplicate check is the one place this kind of rule is actually enforced (a
batch `groupBy().count()` over `dp.read()`, not a stream), since `users` reads
Bronze in batch, not as a stream. See "Gold dimension joins" below and
`.claude/kb/data-quality.md` for the full mechanics. This
supersedes `ADR-006`'s "Explicitly NOT migrated" section (Gold, `silver_users`,
`free_edition`) — none of those exclusion reasons survived once Gold's logic
ported cleanly into `@dp.table` bodies and `free_edition` turned out to share
the same workspace (and therefore the same open Kafka-reachability question)
as `dev`/`prod`. All 8 legacy notebooks are retired. See
`docs/adr/006_lakeflow_migration.md` and `docs/adr/007_pipeline_unification.md`.

**Dataset framing**
129k records is an architectural microcosm, not a production volume.
The goal is validating correctness of MERGE idempotency, Data Contracts,
and Liquid Clustering alignment — not demonstrating Petabyte throughput.

**Bronze has two source_modes — volume (dev/free_edition, permanent) and kafka (prod, target state)**
`volume` is the correct, permanent mode for `dev` and `free_edition` in this
project, not a temporary workaround: both run on the same Databricks
workspace/serverless compute, which cannot reach the self-hosted Kafka broker
(outbound network is restricted to a fixed allowlist, not customizable outside
the Enterprise tier). `kafka` is `prod`'s target mode — documented for where
`prod` is headed once Kafka runs on infrastructure Databricks can actually
reach, not a confirmed-working setting today. `pipelines/ubereats_pipeline.py`'s
`register_bronze()` reads a pipeline-level `ubereats.source_mode` configuration
value: `volume` (`spark.read` batch off `/Volumes/<catalog>/landing/kafka_export/`,
`dev`/`free_edition`) or `kafka` (`spark.readStream`, `prod`). Populate the
Volume first with `scripts/export_kafka_to_volume.py` + `databricks fs cp`.
Both modes share the same `@dp.table` registration — idempotency comes from a
full materialized-view recompute every run (`volume`) or Lakeflow's incremental
streaming-table model (`kafka`), not from an explicit checkpoint, so re-running
in either mode never duplicates rows. See
`docs/adr/007_pipeline_unification.md`'s 2026-06-19 addendum for the decision
record.

**databricks.yml: one pipeline + one 1-task Job, identical across all 3 targets**
Free Edition only supports serverless compute — no `job_cluster_key`/
`new_cluster` anywhere in the file, for any target. DABs can't exclude a
root-level resource from one target
([databricks/cli#2872](https://github.com/databricks/cli/issues/2872)), so
each target (`dev`, `prod`, `free_edition`) still owns its own
`resources.pipelines.ubereats_pipeline`/`resources.jobs.ubereats_pipeline` —
but as of v1.2.0 both are identical aliases of one shared anchor pair
(`pipeline_resource`/`pipeline_task`), not three different shapes. Targets
differ only by `variables:` (`catalog`, `bronze_source_mode`, `landing_base`).
See `docs/adr/007_pipeline_unification.md`.

**Gold dimension joins must target a column enforced unique in Silver**
3 of the 6 Gold notebooks join a Silver dimension on a column that is not that
table's `merge_key` (`gold_user_behavior` → `silver.users.user_id`, real key
`cpf`; `gold_driver_performance` → `silver.drivers.driver_id`, real key
`uuid`; `gold_revenue_per_restaurant` → `silver.restaurants.cnpj`, real key
`uuid`). Nothing guaranteed those columns were unique, which already caused a
`DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` failure once
(`gold_user_behavior`). Two layers were designed, but only one actually
enforces today: (1) a contract quality-rule type, `check: unique`
(`contracts/drivers.yml`/`contracts/restaurants.yml`), declared and
structurally validated (`tests/test_contracts.py::test_09`) but **not enforced
at runtime** for these two — `contracts/dlt_adapter.py`'s
`quarantine_row_level_predicate()` explicitly excludes `check: unique` from the
row-level SQL it generates, because Structured Streaming rejects the cross-row
`COUNT(DISTINCT ...)` this would need, without a watermark, in `kafka` mode.
The one place uniqueness *is* actually enforced is `register_silver_users()`'s
hand-written `user_id` duplicate check — it can run a real
`groupBy("user_id").count()` because `users` reads Bronze in batch
(`dp.read()`, not `dp.read_stream()`), and duplicates are genuinely quarantined
with `_quarantine_reason="duplicate_user_id"`. (2) The `row_number()` guard
kept right before every affected Gold table's return statement is therefore
the **primary** protection for `driver_performance`/`revenue_per_restaurant`/
`user_behavior` against `driver_id`/`cnpj`/`user_id` duplicates — not
defense-in-depth backstopping a runtime quarantine check, since that check
never runs for the streaming-based generic Silver domains. `merge_key` itself
is never changed — it stays the real CDC identity (`uuid`/`cpf`), not the
column Gold happens to join on. See
`docs/adr/005_gold_dimension_join_integrity.md` and
`.claude/kb/data-quality.md`. Flagged for `/design` review: closing this gap
means either enforcing `check: unique` via a periodic batch reconciliation job
(the streaming path can't do it inline) or formally accepting the
`row_number()` guard as the permanent, sole mechanism and updating the ADR
accordingly.

## Unity Catalog structure

```
ubereats_dev/
├── bronze/      ← 20 tables (one per domain, flat post-SMT records)
├── silver/      ← 11 tables (cleansed + deduped + quality rules)
├── gold/        ← 6 cross-domain analytics tables
├── quarantine/  ← 11 tables (mirrors silver domains)
├── checkpoints/ ← operational only, no data tables — 2 Volumes (bronze, silver),
│                  provisioned by scripts/preflight_unity_catalog.sh. Unused as
│                  of v1.2.0: Lakeflow self-manages pipeline storage, so nothing
│                  in pipelines/ubereats_pipeline.py reads/writes these paths —
│                  left in place as a follow-up cleanup, not yet removed
└── landing/     ← 1 Volume (kafka_export) — Parquet snapshot of the 20 Kafka
                   topics, written by scripts/export_kafka_to_volume.py, read
                   by register_bronze() in source_mode=volume (dev, permanent —
                   same Volume also backs free_edition)

ubereats_prod/ ← same structure (source_mode=kafka, prod's target mode once
                 Kafka is reachable from Databricks — not yet verified, so
                 landing/ is unused only until that changes)
```

## Domain map (20 tables)

| Type | Table | Source | PK | Records |
|---|---|---|---|---|
| event | payment_events | kafka_events | event_id | 2,208 |
| event | gps_events | kafka_gps | gps_id | 7,350 |
| event | order_status | kafka_status | status_id | 4,176 |
| event | search_events | kafka_search | search_id | 202 |
| event | recommendations | mongodb_recommendations | event_id | 254 |
| fact | order_items | mongodb_items | order_item_id | 110,001 |
| entity | orders | kafka_orders | order_id | 405 |
| entity | payments | kafka_payments | payment_id | 260 |
| entity | routes | kafka_route | route_id | 410 |
| entity | receipts | kafka_receipts | receipt_id | 377 |
| entity | driver_shifts | kafka_shift | shift_id | 468 |
| entity | support_tickets | mongodb_support | ticket_id | 410 |
| entity | users_mongo | mongodb_users | uuid | 411 |
| entity | users_mssql | mssql_users | uuid | 288 |
| entity | restaurants | mysql_restaurants | uuid | 461 |
| entity | drivers | postgres_drivers | uuid | 354 |
| entity | products | mysql_products | product_id | 368 |
| entity | menu_sections | mysql_menu | menu_section_id | 362 |
| entity | ratings | mysql_ratings | rating_id | 327 |
| entity | inventory | postgres_inventory | stock_id | 261 |

## Silver domains (11 — registered in pipelines/ubereats_pipeline.py)

payment_events, orders, payments, users
(merge users_mongo + users_mssql by CPF), drivers, order_items,
driver_shifts, restaurants, order_status, search_events, recommendations

## Bronze-only domains (8 — feed Gold directly when needed)

gps_events, routes, receipts, support_tickets, products,
menu_sections, ratings, inventory

## Hub table — orders foreign keys

| Field | Type | Resolves to |
|---|---|---|
| user_key | CPF (000.000.000-00) | users_mongo.cpf / users_mssql.cpf |
| restaurant_key | CNPJ (00.000.000/0000-00) | restaurants.cnpj |
| driver_key | string | drivers.driver_id |
| payment_key | UUID | payments.payment_id |
| rating_key | UUID | ratings.uuid |

## Slash commands

| Command | Phase | Purpose |
|---|---|---|
| `/brainstorm` | 01 | Explore multi-source consolidation and domain model |
| `/define` | 02 | Review ACs and 20-domain clarity gate |
| `/design` | 03 | Review ADRs and domain_map |
| `/build` | 04 | Execute delegated agent tasks |
| `/ship` | 05 | Archive feature with lessons learned |

## Services and ports (local with override)

| Service | Port | URL |
|---|---|---|
| PostgreSQL | 5432 | `$DATABASE_URL` |
| Kafka | 9092 | `localhost:9092` |
| Schema Registry | 8081 | `http://localhost:8081` |
| Kafka Connect | 8083 | `http://localhost:8083` |
| Kafka UI | 8080 | `http://localhost:8080` |
| Prometheus | 9090 | `http://localhost:9090` |
| Grafana | 3001 | `http://localhost:3001` (admin/admin) |

## Load commands

```bash
# Dry-run (no DB needed)
python3 tests/load_to_postgres.py --data-dir tests/data/ --dry-run

# Initial load (80%)
python3 tests/load_to_postgres.py --data-dir tests/data/ --batch initial --db-url $DATABASE_URL

# Incremental load (20%)
python3 tests/load_to_postgres.py --data-dir tests/data/ --batch incremental --db-url $DATABASE_URL
```

## Continuous improvement

After every change:
- Update `.claude/05_implementation_log.md`
- Update `.claude/06_retrospective.md` at end of iteration
- Flag manifest divergences for /design review
