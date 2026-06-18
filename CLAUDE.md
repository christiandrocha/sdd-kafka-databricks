# sdd-kafka-databricks v1.0.0

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
| Notebooks | 2 parametrized (bronze + silver) + 6 cross-domain gold |
| Silver domains | 11 (payment_current_state dropped — covered by gold_payment_lifecycle) |

## What changed from sdd-kafka-snowflake

| Component | sdd-kafka-snowflake | sdd-kafka-databricks |
|---|---|---|
| Destination | Snowflake Sink Connector | Databricks Structured Streaming |
| Transformation | dbt | Parametrized PySpark notebooks |
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

**2 parametrized notebooks, not 60**
pipeline_bronze.ipynb and pipeline_silver.ipynb receive table_name, kafka_topic,
contract_path as widgets. DABs orchestrates them 20x (bronze) and 11x (silver).
See ADR-03.

**Dataset framing**
129k records is an architectural microcosm, not a production volume.
The goal is validating correctness of MERGE idempotency, Data Contracts,
and Liquid Clustering alignment — not demonstrating Petabyte throughput.

**Bronze has two source_modes — kafka (default) and volume (Free Edition)**
Databricks Free Edition's serverless compute can't reach a self-hosted Kafka
broker (outbound network is restricted to a fixed allowlist, not customizable
outside the Enterprise tier). `pipeline_bronze.ipynb` accepts `source_mode`:
`kafka` (default, `spark.readStream` + checkpoint, used by `dev`/`prod`) or
`volume` (`spark.read` batch off `/Volumes/<catalog>/landing/kafka_export/`,
used by the `free_edition` DABs target). Populate the Volume first with
`scripts/export_kafka_to_volume.py` + `databricks fs cp`. Both modes share
the same contract/DDL/MERGE logic — idempotency comes from
`MERGE INTO ... WHEN NOT MATCHED`, not from the checkpoint, so re-running in
either mode never duplicates rows. See ADR-05.

**databricks.yml: classic compute (dev/prod) vs. serverless (free_edition)**
Free Edition only supports serverless compute — no `job_cluster_key`/
`new_cluster` allowed. DABs can't exclude a root-level resource from one
target ([databricks/cli#2872](https://github.com/databricks/cli/issues/2872)),
so the 37 tasks are defined once as YAML anchors (`task_definitions`) and
each target (`dev`, `prod`, `free_edition`) owns its own
`resources.jobs.ubereats_pipeline`, referencing either `classic_tasks` (with
`job_cluster_key`) or `serverless_tasks` (without). See ADR-06.

## Unity Catalog structure

```
ubereats_dev/
├── bronze/      ← 20 tables (one per domain, flat post-SMT records)
├── silver/      ← 11 tables (cleansed + deduped + quality rules)
├── gold/        ← 6 cross-domain analytics tables
├── quarantine/  ← 11 tables (mirrors silver domains)
├── checkpoints/ ← operational only, no data tables — 2 Volumes (bronze, silver)
│                  for Structured Streaming checkpoint locations; provisioned
│                  by scripts/preflight_unity_catalog.sh, not by any notebook
└── landing/     ← 1 Volume (kafka_export) — Parquet snapshot of the 20 Kafka
                   topics, written by scripts/export_kafka_to_volume.py, read
                   by pipeline_bronze.ipynb in source_mode=volume (Free Edition)

ubereats_prod/ ← same structure (source_mode=kafka only — landing/ unused)
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

## Silver domains (11 — have dedicated Silver notebook)

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
