# 04 — Build Delegation
# sdd-kafka-databricks v1.0.0
# Scope: 20 domains, 2 parametrized notebooks, 6 cross-domain gold, 20 contracts

---

## How to use this file

Each agent below is responsible for a specific slice of the build.
Reference the spec (02_define.spec.yaml) and manifest (03_design.manifest.json)
before starting each task. Update 05_implementation_log.md after each session.

---

## Agent 1 — infra-base

**Agent:** `data-platform-engineer`
**KB domains:** streaming, kafka-cdc

**Scope:**
- `docker-compose.yml` — PostgreSQL 16 (WAL logical), Kafka KRaft, Confluent Schema Registry,
  Kafka Connect (Debezium), Kafka UI, Prometheus, Grafana, kafka-exporter
- `docker-compose.override.yml` — local dev port bindings
- `Dockerfile.connect` — Debezium PostgreSQL Source plugin
- `.env.example` — all required variables

**Constraints:**
- PostgreSQL: `wal_level=logical`, `max_replication_slots=10`, `max_wal_senders=10`
- Kafka: KRaft mode, no Zookeeper
- Schema Registry: Confluent (port 8081)
- No Snowflake Sink connector (this is the key difference from sdd-kafka-snowflake)
- All services have healthchecks; Kafka Connect depends on postgres + broker + schema-registry

---

## Agent 2 — postgres

**Agent:** `data-platform-engineer`
**KB domains:** kafka-cdc

**Scope:**
- `scripts/init.sql` — 20 tables with correct schemas (copy from sdd-kafka-snowflake with adjustments)
- `scripts/register_connectors.sh`
- `scripts/set_compatibility.sh`

**Special cases from sdd-kafka-snowflake v4.1.0:**
- `payment_events.event` — JSONB column (nested event_name + timestamp)
- `order_status.status` — JSONB column (nested status_name + timestamp)
- `receipts` — no dt_current_timestamp → uses receipt_generated_at
- `inventory` — no dt_current_timestamp → uses last_updated
- `search_events` — no dt_current_timestamp → uses search_timestamp (or timestamp)
- `ratings.rating_id` — INTEGER (not UUID, real data has integers)
- `driver_shifts.issues_reported` — VARCHAR(100) (not INTEGER, contains strings)
- `users_mongo.cpf` and `users_mssql.cpf` — NOT UNIQUE (CPF may repeat across snapshot exports)
- `orders.driver_key` — must be consistent with `drivers.driver_id` (integer, not alphanumeric)

**Publication:**
```sql
CREATE PUBLICATION dbz_publication FOR ALL TABLES;
```

---

## Agent 3 — kafka-stack

**Agent:** `streaming-engineer`
**KB domains:** kafka-cdc, schema-registry

**Scope:**
- `connectors/debezium.json` — 20 tables in table.include.list

**Connector config:**
```json
{
  "table.include.list": "public.payment_events,public.orders,...(20 tables)",
  "transforms": "unwrap",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.add.fields": "op,source.ts_ms",
  "transforms.unwrap.add.fields.prefix": "__",
  "transforms.unwrap.drop.tombstones": "false",
  "value.converter": "io.confluent.connect.avro.AvroConverter"
}
```

**WAIT — Critical:**
The debezium connector USES SMT ExtractNewRecordState because the Bronze notebook
reads from Kafka (not from a raw topic). The Bronze notebook persists the post-SMT
flat record WITH __op and __source_ts_ms fields. This is the sdd-kafka-snowflake
pattern adapted for Databricks.

**Correction from earlier design discussion:**
The decision to keep raw envelope (no SMT) was for a bidirectional topology.
In this unidirectional topology (load_to_postgres → PostgreSQL → Debezium → Kafka → Databricks),
using SMT is correct: it simplifies the Bronze notebook significantly and matches
the sdd-kafka-snowflake proven pattern. Bronze still receives consistent flat records
with __op and __source_ts_ms metadata.

---

## Agent 4 — data-contracts

**Agent:** `data-contracts-engineer`
**KB domains:** data-quality, spark

**Scope:**
- `contracts/{table}.yml` — 20 YAML contracts (one per domain)
- `contracts/loader.py` — parse + validate YAMLs (no external deps)
- `contracts/spark_schema.py` — generate StructType + TBLPROPERTIES from YAML
- `contracts/pydantic_models.py` — Pydantic v2 models for load_to_postgres validation
- `tests/test_contracts.py` — validate YAML consistency

**Contract schema:**
Each YAML must include:
- `table.name`, `table.layer`, `table.merge_key`
- `schema` — list of {name, type, nullable}
- `quality.rules` — list of {field, check, values?, on_failure, severity, scope}
- `storage.cluster_by`, `storage.compression`, `storage.properties`
- `schema_evolution` — {new_fields, removed_fields, type_changes}

**test_contracts.py validates:**
- All YAMLs syntactically valid
- quality.rules reference existing schema fields
- allowed_values are non-empty lists
- cluster_by is subset of schema fields
- merge_key is in cluster_by (ADR-04)

---

## Agent 5 — spark-bronze

**Agent:** `spark-specialist`
**KB domains:** spark, medallion, kafka-cdc

**Scope:**
- `notebooks/pipeline_bronze.ipynb` — parametrized Bronze notebook

**Widgets:**
```python
dbutils.widgets.text("table_name",           "payment_events")
dbutils.widgets.text("kafka_topic",          "pg.public.payment_events")
dbutils.widgets.text("kafka_bootstrap",      "localhost:9092")
dbutils.widgets.text("schema_registry_url",  "http://localhost:8081")
dbutils.widgets.text("bronze_table",         "ubereats_dev.bronze.payment_events")
dbutils.widgets.text("checkpoint_path",      "/Volumes/ubereats_dev/checkpoints/bronze/payment_events")
dbutils.widgets.text("max_offsets",          "1000")
dbutils.widgets.text("starting_offsets",     "earliest")
dbutils.widgets.text("contract_path",        "contracts/payment_events.yml")
```

**Logic:**
1. Create table DDL (CREATE TABLE IF NOT EXISTS) from contract
2. readStream from Kafka topic
3. Parse Avro with schema registry address
4. Select fields + add _ingested_at + _ingested_date + _source_file metadata
5. Filter to non-null PK records
6. MERGE INTO bronze table (whenNotMatchedInsertAll only — Bronze never updates)
7. trigger(availableNow=True)

**Special handling:**
- payment_events and order_status: JSONB event/status fields → PARSE_JSON in Bronze
- order_items: maxOffsetsPerTrigger=5000 (vs 1000 for others)

---

## Agent 6 — spark-silver

**Agent:** `spark-specialist`
**KB domains:** spark, medallion, data-quality

**Scope:**
- `notebooks/pipeline_silver.ipynb` — parametrized Silver notebook

**Widgets:**
```python
dbutils.widgets.text("table_name",       "payment_events")
dbutils.widgets.text("bronze_table",     "ubereats_dev.bronze.payment_events")
dbutils.widgets.text("silver_table",     "ubereats_dev.silver.payment_events")
dbutils.widgets.text("quarantine_table", "ubereats_dev.quarantine.payment_events")
dbutils.widgets.text("contract_path",    "contracts/payment_events.yml")
dbutils.widgets.text("checkpoint_path",  "/Volumes/ubereats_dev/checkpoints/silver/payment_events")
```

**Logic:**
1. Load contract from contract_path
2. Read Bronze (incremental by _ingested_date)
3. Apply quality rules from contract (not_null, allowed_values, not_future)
4. Route invalid records to quarantine
5. MERGE INTO Silver: WHEN MATCHED AND newer ts → UPDATE, WHEN NOT MATCHED → INSERT
6. Liquid Clustering: cluster_by from contract (ADR-04)

**Silver users special case:**
- Handled by a separate users notebook (not parametrized pipeline_silver)
- FULL OUTER JOIN users_mongo + users_mssql by cpf_normalized
- materialized = table (full refresh, ~700 records)

---

## Agent 7 — spark-gold

**Agent:** `medallion-architect`
**KB domains:** spark, medallion, data-modeling

**Scope:**
- 6 cross-domain Gold notebooks

**Pattern for each:**
- Read from Silver (never Bronze directly — strict medallion lineage)
- JOIN fact × dimension tables with explicit business key alignment
- MERGE INTO Gold by domain merge_key
- Liquid Clustering by merge_key

**Gold notebooks and their JOINs:**
- `gold_payment_lifecycle` — silver.payment_events → lifecycle per payment_id
- `gold_payment_funnel` — silver.payment_events → conversion funnel by event_name
- `gold_payments_by_status` — silver.payments → aggregation by status
- `gold_driver_performance` — silver.driver_shifts × silver.orders × silver.drivers
- `gold_revenue_per_restaurant` — silver.order_items × silver.orders × silver.restaurants (CNPJ join)
- `gold_user_behavior` — silver.search_events × silver.recommendations × silver.users (CPF join)

---

## Agent 8 — orchestration

**Agent:** `data-platform-engineer`
**KB domains:** cicd, databricks

**Scope:**
- `databricks.yml` — DABs with targets dev + prod

**Task structure:**
```yaml
tasks:
  # Bronze: 20 tasks (pipeline_bronze.ipynb with domain-specific params)
  - task_key: bronze_payment_events
    notebook_task:
      notebook_path: notebooks/pipeline_bronze.ipynb
      base_parameters:
        table_name: payment_events
        kafka_topic: pg.public.payment_events
        bronze_table: "{{var.catalog}}.bronze.payment_events"
        max_offsets: "1000"

  # order_items gets larger buffer
  - task_key: bronze_order_items
    notebook_task:
      base_parameters:
        max_offsets: "5000"

  # Silver: 12 tasks (pipeline_silver.ipynb)
  # Gold: 6 tasks (cross_domain notebooks, depend on Silver tasks)
```

---

## Agent 9 — cicd

**Agent:** `ci-cd-specialist`
**KB domains:** cicd

**Scope:**
- `.github/workflows/ci.yml` — lint (ruff + yamllint) + test_contracts + bundle validate
- `.github/workflows/deploy.yml` — databricks bundle deploy on merge to main
- `Makefile` — up, down, produce-initial, produce-incremental, lint, test, deploy-dev, deploy-prod
- `.gitignore`
- `pyproject.toml`

---

## Agent 10 — observability

**Agent:** `data-platform-engineer`
**KB domains:** observability

**Scope:**
- `observability/prometheus/prometheus.yml`
- `observability/prometheus/alert_rules.yml` — KafkaConsumerLagHigh, ConnectorTaskFailed, BrokerDown
- `observability/grafana/dashboards/kafka.json` — consumer lag (20 topics), messages in, bytes in
- `observability/grafana/dashboards/kafka_connect.json` — connector status
- `observability/jmx/kafka-jmx-exporter.yml`

**Scope clarity (from design session):**
Prometheus monitors Kafka + Debezium ONLY.
Databricks monitoring: DABs Notifications (native) + System Tables.
Do NOT attempt to push Spark/Databricks metrics to this Prometheus instance.

---

## Build order

```
1. infra-base (docker-compose, Dockerfile)
2. postgres (init.sql, scripts)
3. kafka-stack (connectors/debezium.json)
4. data-contracts (all 20 contracts + loaders + tests)
5. spark-bronze (pipeline_bronze.ipynb)
6. spark-silver (pipeline_silver.ipynb + users special case)
7. spark-gold (6 cross-domain notebooks)
8. orchestration (databricks.yml)
9. cicd (.github/workflows, Makefile)
10. observability (prometheus, grafana)
```
