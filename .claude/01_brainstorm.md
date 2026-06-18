# 01 — Brainstorm Prompt
# sdd-kafka-databricks
# Phase 0: Exploration of multi-source consolidation and Databricks migration
# Generated from: design session with 5 validation rounds (Staff/Principal level)

---

## Section 1 — Origin and motivation

**Starting point:** sdd-kafka-snowflake v4.2.0 — a production-grade CDC pipeline
for the Uber Eats Brazilian market with 20 domains, 129,353 records, Kafka + Debezium
+ Snowflake + dbt + Dagster.

**Migration goal:** Replace Snowflake (Sink + dbt + Dagster) with Databricks
(Structured Streaming + PySpark notebooks + DABs) while keeping the entire
Kafka/Debezium/PostgreSQL infrastructure identical.

**Why Databricks instead of Snowflake:**
- Unity Catalog for governance, lineage, and RBAC in one place
- Spark-native streaming (no Snowpipe latency, no connector dependency)
- Delta Lake Liquid Clustering replaces manual ZORDER BY
- Databricks Asset Bundles (DABs) for GitOps-native orchestration
- Serverless compute with trigger(availableNow=True) — scales to zero

---

## Section 2 — Dataset analysis

100 JSON files, 4 source systems, 20 domains, 129,353 records:

```
kafka_events       →  payment_events    (2,208 records)
kafka_gps          →  gps_events        (7,350 records)
kafka_status       →  order_status      (4,176 records)
kafka_search       →  search_events     (202 records)
kafka_orders       →  orders            (405 records)
kafka_payments     →  payments          (260 records)
kafka_route        →  routes            (410 records)
kafka_receipts     →  receipts          (377 records)
kafka_shift        →  driver_shifts     (468 records)
mongodb_items      →  order_items       (110,001 records — 85% of total)
mongodb_recommendations → recommendations (254 records)
mongodb_support    →  support_tickets   (410 records)
mongodb_users      →  users_mongo       (411 records)
mssql_users        →  users_mssql       (288 records)
mysql_restaurants  →  restaurants       (461 records)
mysql_products     →  products          (368 records)
mysql_menu         →  menu_sections     (362 records)
mysql_ratings      →  ratings           (327 records)
postgres_drivers   →  drivers           (354 records)
postgres_inventory →  inventory         (261 records)
```

**Key findings:**
- order_items is 85% of volume → needs separate handling (larger maxOffsetsPerTrigger)
- orders is the hub table: CPF (user), CNPJ (restaurant), driver_id, payment UUID, rating UUID
- users_mongo and users_mssql share CPF business key → require Silver merge
- payment_events has JSONB nested event field → requires PARSE handling
- order_status has JSONB nested status field → same pattern

---

## Section 3 — Architecture decisions explored

### Decision A: SMT ExtractNewRecordState in Debezium connector
**Explored:** Use SMT to flatten Debezium envelope before publishing to Kafka.
**Rejected:** Destroys audit trail (before, txId, lsn), prevents replay fidelity.
**Chosen:** No SMT. Bronze persists raw envelope. Silver does unwrap via Spark.
**Cost accepted:** ~5-10% more compute in Silver — deliberate for financial data.
See ADR-02.

### Decision B: 60 notebooks (one per domain per layer) vs 2 parametrized
**Explored:** Create bronze_payment_events.ipynb, bronze_orders.ipynb... (60 files)
**Rejected:** Violates DRY. Logic change requires editing 20 files.
**Chosen:** pipeline_bronze.ipynb + pipeline_silver.ipynb with dbutils.widgets.
DABs orchestrates each 20x/12x with domain-specific parameters.
See ADR-03.

### Decision C: Apicurio Registry vs Confluent Schema Registry
**Explored:** Apicurio (open source, no license) with PostgreSQL backend.
**Rejected after analysis:** Added Apicurio in-memory loss risk, Groovy filter complexity,
bidirectional Debezium loop problem.
**Chosen:** Confluent Schema Registry — battle-tested, identical to sdd-kafka-snowflake,
Debezium native support, no additional containers.

### Decision D: Bidirectional Debezium (Kafka→PG→Kafka) vs unidirectional
**Explored:** JDBC Sink → PostgreSQL → CDC Source (bidirectional).
**Rejected:** Creates infinite replication loop risk, requires complex Groovy filter,
_source column anti-loop mechanism. Over-engineering for this problem.
**Chosen:** Unidirectional (same as sdd-kafka-snowflake): load_to_postgres.py populates
PostgreSQL directly. Debezium CDC Source reads WAL → Kafka → Databricks.

### Decision E: Liquid Clustering key alignment
**Discovered:** cluster_by columns MUST match MERGE ON clause for Databricks to
use clustering for file pruning during MERGE. Misalignment = full table scan.
See ADR-04.

---

## Section 4 — Features removed (YAGNI)

- Apicurio Registry with PostgreSQL backend → replaced by Confluent Schema Registry
- Bidirectional Debezium with JDBC Sink → replaced by unidirectional load_to_postgres.py
- 60 static notebooks → replaced by 2 parametrized notebooks
- Groovy Filter Transform for anti-loop → not needed (no bidirectional topology)
- _source column in PostgreSQL → not needed
- Lakeflow Declarative Pipelines (DLT) → Structured Streaming is correct for this use case

---

## Section 5 — Unity Catalog structure decided

```
Catalog: ubereats_dev / ubereats_prod  (environment-based — Databricks best practice)
Schemas: bronze / silver / gold / quarantine  (Medallion layers)
Tables:  20 bronze + 12 silver + 6 gold + 12 quarantine = 50 total
```

Environment-based catalog + layer-based schemas is the "gold standard" UC pattern
per Databricks official documentation and community consensus (Jan 2026).

---

## Section 6 — Data contracts as differentiator

Neither sdd-kafka-snowflake nor ai-uber-eats have data contracts.
This project introduces YAML contracts per table with:
- Schema (types, nullability)
- Quality rules (not_null, allowed_values, not_future — with on_failure, severity, scope)
- Delta properties (cluster_by, enableChangeDataFeed, compression)
- schema_evolution policy
- merge_key aligned with cluster_by

loader.py + spark_schema.py + pydantic_models.py generated from contracts.
test_contracts.py validates YAML consistency before any code runs.

---

## Section 7 — Observability scope (realistic)

Prometheus monitors Kafka + Debezium only (same as sdd-kafka-snowflake):
- Consumer lag per topic (20 topics)
- Connector health (Debezium Source)
- JMX metrics (broker, network, requests)
- kafka-exporter for consumer group lag

Databricks monitoring: DABs Notifications (native, zero config) + System Tables.
NOT attempting to push Spark metrics to Prometheus in managed Databricks environment.

---

## Section 8 — Portfolio positioning

**What this demonstrates simultaneously:**
1. Data engineering: CDC multi-domain, Structured Streaming, Medallion, Liquid Clustering
2. Software engineering: Data Contracts, ADRs, DRY notebooks, CI/CD, contract tests
3. Methodology: AgentSpec SDD with Claude Code, 5-phase workflow, 58 specialized agents
4. Scale: 20 domains, 129k records, 4 heterogeneous sources, cross-domain analytics

**Dataset volume narrative (for interviews):**
"129k records is an architectural microcosm. The goal is validating correctness —
MERGE idempotency, Data Contract governance, Liquid Clustering alignment — in a
controlled low-cost environment. The design scales horizontally when real volume arrives."

**Key differentiators vs market:**
- Data Contracts (YAML → StructType → Pydantic) — rare in portfolios
- ADR-aligned Liquid Clustering (cluster_by = merge_key) — senior-level detail
- AgentSpec .claude/ structure — AI-Native Engineer signal
- Parametrized notebooks via DABs — DRY engineering applied to data
