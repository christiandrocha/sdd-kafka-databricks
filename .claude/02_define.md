version: "1.0.0"
project: sdd-kafka-databricks
platform: "Uber Eats food delivery — Brazilian market"
last_updated: "2026-06-16"

# =============================================================================
# PROBLEM STATEMENT
# =============================================================================

problem: >
  Migrate the sdd-kafka-snowflake pipeline from Snowflake + dbt + Dagster to
  Databricks Structured Streaming + PySpark notebooks + DABs, while preserving
  the 20-domain Uber Eats CDC pipeline and introducing Data Contracts as a
  governance differentiator absent from both reference projects.

target_users:
  - Data engineers building production CDC pipelines
  - Recruiters evaluating Staff/Principal-level portfolio projects
  - Architects evaluating Databricks Medallion implementations

# =============================================================================
# SCOPE
# =============================================================================

in_scope:
  - PostgreSQL as CDC source (20 tables, WAL logical)
  - Debezium PostgreSQL Source Connector (SMT ExtractNewRecordState — flat records)
  - Kafka with Confluent Schema Registry (Avro, BACKWARD)
  - Databricks Structured Streaming (pipeline_bronze + pipeline_silver parametrized)
  - Unity Catalog: ubereats_dev / ubereats_prod (bronze/silver/gold/quarantine)
  - 6 cross-domain Gold notebooks (payment lifecycle, funnel, by_status, driver, restaurant, user)
  - Data Contracts YAML per table (20 contracts + loader + spark_schema + pydantic_models)
  - Databricks Asset Bundles (DABs) orchestration — targets dev and prod
  - Prometheus + Grafana for Kafka/Debezium monitoring
  - GitHub Actions CI/CD (lint + test_contracts + bundle validate + deploy)
  - AgentSpec .claude/ structure (agents, commands, kb, sdd)

out_of_scope:
  - Lakeflow Declarative Pipelines (DLT) — Structured Streaming is correct here
  - Apicurio Registry — Confluent Schema Registry is used
  - Bidirectional Debezium (JDBC Sink) — unidirectional only
  - Raw Debezium envelope in Bronze — superseded; ADR-02 uses the SMT instead (v1.0.1)
  - Spark metrics in Prometheus — DABs Notifications + System Tables cover Databricks
  - Real Petabyte-scale testing — 129k records is the architectural microcosm

# =============================================================================
# DOMAIN MAP
# =============================================================================

domains:
  total: 20
  silver_domains: 11
  bronze_only_domains: 8
  gold_models: 6

  silver_list:
    - payment_events
    - orders
    - payments
    - users           # merge users_mongo + users_mssql by CPF
    - drivers
    - order_items
    - driver_shifts
    - restaurants
    - order_status
    - search_events
    - recommendations

  bronze_only_list:
    - gps_events
    - routes
    - receipts
    - support_tickets
    - products
    - menu_sections
    - ratings
    - inventory

  gold_list:
    - gold_payment_lifecycle
    - gold_payment_funnel
    - gold_payments_by_status
    - gold_driver_performance
    - gold_revenue_per_restaurant
    - gold_user_behavior

# =============================================================================
# ACCEPTANCE CRITERIA
# =============================================================================

acceptance_criteria:

  - id: AC-01
    description: "dry-run: 100 files, 129,353 records, 0 errors"
    verification: "python3 tests/load_to_postgres.py --dry-run → 0 errors"
    cycle: load

  - id: AC-02
    description: "Initial load: 20 PostgreSQL tables populated (80 files)"
    verification: "SELECT COUNT(*) per table — all > 0"
    cycle: load

  - id: AC-03
    description: "Incremental load: no duplicates on re-load (upsert by PK)"
    verification: "Run --batch incremental twice → same row count"
    cycle: load

  - id: AC-04
    description: "20 Kafka topics + 20 Schema Registry subjects registered"
    verification: "kafka-topics.sh --list → 20 pg.public.* topics"
    cycle: core

  - id: AC-05
    description: "Bronze: 20 tables in ubereats_dev.bronze — append-only, raw envelope"
    verification: "SELECT COUNT(*) FROM ubereats_dev.bronze.payment_events > 0"
    cycle: core

  - id: AC-06
    description: "Silver MERGE INTO idempotent: re-run produces same row count"
    verification: "Run pipeline_silver twice → identical SELECT COUNT(*)"
    cycle: core

  - id: AC-07
    description: "silver.users: 0 duplicate CPFs (FULL OUTER JOIN merge)"
    verification: "SELECT cpf, COUNT(*) FROM ubereats_dev.silver.users GROUP BY 1 HAVING COUNT(*) > 1 → 0 rows"
    cycle: core

  - id: AC-08
    description: "Gold: 6 cross-domain models populated with JOIN fact × dimensions"
    verification: "SELECT COUNT(*) from each gold table > 0"
    cycle: core

  - id: AC-09
    description: "Unity Catalog: ubereats_dev with 4 schemas (bronze/silver/gold/quarantine)"
    verification: "SHOW SCHEMAS IN ubereats_dev → 4 schemas"
    cycle: core

  - id: AC-10
    description: "DABs: pipeline_bronze runs 20x, pipeline_silver runs 11x with correct parameters"
    verification: "databricks bundle validate → 31 tasks defined"
    cycle: core

  - id: AC-11
    description: "Data Contracts: all 20 YAML files syntactically valid"
    verification: "pytest tests/test_contracts.py → 0 failures"
    cycle: contracts

  - id: AC-12
    description: "Liquid Clustering: cluster_by aligned with merge_key in all Silver/Gold contracts"
    verification: "test_contracts.py: test_cluster_by_aligns_with_merge_key passes"
    cycle: contracts

  - id: AC-13
    description: "CI: lint + test_contracts + bundle validate pass on valid PR"
    verification: "GitHub Actions ci.yml → green"
    cycle: cicd

  - id: AC-14
    description: "docker-compose config valid"
    verification: "docker compose config --quiet → exit 0"
    cycle: infra

  - id: AC-15
    description: ".env never committed (.gitignore blocks it)"
    verification: "git add .env → rejected by .gitignore"
    cycle: cicd

  - id: AC-16
    description: "Full stack boots healthy (PostgreSQL, Kafka, Schema Registry, Kafka Connect)"
    verification: "docker compose up --wait → all services healthy"
    cycle: infra

  - id: AC-17
    description: "Prometheus: kafka-jmx + kafka-exporter targets UP"
    verification: "curl http://localhost:9090/targets → both UP"
    cycle: observability

  - id: AC-18
    description: "Grafana: Kafka consumer lag dashboard with 20-topic visibility"
    verification: "http://localhost:3001 → kafka dashboard loads with lag panels"
    cycle: observability

# =============================================================================
# CLARITY GATE
# =============================================================================

clarity_gate:
  score: 15
  minimum: 12

  breakdown:
    problem:
      score: 3
      note: "Clear migration from Snowflake to Databricks with specific technical rationale"
    users:
      score: 3
      note: "Data engineers + portfolio evaluators identified with distinct needs"
    goals:
      score: 3
      note: "18 measurable ACs with verification commands"
    success:
      score: 3
      note: "AC-01 (dry-run), AC-06 (MERGE idempotency), AC-11 (contracts) are testable"
    scope:
      score: 3
      note: "Explicit in_scope and out_of_scope with rationale for each exclusion"

# =============================================================================
# CONSTRAINTS
# =============================================================================

constraints:
  databricks_runtime: "14.1+"     # Liquid Clustering requires Delta Lake 3.1
  kafka_mode: "KRaft"             # No Zookeeper
  schema_registry: "Confluent"    # Not Apicurio
  smt: "none"                     # Bronze = raw Debezium envelope
  notebooks: 2                    # Parametrized — not 60 static
  bronze_write_mode: "append"     # Immutable raw envelope
  silver_write_mode: "merge"      # MERGE INTO ON merge_key
  cluster_by_equals_merge_key: true  # ADR-04 enforcement
