# sdd-kafka-databricks

![CI](https://github.com/christiandrocha/sdd-kafka-databricks/workflows/CI/badge.svg)
![Python](https://img.shields.io/badge/python-3.11-blue)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.1-orange)
![Databricks](https://img.shields.io/badge/Databricks-DBR%2014.1-red)
![Kafka](https://img.shields.io/badge/Kafka-3.7%20KRaft-black)
![License](https://img.shields.io/badge/license-MIT-green)

---

## TL;DR

A food-delivery company's data is scattered across four different systems —
a live order stream, a customer database, a restaurant catalog, a driver
registry — each speaking its own language and updating on its own schedule.
This project builds the pipeline that listens to every change the moment it
happens and lands it, cleaned and trustworthy, in one place business teams
can query with confidence.

It's a working, end-to-end simulation of that pipeline for the Uber Eats
Brazilian market: **20 data domains, 4 source systems, 129k records**,
streamed in real time from PostgreSQL through Kafka into Databricks, with
automated data-quality enforcement at every step.

---

## The Problem

Food delivery platforms generate millions of events across heterogeneous systems —
Kafka streams, MongoDB documents, MySQL catalogs, MSSQL registries. Without a
governed, streaming-native pipeline, these domains remain silos.

This project builds a **production-grade CDC streaming pipeline** for the Uber Eats
Brazilian market: 20 domains, 4 source systems, 129k records flowing from PostgreSQL
through Debezium and Kafka into Databricks Unity Catalog via the Medallion Architecture.

> **Dataset framing:** 129k records is an architectural microcosm, not a production volume.
> The goal is validating MERGE idempotency, Data Contract governance, and Liquid Clustering
> alignment in a controlled environment — ensuring the design scales horizontally when
> real volume arrives.

---

## Architecture

```
[100 JSON files — 20 domains — 129,353 records]
                    │
                    ▼
        load_to_postgres.py
        80% initial (op='r') + 20% incremental (op='c')
                    │
                    ▼
        PostgreSQL 16 — 20 tables
        WAL: wal_level=logical
        Publication: dbz_publication
                    │
                    ▼
        Debezium PostgreSQL Source
        SMT: ExtractNewRecordState
        Adds: __op, __source_ts_ms
        Format: Avro + Confluent Schema Registry
                    │
                    ▼
        Kafka KRaft — 20 topics (pg.public.{table})
                    │
                    ▼
        Databricks Structured Streaming
        trigger(availableNow=True) — scales to zero
        2 parametrized notebooks via DABs (20x bronze + 11x silver)
                    │
         ┌──────────┼──────────┐
         ▼          ▼          ▼
      Bronze     Silver      Gold
   (20 tables)(11 tables)(6 tables)
                    │
                    ▼
               Quarantine
              (11 tables)
```

---

## Stack

| Layer | Technology | Decision |
|---|---|---|
| Data loading | `load_to_postgres.py` | Unified PostgreSQL from 4 source systems |
| Database | PostgreSQL 16 (Docker) | WAL logical for Debezium CDC |
| CDC | Debezium 2.x + SMT | Flat records with __op + __source_ts_ms |
| Broker | Kafka 3.7 KRaft | No Zookeeper — modern standard |
| Schema | Confluent Schema Registry | Avro + BACKWARD compatibility |
| Streaming | Databricks Structured Streaming | trigger(availableNow=True) |
| Storage | Delta Lake 3.1 | Liquid Clustering + MERGE INTO |
| Catalog | Unity Catalog | ubereats_dev / ubereats_prod |
| Orchestration | Databricks Asset Bundles | GitOps-native, targets dev/prod/free_edition |
| Contracts | YAML per table | loader + spark_schema + pydantic |
| Alerting | DABs Notifications | Zero-config job alerting |
| CI/CD | GitHub Actions | lint + contracts + bundle validate |
| IDE | Claude Code + AgentSpec | 5-phase SDD, 58 specialized agents |

---

## Engineering Decisions & Tradeoffs

| Decision | Alternative Considered | Why This |
|---|---|---|
| 2 parametrized notebooks | 60 static notebooks | DRY principle — single change point for all 20 domains |
| Debezium SMT (flat records) | Raw envelope in Bronze | Matches proven sdd-kafka-snowflake pattern; simpler Silver |
| MERGE INTO (idempotent) | Append-only streaming | Exactly-once semantics without Kafka transactions |
| Liquid Clustering (cluster_by = merge_key) | ZORDER BY static | Incremental, no rewrite; validated by test_contracts.py |
| Confluent Schema Registry | Apicurio | Battle-tested Debezium integration, no additional containers |
| Unidirectional topology | Bidirectional JDBC Sink | Eliminates loop risk; sources are snapshot exports |
| trigger(availableNow=True) | Continuous streaming | Scales to zero — no always-on cluster cost |
| YAML Data Contracts | Schema hardcoded in code | Single source of truth for schema, quality rules, Delta props |
| Dual `source_mode` (`kafka` / `volume`) | Enterprise-tier networking | Databricks Free Edition's serverless compute can't reach a self-hosted broker — built a Volume-backed batch fallback that shares the same MERGE/contract logic, so the same notebook runs on a $0 tier and a real cluster |

> Full rationale documented as ADRs in [`docs/adr/`](docs/adr/) (4 canonical write-ups); the
> Free Edition constraints above are tracked directly in `CLAUDE.md` (ADR-05, ADR-06).

---

## Data Contracts — The Differentiator

Neither reference projects (sdd-kafka-snowflake, ai-uber-eats) have data contracts.
This project introduces 20 YAML contracts as the single source of truth:

```yaml
# contracts/payment_events.yml (excerpt)
table:
  name: payment_events
  layer: silver
  merge_key: event_id

schema:
  - name: event_id
    type: string
    nullable: false
  - name: event_name
    type: string
    nullable: false

quality:
  rules:
    - field: event_name
      check: allowed_values
      values: [created, authorized, captured, succeeded, settled, closed]
      on_failure: quarantine
      severity: critical

storage:
  cluster_by: [event_id, event_ts]  # aligned with merge_key (ADR-04)
  properties:
    delta.enableChangeDataFeed: true
```

`test_contracts.py` validates that every contract is consistent before any code runs:
- All YAMLs syntactically valid
- Quality rules reference existing schema fields
- `cluster_by` is a subset of schema fields
- `merge_key` is in `cluster_by` (ADR-04 enforcement)

---

## Unity Catalog Structure

```
ubereats_dev/                     ← CATALOG (dev)
├── bronze/                       ← SCHEMA — 20 tables
│   ├── payment_events            ← raw flat records from Debezium
│   ├── orders                    ← hub table (CPF/CNPJ/driver_id keys)
│   └── [18 more domains]
├── silver/                       ← SCHEMA — 11 tables
│   ├── payment_events            ← MERGE + quality rules + Liquid Cluster (event_id, event_ts)
│   ├── users                     ← FULL OUTER JOIN users_mongo + users_mssql by CPF
│   └── [9 more domains]
├── gold/                         ← SCHEMA — 6 cross-domain analytics tables
│   ├── payment_lifecycle         ← lifecycle summary per payment_id
│   ├── driver_performance        ← earnings + delivery metrics per driver
│   ├── revenue_per_restaurant    ← revenue by CNPJ (fact × dimension JOIN)
│   └── [3 more models]
├── quarantine/                   ← SCHEMA — 11 tables (mirrors Silver)
├── checkpoints/                  ← 2 Volumes — Structured Streaming checkpoint locations
└── landing/                      ← 1 Volume (kafka_export) — Parquet snapshot of the 20
                                     Kafka topics, used by Bronze when source_mode=volume
                                     (Databricks Free Edition; see ADR-05)

ubereats_prod/                    ← CATALOG (prod) — same structure, source_mode=kafka only
```

---

## Domain Map (20 tables)

| Type | Table | Source | Records |
|---|---|---|---|
| event | payment_events | Kafka | 2,208 |
| event | gps_events | Kafka | 7,350 |
| event | order_status | Kafka | 4,176 |
| event | search_events | Kafka | 202 |
| event | recommendations | MongoDB | 254 |
| fact | order_items | MongoDB | **110,001** (85% of total) |
| entity | orders | Kafka | 405 |
| entity | payments | Kafka | 260 |
| entity | routes | Kafka | 410 |
| entity | receipts | Kafka | 377 |
| entity | driver_shifts | Kafka | 468 |
| entity | support_tickets | MongoDB | 410 |
| entity | users_mongo | MongoDB | 411 |
| entity | users_mssql | MSSQL | 288 |
| entity | restaurants | MySQL | 461 |
| entity | drivers | PostgreSQL | 354 |
| entity | products | MySQL | 368 |
| entity | menu_sections | MySQL | 362 |
| entity | ratings | MySQL | 327 |
| entity | inventory | PostgreSQL | 261 |

`orders` is the hub table — it links every other domain via CPF (customer), CNPJ
(restaurant), `driver_id`, and payment/rating UUIDs.

---

## How to Run

### Prerequisites
- Docker Desktop 4.x+
- Python 3.11+
- Databricks CLI 0.200+
- Databricks workspace with Unity Catalog and DBR 14.1+ (or a free [Databricks Free Edition](https://www.databricks.com/learn/free-edition) account)

### Local setup

```bash
git clone https://github.com/christiandrocha/sdd-kafka-databricks
cd sdd-kafka-databricks

cp .env.example .env
# Edit .env with your credentials

make up                      # Start Kafka + PostgreSQL + Debezium
make produce-initial         # Load 80 files into PostgreSQL
make produce-incremental     # Load 20 incremental files
```

Access Kafka UI at `http://localhost:8080` and Grafana at `http://localhost:3001`.

### Databricks deploy

```bash
make deploy-dev              # databricks bundle deploy --target dev
make deploy-prod             # databricks bundle deploy --target prod
```

### Tests

```bash
make lint                    # ruff + yamllint
make test                    # pytest tests/test_contracts.py
```

---

## Methodology — AgentSpec SDD

This project was built using **Spec Driven Development (AgentSpec)** with Claude Code:

```
Brainstorm → Define → Design → Build → Ship
```

The `.claude/` directory contains 58 specialized agents, 8 knowledge base domains,
slash commands, and SDD templates. The architecture was validated through 5 rounds
of external Staff/Principal-level review before a single line of code was written.

```
.claude/
├── agents/           ← 58 specialized agents (streaming-engineer, medallion-architect...)
├── commands/         ← /brainstorm /define /design /build /ship
├── kb/               ← 8 knowledge domains (kafka-cdc, databricks, spark, medallion...)
├── sdd/              ← SDD templates, architecture, features, archive
├── 01_brainstorm.prompt
├── 02_define.spec.yaml
├── 03_design.manifest.json
├── 04_build.delegation.md
├── 05_implementation_log.md
└── 06_retrospective.md
```

---

## What Evolved from sdd-kafka-snowflake

| Component | sdd-kafka-snowflake | sdd-kafka-databricks |
|---|---|---|
| Destination | Snowflake Sink | Databricks Structured Streaming |
| Transformation | dbt | Parametrized PySpark notebooks |
| Orchestration | Dagster | Databricks Asset Bundles (DABs) |
| Storage | Snowflake VARIANT | Delta Lake 3.1 + Liquid Clustering |
| Catalog | Snowflake schemas | Unity Catalog (ubereats_dev/prod) |
| Data contracts | ❌ | ✅ YAML per table |
| Methodology | AgentSpec (basic) | AgentSpec (full — 58 agents) |

---

## Next Steps

- [ ] Databricks Delta Live Tables (DLT) migration — declarative pipeline evolution
- [ ] Databricks Feature Store for ML features from Gold tables
- [ ] Real-time monitoring with Databricks System Tables + SQL Alerts
- [ ] Schema Registry BACKWARD compatibility enforcement in CI pipeline

---

## Interview Cheat Sheet

**On Bronze architecture:**
> "We use SMT ExtractNewRecordState in Debezium to get flat records with __op and __source_ts_ms.
> This matches the proven sdd-kafka-snowflake pattern and simplifies the Silver notebooks significantly."

**On parametrized notebooks:**
> "Instead of 60 static notebooks, we built 2 parametrized ones via dbutils.widgets.
> DABs orchestrates them 20x and 11x with domain-specific configs — DRY principle applied to data engineering."

**On Liquid Clustering:**
> "cluster_by must match the MERGE ON columns for Databricks to use file pruning during MERGE.
> We enforce this alignment in test_contracts.py — it's a constraint, not a guideline."

**On dataset volume:**
> "129k records is an architectural microcosm. We're validating MERGE idempotency,
> contract governance, and clustering alignment — the design scales horizontally when real volume arrives."

**On the Free Edition constraint:**
> "Free Edition's serverless compute can't reach a self-hosted Kafka broker — outbound
> networking is locked to a fixed allowlist below the Enterprise tier. Rather than fork the
> pipeline, Bronze takes a `source_mode` parameter: `kafka` for dev/prod, `volume` for a
> batch read off a Parquet snapshot in Free Edition. Same contract, same MERGE, same
> idempotency guarantees — only the read path changes."

---

## License

[MIT](LICENSE) — free to use, modify, and learn from.

## Author

Built by [Christian Rocha](https://github.com/christiandrocha) as a hands-on exploration of
streaming CDC architecture on Databricks. Feedback and questions welcome via GitHub issues.
