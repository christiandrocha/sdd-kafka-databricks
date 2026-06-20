# KB: Observability — Prometheus + Grafana for the Kafka/Debezium stack
# sdd-kafka-databricks specific — this file used to be a near-verbatim copy of
# sdd-kafka-snowflake's observability KB (Dagster, dbt.md, Snowflake Sink
# connectors). None of that applies here: this project's downstream is
# Databricks Structured Streaming reading Kafka directly (no Sink Connector),
# orchestrated by DABs (no Dagster). Corrected 2026-06-20.

## Two types of observability

**Infrastructure observability** (this KB):
- Is the pipeline running? Are services healthy?
- Is Kafka keeping up with PostgreSQL's event rate?
- Is the `debezium-postgres-cdc` connector task RUNNING?
- Is the Databricks job (`ubereats_pipeline`, see `databricks.yml`) succeeding?
- Tool: Prometheus + Grafana (this KB) for the Kafka/Debezium side; Databricks
  job run history/system tables for the pipeline side — there is no
  Dagster/dbt layer in this project.

**Data quality observability** (see `kb/data-quality.md`, `kb/anti-patterns.md`):
- Is the data correct? Fresh? Complete?
- Are rows landing in `quarantine.<domain>` that shouldn't be?
- **Did a DELETE propagate correctly to Silver/Gold?** — fixed 2026-06-20
  (`kb/anti-patterns.md` C08, `CLAUDE.md`): `REPLICA IDENTITY FULL` +
  `apply_as_deletes` now make this work, verified live on the Postgres/Kafka
  side. Still no infra metric in this file would have caught the original bug
  (it was a data correctness gap, not an infra failure) or would catch a
  regression — a scheduled data-quality check on this is still not
  implemented, tracked as a follow-up, not just historical color.
- Tool: `tests/test_contracts.py`/`tests/test_dlt_adapter.py` (structural only,
  see `kb/data-quality.md`'s "what tests actually validate" section) — there
  is no dbt/Great Expectations layer in this project.

Both are necessary. This KB covers infrastructure observability only.

## Architecture (matches docker-compose.yml as of v1.2.0)

```
Kafka broker JMX (jmx-kafka sidecar, bitnami/jmx-exporter)
    └─▶ Prometheus job "kafka-jmx" (jmx-kafka:9101)

Kafka Connect / Debezium JMX (jmx-kafka-connect sidecar)
    └─▶ Prometheus job "kafka-connect-jmx" (jmx-kafka-connect:9404)

Consumer group lag (danielqsj/kafka-exporter)
    └─▶ Prometheus job "kafka-consumer-lag" (kafka-exporter:9308)

Prometheus (port 9090, scrape_interval 15s)
    └─▶ Grafana (port 3001 externally / 3000 internally)
        dashboards: observability/grafana/dashboards/{kafka,kafka_connect}.json
```

There is no Kafka Sink Connector in this project — only one connector exists,
`debezium-postgres-cdc` (`connectors/debezium.json`). Databricks consumes
Kafka directly via Structured Streaming (`source_mode=kafka` in
`pipelines/ubereats_pipeline.py`) or via the `landing` Volume snapshot
(`source_mode=volume`) — never via a second Kafka Connect sink. If you see
"sink"/"sinkitems" referenced anywhere else in this KB, that's leftover from
the sdd-kafka-snowflake port; flag it.

## Real scrape config and alerts

The actual config lives in `observability/prometheus/prometheus.yml` and
`observability/prometheus/alert_rules.yml` — read those directly rather than
trusting a copy pasted here (this file drifted from them once already).
Summary of the 5 alerts currently defined, by metric:

| Alert | Metric | Severity | Meaning |
|---|---|---|---|
| `KafkaConsumerLagHigh` | `kafka_consumergroup_lag > 1000` for 5m | warning | A consumer group is falling behind on a topic |
| `KafkaConsumerLagCritical` | `sum(kafka_consumergroup_lag) > 10000` for 10m | critical | Bronze ingestion may be stalled |
| `ConnectorTaskFailed` | `kafka_connect_connector_task_metrics_running_ratio < 1` for 2m | critical | `debezium-postgres-cdc` task not RUNNING — CDC interrupted |
| `BrokerDown` | `kafka_brokers < 1` for 1m | critical | Entire Kafka cluster unreachable |
| `UnderReplicatedPartitions` | `kafka_server_replicamanager_underreplicatedpartitions > 0` for 2m | warning | Not relevant for the local single-broker setup, kept for when this targets a real cluster |

None of these would have caught the DELETE/NULL-corruption gap (C08, fixed
2026-06-20) — that needed a data-level check (e.g. a scheduled query counting
NULL-heavy rows per `merge_key` in Silver), not an infra alert, and still
would for any regression. Not implemented; flagged here so it isn't assumed
to be covered.

## Connector status check

```bash
# Only one connector in this project
curl http://localhost:8083/connectors/debezium-postgres-cdc/status

# Connector-level "RUNNING" can mask a FAILED task underneath — always check
# tasks[].state too (hit this directly during the 2026-06-20 live DELETE test:
# connector showed RUNNING while its one task had crashed on a stale
# credential and silently stopped streaming).
curl http://localhost:8083/connectors/debezium-postgres-cdc/status | python3 -c \
  "import json,sys; d=json.load(sys.stdin); print(d['connector']['state'], [t['state'] for t in d['tasks']])"
```

## Verifying observability setup

```bash
# Prometheus targets all UP
curl http://localhost:9090/targets

# Consumer lag for a specific topic
curl 'http://localhost:9090/api/v1/query?query=kafka_consumergroup_lag' | python3 -m json.tool

# Grafana accessible (default: admin/admin, port 3001 externally)
curl http://localhost:3001

# Alert rules loaded
curl http://localhost:9090/api/v1/rules | python3 -m json.tool
```

## Replication slot lag (PostgreSQL side, not in Prometheus today)

```sql
SELECT slot_name, active, confirmed_flush_lsn, restart_lsn, pg_current_wal_lsn()
FROM pg_replication_slots;
```

Not currently scraped into Prometheus — checked manually. A large gap between
`restart_lsn` and `pg_current_wal_lsn()` means Debezium has a backlog to
replay (observed during the 2026-06-20 live test, after the stack had been
stopped/started across multiple sessions without a clean teardown — `make
down` between sessions avoids this).
