# ADR 002 — SMT ExtractNewRecordState and Bronze pattern

**Status:** Accepted
**Date:** 2026-06-16

## Context

Debezium publishes a nested envelope: `{before, after, op, ts_ms, source}`.
The question is whether to unwrap this envelope in the Kafka Connect connector
(via SMT) or in the Databricks notebook (via Spark).

## Decision

Use SMT ExtractNewRecordState in the Debezium connector.
Bronze receives flat records with `__op` and `__source_ts_ms` fields.

## Rationale

In a **unidirectional topology** (load_to_postgres.py → PostgreSQL → Debezium → Kafka → Databricks),
SMT is the correct pattern:

1. Matches the proven sdd-kafka-snowflake architecture exactly
2. Simplifies Bronze notebook (no envelope navigation)
3. Bronze still has `__op` and `__source_ts_ms` for lineage and CDC tracking
4. Bronze is still append-only — immutability is preserved

## Note on design session

Earlier in the design session, we discussed removing SMT to keep the raw envelope in Bronze
for "replay fidelity". That decision was made in the context of a **bidirectional Debezium
topology** (JDBC Sink → PostgreSQL → CDC Source) which was later abandoned.

In the final **unidirectional topology**, using SMT is correct. The "raw envelope" concern
applies when the Bronze layer is the only audit trail. Here, PostgreSQL itself serves as
the authoritative source and can be replayed via load_to_postgres.py if needed.

## Alternatives considered

- **Raw envelope in Bronze (no SMT)** — rejected for this topology: adds complexity
  (from_avro envelope navigation in Silver) without the benefit it provides in
  bidirectional architectures where PostgreSQL is not the source of truth

## Consequences

**Positive:** Simpler Bronze and Silver notebooks; proven pattern from sdd-kafka-snowflake
**Negative:** Bronze records are post-SMT flat (not raw Debezium envelope); `before` field not available
