# ADR 001 — Databricks instead of Snowflake

**Status:** Accepted
**Date:** 2026-06-16

## Context

sdd-kafka-snowflake uses Snowflake as the data warehouse with dbt for transformation
and Dagster for orchestration. This project migrates the destination layer while
keeping the Kafka/Debezium/PostgreSQL infrastructure identical.

## Decision

Replace Snowflake Sink + dbt + Dagster with:
- Databricks Structured Streaming (ingestion)
- PySpark notebooks (transformation)
- Databricks Asset Bundles — DABs (orchestration)
- Unity Catalog (governance)

## Rationale

| Factor | Snowflake | Databricks |
|---|---|---|
| Streaming | Snowpipe (~1-2 min latency) | Native streaming (seconds) |
| Clustering | Manual CLUSTER BY | Liquid Clustering (auto) |
| Catalog | Schemas only | Unity Catalog (RBAC + lineage) |
| Orchestration | Dagster (external) | DABs (native GitOps) |
| Cost | Credits per query | Serverless scales to zero |
| Notebook support | Limited | First-class |

## Alternatives considered

- **Snowflake (original)** — rejected: external Dagster dependency, Snowpipe latency,
  no native streaming, separate catalog governance
- **BigQuery** — rejected: no Unity Catalog equivalent, different ecosystem
- **Databricks Lakeflow (DLT)** — evaluated but Structured Streaming is more explicit
  and gives more control for demonstrating engineering decisions

## Consequences

**Positive:** Native streaming, Unity Catalog, DABs GitOps, Liquid Clustering, scales to zero
**Negative:** Requires Databricks workspace; local dev needs cloud connectivity for notebooks
