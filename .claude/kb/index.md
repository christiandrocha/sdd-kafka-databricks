# Knowledge Base Index
# sdd-kafka-databricks
# Read this file first before using any KB domain.

## Available domains

| Domain | File | When to use |
|---|---|---|
| kafka-cdc | kafka-cdc.md | Debezium, WAL, SMT, connectors, 20-table CDC |
| databricks | databricks.md | Structured Streaming, MERGE INTO, DABs, Unity Catalog |
| spark | spark.md | PySpark patterns, Delta Lake, from_avro, Liquid Clustering |
| medallion | medallion.md | Bronze/Silver/Gold patterns, quality gates, quarantine |
| data-quality | data-quality.md | Data Contracts YAML, quality rules, test_contracts |
| schema-registry | schema-registry.md | Avro, Confluent, BACKWARD compatibility |
| cicd | cicd.md | GitHub Actions, DABs deploy, lint, bundle validate |
| observability | observability.md | Prometheus, Grafana, Kafka consumer lag, alert rules |

## Project-specific context

Always read CLAUDE.md before any KB file.
CLAUDE.md has the domain map, ADR summaries, and critical architecture decisions.

## Key decisions (quick reference)

- **Bronze**: SMT ExtractNewRecordState → flat records with __op + __source_ts_ms
- **Silver**: MERGE INTO with Liquid Clustering (cluster_by = merge_key — ADR-04)
- **Notebooks**: 2 parametrized (pipeline_bronze + pipeline_silver) via DABs
- **Schema Registry**: Confluent (not Apicurio)
- **Topology**: Unidirectional (load_to_postgres.py → PostgreSQL → Debezium → Kafka → Databricks)
- **Unity Catalog**: ubereats_dev/prod → bronze/silver/gold/quarantine
