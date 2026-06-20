# KB: Databricks — sdd-kafka-databricks specific patterns
# Corrected 2026-06-20: this file described the pre-v1.2.0 architecture (2
# parametrized notebooks, hand-written MERGE INTO via foreachBatch, per-domain
# DABs job tasks, dbutils.widgets). None of that exists anymore — v1.2.0
# retired all 8 notebooks into one Lakeflow Declarative Pipeline
# (pipelines/ubereats_pipeline.py) using create_auto_cdc_flow(), and
# databricks.yml collapsed to one pipeline + one 1-task Job, identical across
# all 3 targets. See kb/medallion.md (Bronze/Silver/Gold/CDC patterns) and
# kb/schema-registry.md (Kafka+Avro ingestion) for what replaced the content
# that used to be here — this file now covers only DABs/Unity
# Catalog/Liquid-Clustering infrastructure specifics not covered there.

## Liquid Clustering

### Create table with clustering
```python
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name}
    USING DELTA
    CLUSTER BY ({', '.join(cluster_by_cols)})
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")
```
In practice, every table's `cluster_by`/`TBLPROPERTIES` are declared in its
contract (`contracts/*.yml`'s `storage:` block) and translated by
`contracts/spark_schema.py`'s `to_tblproperties()` — not written ad hoc per
table the way this snippet implies. Use the contract, not a raw `CREATE TABLE`.

### Critical: cluster_by MUST match merge_key (ADR-04)
```
silver.payment_events: cluster_by=[event_id]   merge_key=event_id   ✅
silver.users:          cluster_by=[cpf]        merge_key=cpf        ✅
silver.drivers:        cluster_by=[uuid]       merge_key=uuid       ✅
```
Enforced by `tests/test_contracts.py::test_06_merge_key_in_cluster_by` — a
contract that violates this fails CI, not just a runtime surprise.

## Databricks Asset Bundles (DABs) — real structure (v1.2.0)

`databricks.yml` is **one pipeline + one 1-task Job, identical across `dev`,
`prod`, and `free_edition`** — not three different resource shapes. DABs has
no way to exclude a root-level resource from a single target
([databricks/cli#2872](https://github.com/databricks/cli/issues/2872)), so
each target still declares its own `resources.pipelines.ubereats_pipeline`/
`resources.jobs.ubereats_pipeline`, but both are aliases of one shared YAML
anchor pair:

```yaml
variables:
  _pipeline_anchors:
    default:
      pipeline_resource: &pipeline_resource
        name: ubereats_pipeline
        catalog: ${var.catalog}
        serverless: true            # Free Edition workspace — no job_clusters anywhere
        libraries:
          - file: { path: pipelines/ubereats_pipeline.py }
        configuration:
          ubereats.catalog: ${var.catalog}
          ubereats.source_mode: ${var.bronze_source_mode}
          bundle.files.path: ${workspace.file_path}

      pipeline_task: &pipeline_task
        task_key: ubereats_pipeline_task
        pipeline_task:
          pipeline_id: ${resources.pipelines.ubereats_pipeline.id}

targets:
  dev:
    variables: { catalog: ubereats_dev, bronze_source_mode: volume }
    resources:
      pipelines: { ubereats_pipeline: { <<: *pipeline_resource } }
      jobs: { ubereats_pipeline: { tasks: [*pipeline_task] } }
  # prod / free_edition: same shape, different `variables:` only
```

Targets differ **only** by `variables:` (`catalog`, `bronze_source_mode`,
`landing_base`) — never by resource shape or task count. There is no per-domain
task anymore (the old shape was a 20+ task Job, one notebook task per domain);
the entire 37-table DAG (20 Bronze + 11 Silver/quarantine pairs + 6 Gold) is
one `pipeline_task` pointing at one Lakeflow pipeline. See
`docs/adr/007_pipeline_unification.md`.

## Unity Catalog

### Namespace: catalog.schema.table
```python
# Always use fully qualified names — every table reference in
# pipelines/ubereats_pipeline.py is built this way, never a bare table name
full_table = f"{catalog}.{schema}.{table_name}"
# Examples:
# ubereats_dev.bronze.payment_events
# ubereats_dev.silver.users
# ubereats_prod.gold.payment_lifecycle
```

### Create catalog and schema (idempotent)
```python
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.quarantine")
```
Done by `scripts/preflight_unity_catalog.sh`, not inline in the pipeline —
`pipelines/ubereats_pipeline.py` assumes the catalog/schemas already exist.

## JSONB fields (payment_events.event, order_status.status)

These columns arrive as escaped JSON strings in the Avro payload (verified
against `contracts/payment_events.yml`/`contracts/order_status.yml` — both
declare `type: string`, not a struct). Direct path traversal fails; use
`get_json_object`:

```python
from pyspark.sql.functions import get_json_object, coalesce, col

# Real usage, register_gold_payment_funnel() / register_gold_payment_lifecycle()
# in pipelines/ubereats_pipeline.py:
df = df.withColumn(
    "event_name",
    coalesce(get_json_object(col("event"), "$.event_name"), col("event")),
)
```
The `coalesce(..., col("event"))` fallback matters — not every row's `event`
field is JSON-shaped; falling back to the raw string avoids turning a parse
miss into a silent `NULL`.

## Anti-patterns

See `kb/anti-patterns.md` for the full, severity-ranked list (this file used
to keep its own separate table — consolidated there to avoid two copies
drifting apart). The two items most specific to this file's topics are C01
(Gold's full-recompute "scan" is intentional, don't add a `WHERE`) and H02
(Liquid Clustering, not partitioning — `cluster_by` must equal `merge_key`).
