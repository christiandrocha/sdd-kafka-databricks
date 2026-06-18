# Databricks Knowledge Base
# sdd-kafka-databricks specific patterns

## Structured Streaming patterns

### Reading from Kafka with Avro + Schema Registry
```python
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", starting_offsets) \
    .option("maxOffsetsPerTrigger", max_offsets) \
    .load()

# Parse Avro with Schema Registry (schema ID embedded in wire format)
from pyspark.sql.functions import from_avro
parsed = df.select(
    from_avro(
        col("value"),
        options={"schemaRegistryAddress": schema_registry_url}
    ).alias("data"),
    col("offset").alias("_kafka_offset"),
    col("timestamp").alias("_kafka_timestamp")
)
```

### trigger(availableNow=True) — scales to zero
```python
query = df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(availableNow=True) \
    .toTable(table_name)
query.awaitTermination()
```

### MERGE INTO with foreachBatch
```python
def merge_to_silver(df_batch, batch_id):
    df_batch = df_batch.withColumn("_processed_at", current_timestamp())
    df_batch.createOrReplaceTempView("staging")
    spark.sql(f"""
        MERGE INTO {silver_table} AS target
        USING staging AS source
        ON target.{merge_key} = source.{merge_key}
        WHEN MATCHED AND source._source_ts_ms > target._source_ts_ms
            THEN UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)

df.writeStream \
    .foreachBatch(merge_to_silver) \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(availableNow=True) \
    .start()
```

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

### Critical: cluster_by MUST match MERGE ON (ADR-04)
```
silver.payment_events: cluster_by=[event_id, event_ts]  MERGE ON event_id  ✅
silver.users:          cluster_by=[cpf]                  MERGE ON cpf       ✅
gold.payment_lifecycle: cluster_by=[payment_id]          MERGE ON payment_id ✅
```

## Databricks Asset Bundles (DABs)

### databricks.yml structure
```yaml
bundle:
  name: sdd-kafka-databricks

targets:
  dev:
    mode: development
    variables:
      catalog: ubereats_dev
      checkpoint_base: /Volumes/ubereats_dev/checkpoints

  prod:
    mode: production
    variables:
      catalog: ubereats_prod
      checkpoint_base: /Volumes/ubereats_prod/checkpoints

resources:
  jobs:
    sdd_kafka_pipeline:
      name: sdd-kafka-databricks-pipeline
      tasks:
        - task_key: bronze_payment_events
          notebook_task:
            notebook_path: notebooks/pipeline_bronze.ipynb
            base_parameters:
              table_name: payment_events
              kafka_topic: pg.public.payment_events
              bronze_table: "{{var.catalog}}.bronze.payment_events"
              max_offsets: "1000"
```

## Unity Catalog

### Namespace: catalog.schema.table
```python
# Always use fully qualified names
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

## Widgets pattern (parametrized notebooks)
```python
# Always provide defaults valid for dev environment
dbutils.widgets.text("table_name",      "payment_events")
dbutils.widgets.text("catalog",         "ubereats_dev")
dbutils.widgets.text("kafka_bootstrap", "localhost:9092")
dbutils.widgets.text("max_offsets",     "1000")

# Read all widgets at the top
TABLE_NAME  = dbutils.widgets.get("table_name")
CATALOG     = dbutils.widgets.get("catalog")
```

## JSONB fields (payment_events.event, order_status.status)
These JSONB columns arrive as escaped JSON strings in the Avro payload.
Direct path traversal fails. Use get_json_object or from_json:

```python
from pyspark.sql.functions import get_json_object, col

df = df.withColumn("event_name",
    get_json_object(col("data.event"), "$.event_name"))
df = df.withColumn("event_ts",
    get_json_object(col("data.event"), "$.timestamp").cast("long"))
```

## Anti-patterns

| Never do | Why | Instead |
|---|---|---|
| Read Bronze in Gold notebooks | Violates medallion lineage | Always read Silver |
| Hardcode catalog name | Breaks dev/prod parity | Use widget + DABs variable |
| cluster_by ≠ merge_key | Full table scan on MERGE | Align them (ADR-04) |
| 60 static notebooks | DRY violation | 2 parametrized via DABs |
| trigger(continuous=True) | Always-on cluster cost | trigger(availableNow=True) |
