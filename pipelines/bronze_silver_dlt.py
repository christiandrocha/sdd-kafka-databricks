import sys
from pathlib import Path

import requests
from pyspark import pipelines as dp
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, current_timestamp, expr

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from contracts.dlt_adapter import (
    quarantine_row_level_predicate,
    to_reject_expectations,
    to_warn_expectations,
    unique_check_fields,
)
from contracts.loader import load_contract

CATALOG = spark.conf.get("ubereats.catalog", "ubereats_dev")
KAFKA_BOOTSTRAP = spark.conf.get("ubereats.kafka_bootstrap", "localhost:9092")
SCHEMA_REGISTRY_URL = spark.conf.get("ubereats.schema_registry_url", "http://localhost:8081")
STARTING_OFFSETS = spark.conf.get("ubereats.starting_offsets", "earliest")
CONTRACTS_DIR = Path(__file__).resolve().parent.parent / "contracts"

# ADR-08: order_items is 85% of total volume (110,001 records) — the same maxOffsetsPerTrigger
# used for every other domain would take ~110 micro-batches instead of 1-5. Was a per-task
# DABs parameter; now a Python-side override since there's a single shared pipeline.
MAX_OFFSETS_OVERRIDES: dict[str, int] = {"order_items": 5000}
DEFAULT_MAX_OFFSETS = 1000

# silver_users (pipeline_users.ipynb) stays a notebook (DESIGN_LAKEFLOW_MIGRATION.md, Out of
# Scope) — its FULL OUTER JOIN + full-refresh logic doesn't fit the incremental model here.
# users_mongo/users_mssql still get a Bronze table from this pipeline; only the per-domain
# generic Silver treatment is skipped for them.
SILVER_EXCLUDED_DOMAINS = {"users_mongo", "users_mssql"}


def _avro_schema_str(kafka_topic: str) -> str:
    subject = f"{kafka_topic}-value"
    resp = requests.get(
        f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest",
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["schema"]


def _unique_violations(candidate_df, fields, merge_key, silver_table):
    """Stream-static join — porta _unique_violation_values() de pipeline_silver.ipynb para
    uma transformacao declarativa. O lado estatico (spark.read.table) e' reavaliado a cada
    disparo de microbatch pelo motor Spark Structured Streaming."""
    bad = candidate_df.sparkSession.createDataFrame([], candidate_df.schema).limit(0)
    for field in fields:
        existing = spark.read.table(silver_table).select(field, merge_key).distinct()
        cross_batch = (
            candidate_df.select(field, merge_key).distinct().alias("i")
            .join(existing.alias("e"), field)
            .filter(col(f"i.{merge_key}") != col(f"e.{merge_key}"))
        )
        bad = bad.unionByName(
            candidate_df.join(cross_batch.select(field).distinct(), field, "left_semi")
        )
    return bad.distinct()


def register_bronze(contract: dict) -> str:
    domain = contract["table"]["name"]
    kafka_topic = contract["table"]["kafka_topic"]
    cluster_by = contract["storage"]["cluster_by"]
    bronze_table = f"{CATALOG}.bronze.{domain}"
    max_offsets = MAX_OFFSETS_OVERRIDES.get(domain, DEFAULT_MAX_OFFSETS)
    avro_schema_str = _avro_schema_str(kafka_topic)

    @dp.table(name=bronze_table, cluster_by=cluster_by, comment=f"Bronze: {domain}")
    @dp.expect_all_or_drop(to_reject_expectations(contract, scope="bronze"))
    def _bronze():
        raw_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", kafka_topic)
            .option("startingOffsets", STARTING_OFFSETS)
            .option("maxOffsetsPerTrigger", max_offsets)
            .load()
            .select(expr("substring(value, 6)").alias("avro_bytes"))
        )
        return (
            raw_stream
            .select(from_avro(col("avro_bytes"), avro_schema_str).alias("d"))
            .select("d.*")
            .withColumn("_ingested_at", current_timestamp())
        )

    return bronze_table


def register_silver(contract: dict, bronze_table: str) -> None:
    domain = contract["table"]["name"]
    merge_key = contract["table"]["merge_key"]
    cluster_by = contract["storage"]["cluster_by"]
    silver_table = f"{CATALOG}.silver.{domain}"
    quarantine_table = f"{CATALOG}.quarantine.{domain}"

    candidate_view = f"{domain}_silver_candidate"
    clean_view = f"{domain}_silver_clean"

    @dp.temporary_view(name=candidate_view)
    @dp.expect_all(to_warn_expectations(contract, scope="silver"))
    def _candidate():
        return dp.read_stream(bronze_table)

    row_predicate = quarantine_row_level_predicate(contract, scope="silver")
    unique_fields = unique_check_fields(contract, scope="silver")

    @dp.table(name=quarantine_table, comment=f"Quarantine: {domain}")
    def _quarantine():
        candidate = dp.read_stream(candidate_view)
        rowlevel_bad = candidate.filter(row_predicate) if row_predicate else candidate.limit(0)
        unique_bad = (
            _unique_violations(candidate, unique_fields, merge_key, silver_table)
            if unique_fields else candidate.limit(0)
        )
        return rowlevel_bad.unionByName(unique_bad).distinct()

    @dp.temporary_view(name=clean_view)
    def _clean():
        candidate = dp.read_stream(candidate_view)
        bad = dp.read_stream(quarantine_table)
        return candidate.join(bad, merge_key, "left_anti")

    dp.create_streaming_table(name=silver_table, cluster_by=cluster_by)
    dp.create_auto_cdc_flow(
        target=silver_table,
        source=clean_view,
        keys=[merge_key],
        sequence_by=col("__source_ts_ms"),
        stored_as_scd_type=1,
    )


for _contract_path in sorted(CONTRACTS_DIR.glob("*.yml")):
    _contract = load_contract(_contract_path)
    _layers = _contract["table"]["layers"]
    _domain = _contract["table"]["name"]

    _bronze_table = register_bronze(_contract)

    if "silver" in _layers and _domain not in SILVER_EXCLUDED_DOMAINS:
        register_silver(_contract, _bronze_table)
