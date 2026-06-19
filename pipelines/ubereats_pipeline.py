import sys
from pathlib import Path

import requests
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    countDistinct,
    current_timestamp,
    desc,
    expr,
    first,
    get_json_object,
    last,
    lit,
    max,
    min,
    regexp_replace,
    row_number,
    sum,
    unix_timestamp,
    when,
)

# Lakeflow Declarative Pipelines execute this file without __file__ defined (it's not run
# as `python <path>` the way a notebook or a plain script is) — bundle.files.path (set in
# databricks.yml's pipeline configuration via the DABs-native ${workspace.file_path}
# variable) replaces every Path(__file__) usage below.
BUNDLE_FILES_PATH = spark.conf.get(
    "bundle.files.path",
    "/Workspace/Users/christiandr@gmail.com/.bundle/sdd-kafka-databricks/dev/files",
)
sys.path.insert(0, BUNDLE_FILES_PATH)
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
CONTRACTS_DIR = Path(BUNDLE_FILES_PATH) / "contracts"

# PIPELINE_UNIFICATION: source_mode replaces the per-task `volume_path`/`source_mode` widgets
# pipeline_bronze.ipynb used to take — one pipeline-level setting now drives all 20 domains,
# for whichever target (dev/prod/free_edition) sets it. See docs/adr/007_pipeline_unification.md.
SOURCE_MODE = spark.conf.get("ubereats.source_mode", "kafka")
VOLUME_BASE = spark.conf.get("ubereats.volume_base", "/Volumes/ubereats_dev/landing/kafka_export")

# ADR-08: order_items is 85% of total volume (110,001 records) — the same maxOffsetsPerTrigger
# used for every other domain would take ~110 micro-batches instead of 1-5. Was a per-task
# DABs parameter; now a Python-side override since there's a single shared pipeline.
MAX_OFFSETS_OVERRIDES: dict[str, int] = {"order_items": 5000}
DEFAULT_MAX_OFFSETS = 1000

# users_mongo/users_mssql still get a Bronze table from the generic loop below; only the
# per-domain generic Silver treatment is skipped for them — register_silver_users() handles
# their FULL OUTER JOIN + full-refresh logic instead (PIPELINE_UNIFICATION, supersedes
# ADR-006's "Explicitly NOT migrated" — see docs/adr/007_pipeline_unification.md).
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

    @dp.table(name=bronze_table, cluster_by=cluster_by, comment=f"Bronze: {domain}")
    @dp.expect_all_or_drop(to_reject_expectations(contract, scope="bronze"))
    def _bronze():
        if SOURCE_MODE == "kafka":
            avro_schema_str = _avro_schema_str(kafka_topic)
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
        elif SOURCE_MODE == "volume":
            return (
                spark.read
                .format("parquet")
                .load(f"{VOLUME_BASE}/{domain}")
                .withColumn("_ingested_at", current_timestamp())
            )
        raise ValueError(f"Unknown source_mode: {SOURCE_MODE!r}")

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


def _prepped_users(bronze_table: str):
    return (
        dp.read(bronze_table)
        .filter(col("__op") != "d")
        .withColumn("cpf_key", regexp_replace(col("cpf"), r"[.\-]", ""))
    )


def _dedup_by_cpf(df):
    """Keep the latest row per cpf_key (highest __source_ts_ms) — ported from
    pipeline_users.ipynb's dedup_by_cpf()."""
    w = Window.partitionBy("cpf_key").orderBy(desc("__source_ts_ms"))
    return df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")


def _to_quarantine_shape(df, source: str, reason: str):
    """Project a single bronze frame (missing cpf) into quarantine.users's shape — ported
    from pipeline_users.ipynb's to_quarantine_shape()."""
    string_null = lit(None).cast("string")
    fields = {
        "cpf": col("cpf_key"),
        "user_id": col("user_id"),
        "uuid_mongo": col("uuid") if source == "mongo" else string_null,
        "uuid_mssql": col("uuid") if source == "mssql" else string_null,
        "email": col("email") if source == "mongo" else string_null,
        "first_name": col("first_name") if source == "mssql" else string_null,
        "last_name": col("last_name") if source == "mssql" else string_null,
        "phone_number": col("phone_number"),
        "city": col("city") if source == "mongo" else string_null,
        "country": col("country"),
        "delivery_address": col("delivery_address") if source == "mongo" else string_null,
        "birthday": col("birthday") if source == "mssql" else lit(None).cast("date"),
        "job": col("job") if source == "mssql" else string_null,
        "company_name": col("company_name") if source == "mssql" else string_null,
        "_ingested_at": col("_ingested_at"),
        "_quarantine_reason": lit(reason),
    }
    return df.select(*[column_expr.alias(name) for name, column_expr in fields.items()])


def _build_joined_users(mongo_df, mssql_df):
    """FULL OUTER JOIN on normalized cpf — ported from pipeline_users.ipynb's join cell."""
    joined_df = mongo_df.alias("m").join(
        mssql_df.alias("s"),
        col("m.cpf_key") == col("s.cpf_key"),
        how="full_outer",
    )
    return joined_df.select(
        coalesce(col("m.cpf_key"), col("s.cpf_key")).alias("cpf"),
        coalesce(col("m.user_id"), col("s.user_id")).alias("user_id"),
        col("m.uuid").alias("uuid_mongo"),
        col("s.uuid").alias("uuid_mssql"),
        col("m.email"),
        col("s.first_name"),
        col("s.last_name"),
        coalesce(col("m.phone_number"), col("s.phone_number")).alias("phone_number"),
        col("m.city"),
        coalesce(col("m.country"), col("s.country")).alias("country"),
        col("m.delivery_address"),
        col("s.birthday"),
        col("s.job"),
        col("s.company_name"),
        coalesce(col("m._ingested_at"), col("s._ingested_at")).alias("_ingested_at"),
    )


def register_silver_users() -> None:
    """silver.users + quarantine.users — ported from notebooks/pipeline_users.ipynb
    (ADR-006's original exclusion, reversed by docs/adr/007_pipeline_unification.md).

    Both tables derive from one candidate view carrying a nullable _quarantine_reason —
    the same inverse-predicate convention contracts/dlt_adapter.py applies to the 10
    generic Silver domains, generalized to two quarantine causes (missing_cpf, computed
    pre-join; duplicate_user_id, computed post-join) instead of one.
    """
    silver_table = f"{CATALOG}.silver.users"
    quarantine_table = f"{CATALOG}.quarantine.users"
    bronze_mongo = f"{CATALOG}.bronze.users_mongo"
    bronze_mssql = f"{CATALOG}.bronze.users_mssql"
    candidate_view = "users_silver_candidate"

    @dp.temporary_view(name=candidate_view)
    def _candidate():
        mongo_raw = _prepped_users(bronze_mongo)
        mssql_raw = _prepped_users(bronze_mssql)

        missing = _to_quarantine_shape(
            mongo_raw.filter(col("cpf_key").isNull()), "mongo", "missing_cpf"
        ).unionByName(
            _to_quarantine_shape(mssql_raw.filter(col("cpf_key").isNull()), "mssql", "missing_cpf")
        )

        mongo_df = _dedup_by_cpf(mongo_raw.filter(col("cpf_key").isNotNull()))
        mssql_df = _dedup_by_cpf(mssql_raw.filter(col("cpf_key").isNotNull()))
        joined = _build_joined_users(mongo_df, mssql_df)

        # user_id is carried for gold_user_behavior's join with search_events/recommendations,
        # but cpf — not user_id — is silver.users's real identity (MERGE/cluster key, ADR-04).
        # A duplicate user_id would fan out that Gold join, so it's quarantined here rather
        # than fixed up downstream — see docs/adr/005_gold_dimension_join_integrity.md.
        dup_user_ids = (
            joined.filter(col("user_id").isNotNull())
            .groupBy("user_id")
            .count()
            .filter("count > 1")
            .select(col("user_id").alias("_dup_user_id"))
        )
        joined_tagged = (
            joined.join(dup_user_ids, col("user_id") == col("_dup_user_id"), "left")
            .withColumn(
                "_quarantine_reason",
                when(col("_dup_user_id").isNotNull(), lit("duplicate_user_id"))
                .otherwise(lit(None).cast("string")),
            )
            .drop("_dup_user_id")
        )

        return joined_tagged.unionByName(missing)

    @dp.table(name=quarantine_table, cluster_by=["cpf"], comment="Quarantine: users")
    def _quarantine_users():
        return (
            dp.read(candidate_view)
            .filter(col("_quarantine_reason").isNotNull())
            .withColumn("_quarantine_ts", current_timestamp())
        )

    @dp.table(name=silver_table, cluster_by=["cpf"], comment="Silver: users")
    def _silver_users():
        return (
            dp.read(candidate_view)
            .filter(col("_quarantine_reason").isNull())
            .drop("_quarantine_reason")
            .withColumn("_merged_at", current_timestamp())
        )


def register_gold_payments_by_status() -> None:
    """Ported from notebooks/cross_domain/gold_payments_by_status.ipynb."""
    gold_table = f"{CATALOG}.gold.payments_by_status"
    silver_payments = f"{CATALOG}.silver.payments"

    @dp.table(name=gold_table, cluster_by=["status"], comment="Gold: payments_by_status")
    def _gold():
        return (
            dp.read(silver_payments)
            .groupBy("status")
            .agg(
                count("payment_id").alias("payment_count"),
                sum("amount").alias("total_amount_brl"),
                avg("amount").alias("avg_amount_brl"),
                sum("net_amount").alias("total_net_amount_brl"),
                sum("platform_fee").alias("total_platform_fee_brl"),
                sum("refund_amount").alias("total_refund_amount_brl"),
                sum("tax_amount").alias("total_tax_amount_brl"),
            )
            .withColumn("_computed_at", current_timestamp())
        )


def register_gold_payment_funnel() -> None:
    """Ported from notebooks/cross_domain/gold_payment_funnel.ipynb."""
    gold_table = f"{CATALOG}.gold.payment_funnel"
    silver_src = f"{CATALOG}.silver.payment_events"

    @dp.table(name=gold_table, cluster_by=["event_name"], comment="Gold: payment_funnel")
    def _gold():
        return (
            dp.read(silver_src)
            .withColumn(
                "event_name",
                coalesce(get_json_object(col("event"), "$.event_name"), col("event")),
            )
            .filter(col("event_name").isNotNull())
            .groupBy("event_name")
            .agg(
                count("event_id").alias("event_count"),
                countDistinct("payment_id").alias("unique_payment_count"),
            )
            .withColumn("_computed_at", current_timestamp())
        )


def register_gold_payment_lifecycle() -> None:
    """Ported from notebooks/cross_domain/gold_payment_lifecycle.ipynb."""
    gold_table = f"{CATALOG}.gold.payment_lifecycle"
    silver_src = f"{CATALOG}.silver.payment_events"

    @dp.table(name=gold_table, cluster_by=["payment_id"], comment="Gold: payment_lifecycle")
    def _gold():
        return (
            dp.read(silver_src)
            .withColumn(
                "event_name",
                coalesce(get_json_object(col("event"), "$.event_name"), col("event")),
            )
            .groupBy("payment_id")
            .agg(
                count("event_id").alias("event_count"),
                first("event_name", ignorenulls=True).alias("first_event_name"),
                last("event_name", ignorenulls=True).alias("last_event_name"),
                min("dt_current_timestamp").alias("first_event_at"),
                max("dt_current_timestamp").alias("last_event_at"),
            )
            .withColumn(
                "lifecycle_duration_sec",
                (unix_timestamp("last_event_at") - unix_timestamp("first_event_at")).cast("double"),
            )
            .withColumn("_computed_at", current_timestamp())
        )


def register_gold_driver_performance() -> None:
    """Ported from notebooks/cross_domain/gold_driver_performance.ipynb. silver.drivers is
    deduped/merged by uuid, not driver_id (docs/adr/005_gold_dimension_join_integrity.md) —
    the row_number() guard below stays as defense-in-depth even though contracts/drivers.yml's
    check: unique rule on driver_id should keep duplicates from reaching this join at all."""
    gold_table = f"{CATALOG}.gold.driver_performance"
    silver_shifts = f"{CATALOG}.silver.driver_shifts"
    silver_drivers = f"{CATALOG}.silver.drivers"
    silver_orders = f"{CATALOG}.silver.orders"

    @dp.table(name=gold_table, cluster_by=["driver_id"], comment="Gold: driver_performance")
    def _gold():
        driver_shifts = dp.read(silver_shifts)
        drivers = dp.read(silver_drivers)
        orders = dp.read(silver_orders)

        shifts_agg = (
            driver_shifts.groupBy("driver_id")
            .agg(
                count("shift_id").alias("total_shifts"),
                sum("num_orders").alias("total_orders"),
                sum("earnings_brl").alias("total_earnings_brl"),
                avg("shift_rating").alias("avg_shift_rating"),
                sum("distance_covered_km").alias("total_distance_km"),
                avg("shift_duration_min").alias("avg_shift_duration_min"),
            )
        )

        driver_order_counts = (
            orders
            .filter(col("driver_key").isNotNull())
            .withColumn("driver_id_int", col("driver_key").cast("integer"))
            .groupBy("driver_id_int")
            .agg(countDistinct("order_id").alias("order_count"))
        )

        perf_df = (
            shifts_agg
            .join(
                drivers.select("driver_id", "first_name", "last_name", "city", "vehicle_type"),
                on="driver_id",
                how="left",
            )
            .join(
                driver_order_counts,
                shifts_agg["driver_id"] == driver_order_counts["driver_id_int"],
                how="left",
            )
            .select(
                shifts_agg["driver_id"],
                col("first_name"),
                col("last_name"),
                col("city"),
                col("vehicle_type"),
                col("total_shifts"),
                col("total_orders"),
                col("order_count"),
                col("total_earnings_brl"),
                col("avg_shift_rating"),
                col("total_distance_km"),
                col("avg_shift_duration_min"),
            )
            .withColumn("_computed_at", current_timestamp())
        )

        w = Window.partitionBy("driver_id").orderBy(desc("_computed_at"))
        return (
            perf_df
            .withColumn("_rn", row_number().over(w))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )


def register_gold_revenue_per_restaurant() -> None:
    """Ported from notebooks/cross_domain/gold_revenue_per_restaurant.ipynb. Order-level
    metrics (avg_order_value_brl, total_orders) are computed BEFORE any join with
    order_items — joining first would repeat each order's total_amount once per item it
    contains, inflating avg_order_value_brl for restaurants with multi-item orders.
    silver.restaurants is deduped/merged by uuid, not cnpj (ADR-005) — the row_number()
    guard stays as defense-in-depth."""
    gold_table = f"{CATALOG}.gold.revenue_per_restaurant"
    silver_order_items = f"{CATALOG}.silver.order_items"
    silver_orders = f"{CATALOG}.silver.orders"
    silver_restaurants = f"{CATALOG}.silver.restaurants"

    @dp.table(
        name=gold_table, cluster_by=["restaurant_cnpj"], comment="Gold: revenue_per_restaurant"
    )
    def _gold():
        order_items = dp.read(silver_order_items)
        orders = dp.read(silver_orders)
        restaurants = dp.read(silver_restaurants)

        orders_agg = (
            orders
            .groupBy("restaurant_key")
            .agg(
                countDistinct("order_id").alias("total_orders"),
                avg("total_amount").alias("avg_order_value_brl"),
            )
        )

        items_agg = (
            order_items.alias("i")
            .join(
                orders.select(col("order_id"), col("restaurant_key")).alias("o"),
                col("i.order_id") == col("o.order_id"),
                "inner",
            )
            .groupBy(col("o.restaurant_key"))
            .agg(
                count(col("i.order_item_id")).alias("total_items_sold"),
                sum(col("i.subtotal")).alias("total_revenue_brl"),
                avg(col("i.discount_applied")).alias("avg_discount_brl"),
            )
        )

        revenue_df = (
            items_agg
            .join(orders_agg, on="restaurant_key", how="left")
            .join(
                restaurants.select(
                    col("cnpj"),
                    col("name").alias("restaurant_name"),
                    col("city"),
                    col("cuisine_type"),
                ),
                items_agg["restaurant_key"] == col("cnpj"),
                "left",
            )
            .select(
                items_agg["restaurant_key"].alias("restaurant_cnpj"),
                col("restaurant_name"),
                col("city"),
                col("cuisine_type"),
                col("total_orders"),
                col("total_items_sold"),
                col("total_revenue_brl"),
                col("avg_order_value_brl"),
                col("avg_discount_brl"),
            )
            .withColumn("_computed_at", current_timestamp())
        )

        w = Window.partitionBy("restaurant_cnpj").orderBy(desc("_computed_at"))
        return (
            revenue_df
            .withColumn("_rn", row_number().over(w))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )


def register_gold_user_behavior() -> None:
    """Ported from notebooks/cross_domain/gold_user_behavior.ipynb. silver.users is
    deduped/merged by cpf, not user_id (ADR-005/docs/adr/007) — the row_number() guard
    stays as defense-in-depth."""
    gold_table = f"{CATALOG}.gold.user_behavior"
    silver_search = f"{CATALOG}.silver.search_events"
    silver_recommendations = f"{CATALOG}.silver.recommendations"
    silver_users = f"{CATALOG}.silver.users"

    @dp.table(name=gold_table, cluster_by=["user_id"], comment="Gold: user_behavior")
    def _gold():
        search_events = dp.read(silver_search)
        recommendations = dp.read(silver_recommendations)
        users = dp.read(silver_users)

        search_agg = (
            search_events.groupBy("user_id")
            .agg(
                count("search_id").alias("total_searches"),
                avg("result_count").alias("avg_results_per_search"),
                countDistinct("query_text").alias("distinct_queries"),
            )
        )

        rec_agg = (
            recommendations.groupBy("user_id")
            .agg(
                count("event_id").alias("total_rec_events"),
                sum(when(col("event_type") == "view", 1).otherwise(0)).alias("rec_views"),
                sum(when(col("event_type") == "click", 1).otherwise(0)).alias("rec_clicks"),
                sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("rec_purchases"),
                countDistinct("product_id").alias("distinct_products_seen"),
            )
        )

        behavior_df = (
            search_agg
            .join(rec_agg, on="user_id", how="full_outer")
            .join(
                users.select("user_id", "cpf", "city", "country"),
                on="user_id",
                how="left",
            )
            .withColumn("_computed_at", current_timestamp())
        )

        w = Window.partitionBy("user_id").orderBy(desc("_computed_at"))
        return (
            behavior_df
            .withColumn("_rn", row_number().over(w))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )


for _contract_path in sorted(CONTRACTS_DIR.glob("*.yml")):
    _contract = load_contract(_contract_path)
    _layers = _contract["table"]["layers"]
    _domain = _contract["table"]["name"]

    _bronze_table = register_bronze(_contract)

    if "silver" in _layers and _domain not in SILVER_EXCLUDED_DOMAINS:
        register_silver(_contract, _bronze_table)

register_silver_users()

register_gold_payments_by_status()
register_gold_payment_funnel()
register_gold_payment_lifecycle()
register_gold_driver_performance()
register_gold_revenue_per_restaurant()
register_gold_user_behavior()
