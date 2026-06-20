# KB: Medallion Architecture — sdd-kafka-databricks patterns
# Bronze/Silver/Gold conventions as implemented in pipelines/ubereats_pipeline.py

## One Lakeflow pipeline, contract-driven (v1.2.0)

There is no per-domain notebook anymore. `pipelines/ubereats_pipeline.py` loops over
every `contracts/*.yml` file and registers Bronze + (generic) Silver for each domain,
then adds the two hand-written exceptions and all 6 Gold tables:

```python
for _contract_path in sorted(CONTRACTS_DIR.glob("*.yml")):
    _contract = load_contract(_contract_path)
    _bronze_table = register_bronze(_contract)
    if "silver" in _contract["table"]["layers"] and _domain not in SILVER_EXCLUDED_DOMAINS:
        register_silver(_contract, _bronze_table)

register_silver_users()          # hand-written: FULL OUTER JOIN users_mongo + users_mssql by cpf
register_gold_payments_by_status()
register_gold_payment_funnel()
register_gold_payment_lifecycle()
register_gold_driver_performance()
register_gold_revenue_per_restaurant()
register_gold_user_behavior()
```

`SILVER_EXCLUDED_DOMAINS = {"users_mongo", "users_mssql"}` — these two still get a
generic Bronze table from the loop; only the generic Silver treatment is skipped
because `users` requires a cross-source join that the generic `register_silver()`
can't express.

## Bronze: always a streaming table, two source_modes

Bronze is **always** `@dp.table` + `spark.readStream` — never `@dp.materialized_view`,
in either mode:

```python
@dp.table(name=bronze_table, cluster_by=cluster_by, comment=f"Bronze: {domain}")
@dp.expect_all_or_drop(to_reject_expectations(contract, scope="bronze"))
def _bronze():
    if SOURCE_MODE == "kafka":        # prod's target mode
        ...spark.readStream.format("kafka")...from_avro(...)
    elif SOURCE_MODE == "volume":     # dev / free_edition, permanent
        ...spark.readStream.format("cloudFiles")...                # Auto Loader, not spark.read
```

`volume` reads newly-arrived Parquet files from `/Volumes/<catalog>/landing/kafka_export/`
via Auto Loader (`cloudFiles`) — a true incremental stream, the same way `kafka` mode
streams newly-arrived Kafka records. Neither mode needs a checkpoint-driven dedup: a
full materialized-view recompute (`volume`) or Lakeflow's own streaming-table model
(`kafka`) already makes re-running idempotent. See `docs/adr/007_pipeline_unification.md`.

`MAX_OFFSETS_OVERRIDES = {"order_items": 5000}` — `order_items` is 85% of total volume
(110,001 records); the shared `DEFAULT_MAX_OFFSETS=1000` would take ~110 micro-batches
for it alone (ADR-08).

## Silver: candidate → quarantine + clean (inverse predicate)

Every generic Silver domain follows the same 4-step shape inside `register_silver()`:

```python
@dp.temporary_view(name=candidate_view)
@dp.expect_all(to_warn_expectations(contract, scope="silver"))   # warn-only, never drops
def _candidate():
    return dp.read_stream(bronze_table)

row_predicate = quarantine_row_level_predicate(contract, scope="silver")

@dp.table(name=quarantine_table, comment=f"Quarantine: {domain}")
def _quarantine():
    candidate = dp.read_stream(candidate_view)
    return candidate.filter(row_predicate) if row_predicate else candidate.limit(0)

@dp.temporary_view(name=clean_view)
def _clean():
    candidate = dp.read_stream(candidate_view)
    return candidate.filter(f"NOT ({row_predicate})") if row_predicate else candidate

dp.create_streaming_table(name=silver_table, cluster_by=cluster_by)
dp.create_auto_cdc_flow(
    target=silver_table, source=clean_view, keys=[merge_key],
    sequence_by=col("__source_ts_ms"), stored_as_scd_type=1,
)
```

**Why `_clean()` re-filters `candidate_view` instead of anti-joining `quarantine_table`:**
Structured Streaming rejects a stream-stream `LEFT ANTI` join when the streaming
DataFrame is on the right side. Both Silver and Quarantine read only from
`candidate_view` — no table reads another table at the same pipeline level
(fixed in `docs/adr/...` — see git history "stop `_clean()` anti-joining
`quarantine_table`, use inverse predicate instead").

## users: the one hand-written Silver (FULL OUTER JOIN by cpf)

`register_silver_users()` generalizes the candidate/quarantine/clean shape above to
**two** quarantine causes instead of one (`missing_cpf`, computed pre-join;
`duplicate_user_id`, computed post-join), tagged via a nullable `_quarantine_reason`
column rather than a boolean predicate:

```python
mongo_raw = _prepped_users(bronze_mongo)          # filters __op != 'd', normalizes cpf_key
mssql_raw = _prepped_users(bronze_mssql)
missing = ...filter(cpf_key IS NULL)...           # -> quarantine, reason=missing_cpf
joined = _build_joined_users(_dedup_by_cpf(mongo_raw), _dedup_by_cpf(mssql_raw))  # FULL OUTER on cpf_key
# duplicate user_id (not the merge key, just carried for Gold joins) -> quarantine, reason=duplicate_user_id
```

`cpf` (not `user_id`) is `silver.users`'s real identity — `user_id` is only carried
for `gold_user_behavior`'s join with `search_events`/`recommendations`.

## Gold: full-recompute materialized views, not MERGE

All 6 Gold tables are plain `@dp.table` reading Silver in **batch** (`dp.read`, not
`dp.read_stream`):

```python
@dp.table(name=gold_table, cluster_by=["driver_id"], comment="Gold: driver_performance")
def _gold():
    ...groupBy(...).agg(...)...
```

No `MERGE INTO`/`create_auto_cdc_flow` here — a full recompute every run already
matches what the old `MERGE INTO ... WHEN MATCHED UPDATE SET *` amounted to, since
each run re-aggregates over the complete Silver table anyway.

### row_number() guard — defense-in-depth for dimension joins

3 of 6 Gold tables join a Silver dimension on a column that is **not** that table's
`merge_key` (`driver_performance` → `drivers.driver_id`, real key `uuid`;
`revenue_per_restaurant` → `restaurants.cnpj`, real key `uuid`;
`user_behavior` → `users.user_id`, real key `cpf`). Each keeps a window-dedup guard
right before the result is returned:

```python
w = Window.partitionBy("driver_id").orderBy(desc("_computed_at"))
return perf_df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")
```

This is defense-in-depth, not the primary control — see `kb/data-quality.md`'s
"check: unique" section for why the contract-level rule alone isn't enough today.
Full context: `docs/adr/005_gold_dimension_join_integrity.md`.

### Order matters: aggregate before joining (revenue_per_restaurant)

`orders_agg` (order-level `avg_order_value_brl`, `total_orders`) is computed
**before** joining `order_items` — joining first would repeat each order's
`total_amount` once per item it contains, inflating the average for restaurants
with multi-item orders.

## Bronze-only domains (8) feed Gold directly — no Silver

`gps_events, routes, receipts, support_tickets, products, menu_sections, ratings,
inventory` have `layers: [bronze]` in their contract — no `silver` entry, so the
loop's `if "silver" in _layers` check skips `register_silver()` for them. None of
the current 6 Gold tables read these yet; if one does in the future, read Bronze
directly for that domain (don't fabricate a Silver layer just to satisfy a lineage
convention that doesn't apply).

## Anti-patterns

| Never do | Why | Instead |
|---|---|---|
| `@dp.materialized_view` for Bronze | Breaks the append-only/immutable Bronze convention in both source_modes | `@dp.table` + `spark.readStream` (Auto Loader or Kafka) |
| Anti-join `clean_view` against `quarantine_table` | Stream-stream LEFT ANTI with streaming DF on the right is unsupported | Inverse predicate on `candidate_view` directly |
| Join `order_items` to `orders` before aggregating order-level metrics | Fans out `total_amount`/`avg_order_value_brl` per item | Aggregate per-order first, join the aggregate |
| Trust a Gold dimension join key without a uniqueness guarantee | Caused `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` once (`gold_user_behavior`) | `check: unique` contract rule (intent) + `row_number()` guard (actual enforcement) — see `kb/data-quality.md` |
| Add a Silver table for a Bronze-only domain "just in case" | Unnecessary lineage hop, nothing reads it | Read Bronze directly from Gold when/if needed |
