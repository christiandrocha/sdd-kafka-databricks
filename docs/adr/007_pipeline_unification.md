# ADR 007 — Unify Bronze, Silver, `silver_users`, Gold, and Quarantine into One Lakeflow Pipeline (Supersedes ADR-006's Exclusions)

**Status:** Accepted
**Date:** 2026-06-19

## Context

`ADR-006` migrated Bronze (20 domains) and 10 of the 11 Silver domains to a single Lakeflow
Declarative Pipeline (`pipelines/bronze_silver_dlt.py`), for the `dev`/`prod` targets only.
Its "Explicitly NOT migrated" section gave three reasons for leaving the rest in place:

1. **Gold** (6 notebooks) — already 1 notebook : 1 execution, so the lineage-grouping
   problem `ADR-006` solved doesn't apply; migrating would trade away explicit imperative
   control (two-stage aggregation, `row_number()` dedup guards) for no lineage benefit.
2. **`silver_users`** (`notebooks/pipeline_users.ipynb`) — same reasoning as Gold, plus its
   `FULL OUTER JOIN` + full-refresh (`mode("overwrite")`) doesn't fit Lakeflow's incremental
   streaming-table model.
3. **`free_edition`** — kept running `pipeline_bronze.ipynb`/`pipeline_silver.ipynb`
   (`source_mode=volume`) "until Lakeflow serverless pipeline support there is verified."
   CLAUDE.md and README separately cite this Kafka-reachability constraint as "ADR-05" —
   no `docs/adr/0XX` file with that content actually exists; it is a numbering gap a prior
   retrospective already flagged (`.claude/sdd/archive/GOLD_DIMENSION_JOIN_INTEGRITY/SHIPPED_2026-06-18.md`).

A later session working on `databricks.yml`'s serverless/schema fixes established that the
real workspace behind **all three** bundle targets (`dev`/`prod`/`free_edition`) is a single
Free Edition account. This directly undercuts reason 3: `free_edition` was never uniquely
constrained — every target has the same untested Kafka-from-serverless reachability
question, and every target already runs on the same serverless-only compute.

## Decision

Extend `pipelines/bronze_silver_dlt.py` (renamed `pipelines/ubereats_pipeline.py`) to also
register `silver_users`, its quarantine table, and all 6 Gold tables — 37 tables total, one
pipeline, one DAG. Fold `free_edition` into the same pipeline resource and the same 1-task
Job as `dev`/`prod`, differing only by `variables:` (`catalog`, `bronze_source_mode`,
`landing_base`). Retire all 8 legacy notebooks (`pipeline_bronze.ipynb`,
`pipeline_silver.ipynb`, `pipeline_users.ipynb`, and the 6 `notebooks/cross_domain/gold_*.ipynb`).

This supersedes all three bullets of `ADR-006`'s "Explicitly NOT migrated" section, and the
free_edition-specific Kafka-reachability premise informally cited as "ADR-05."
`docs/adr/006_lakeflow_migration.md` itself is left unedited — this ADR documents the
reversal going forward, the same way `006` documented its own reversal of `ADR-003` without
rewriting it.

### What changed since `ADR-006`, point by point

1. **Gold's "no lineage benefit" reasoning still holds** — but the goal of this migration is
   no longer "fix a lineage bug." It's architectural consistency: one resource covering every
   layer, instead of a pipeline-plus-7-notebook-tasks hybrid for `dev`/`prod` and a fully
   separate 37-task job for `free_edition`. Gold's imperative logic (two-stage aggregation,
   `row_number()` guards) is ported verbatim into `@dp.table` function bodies — nothing about
   the explicit control `ADR-006` wanted to preserve is lost, it just now lives inside a
   declarative table function instead of a notebook cell.
2. **`silver_users`'s FULL OUTER JOIN does fit Lakeflow** — `ADR-006` conflated "doesn't fit
   the *incremental streaming* model" with "doesn't fit Lakeflow at all." `@dp.table` without
   `create_streaming_table`/`create_auto_cdc_flow` is Lakeflow's materialized-view model: a
   full batch recompute on every pipeline run, which is exactly what the notebook's
   `mode("overwrite")` full refresh already was.
3. **`free_edition`'s exclusion reason no longer holds** — the workspace-sharing fact above
   means `free_edition` was never uniquely Kafka-constrained; it shares the same open question
   every target now has. `pipelines/ubereats_pipeline.py`'s `register_bronze()` gets a
   `source_mode` (`kafka` | `volume`) branch so all 3 targets can run the same code regardless
   of which way that open question resolves per target.

## Rationale

**Why now, not when `ADR-006` shipped:** the workspace-sharing fact is new information from a
later session, not available when `ADR-006` made its scoping decision. `ADR-006`'s exclusions
were the correct call given what was known at the time; this ADR reflects what changed.

**Why fold in `free_edition` rather than leave it on its own track:** keeping `dev`/`prod` and
`free_edition` structurally different already cost `ADR-006` a known asymmetry (different code
for Bronze/Silver, not just different compute). Extending that asymmetry to Gold/`silver_users`
as well would double down on a split that no longer has a justifying reason behind it.

**Why `silver_users`'s quarantine loses its append-only history:** a `@dp.table` recompute can
only ever hold currently-quarantined rows, not a running history. For a project-stage dataset
(~700 `users` records), this is an acceptable, deliberate trade-off — not an oversight.

## Alternatives Considered

1. **Migrate Gold + `silver_users` for `dev`/`prod` only, leave `free_edition` on its 37-task
   job** (Approach B) — rejected: re-introduces the exact `dev`/`prod` vs. `free_edition`
   asymmetry `ADR-006` already created for Bronze/Silver, now for Gold/`silver_users` too, and
   `free_edition`'s legacy notebooks would still need revisiting later regardless.
2. **Rename + fold in `silver_users` only, leave the 6 Gold notebooks as separate Job tasks**
   (Approach C) — rejected: doesn't achieve "one resource covering every layer," and Gold→Silver
   lineage was never actually confirmed broken, so leaving Gold out wouldn't even preserve a
   real benefit, just defer a consistency improvement the user explicitly wanted now.
3. **Generic, contract-driven Gold registration loop** mirroring Bronze/Silver's
   `contracts/*.yml` pattern — rejected: Gold's 6 transforms have non-uniform join keys, dedup
   windows, and aggregation shapes; forcing a shared abstraction over 6 bespoke cases isn't
   justified (YAGNI).

## Consequences

**Positive:** all 37 tables are now one lineage DAG in Unity Catalog, with Gold's upstream
edges resolving directly to `silver.*` (no Bronze-direct edges, confirmed by reading all 6
Gold notebooks during design). `databricks.yml` shrinks from 878 to ~150 lines — the 37
`task_definitions` anchors, the `lakeflow_tasks` anchor, and the `serverless_tasks` anchor
all disappear, replaced by one `pipeline_resource`/`pipeline_task` anchor shared verbatim
across `dev`/`prod`/`free_edition`. The 6 Gold `depends_on` task lists in `databricks.yml`
also disappear — Lakeflow infers those dependencies automatically from each Gold function's
`dp.read(silver_*)` calls.

**Negative:** `quarantine.users` loses its append-only history (confirmed acceptable — see
Rationale). The `source_mode=volume` branch for Bronze and the live Kafka-reachability
question for `free_edition`/`dev`/`prod` remain unverified against a real workspace as of this
ADR — see `.claude/sdd/reports/BUILD_REPORT_PIPELINE_UNIFICATION.md` for what was and wasn't
validated. (Addendum below resolves this for `dev`/`free_edition`; `prod` remains open.)
`scripts/preflight_unity_catalog.sh`'s `checkpoints` schema/Volumes (`bronze`/`silver`)
become unused infrastructure once `free_edition` stops running the checkpointed legacy
notebooks — left in place as a follow-up cleanup, not addressed here.

## Addendum (2026-06-19) — `bronze_source_mode` is a permanent per-target decision, not a temporary workaround

When this ADR was first accepted, `dev`'s `bronze_source_mode` was left at `kafka` pending
verification — sharing the same open Kafka-reachability question as `free_edition`. That
question is now resolved as a deliberate, permanent per-target policy rather than left open:

**Decision:** `dev` and `free_edition` set `bronze_source_mode: volume` permanently — not as
a stopgap until Kafka reachability is confirmed, but as the project's correct mode for both,
because both run on the same Databricks workspace/serverless compute that cannot reach the
self-hosted Kafka broker. `prod` keeps `bronze_source_mode: kafka`, but as a documented
*target* mode, not a confirmed-working one — it reflects where `prod` is headed once Kafka
runs on infrastructure Databricks can actually reach (e.g. a managed/cloud-hosted broker, or
network peering into the self-hosted one), not where `prod` stands today.
`databricks.yml`'s comments were updated to stop describing `volume` as `free_edition`'s
special case and `kafka` as the dev/prod default — it's the reverse: `volume` is the shared
default for `dev`/`free_edition`, and `kafka` is `prod`-only and aspirational.

**Rationale:** `register_bronze()`'s `source_mode` branch was always designed to support both
modes (`DESIGN_PIPELINE_UNIFICATION.md` Decision 2) specifically because this project's real
Databricks workspace cannot reach a self-hosted Kafka broker from serverless compute — that
constraint was never `free_edition`-specific, since `dev` runs on the exact same workspace.
`prod` is the one target where the constraint isn't assumed to hold forever, since `prod` is
expected to eventually run against different, Kafka-reachable infrastructure.

**Consequences:** this resolves DEFINE's Assumption A-001 for `dev`/`free_edition` — it is no
longer "unverified," it is "volume by design," independent of whether Kafka happens to be
reachable from either target's compute. `prod`'s `kafka` setting remains genuinely unverified
and should not be treated as load-bearing until `prod` actually runs against Kafka-reachable
infrastructure — deploying `prod` before then would need `bronze_source_mode: volume`
overridden at deploy time.

## Addendum 2 (2026-06-19) — `register_bronze()` decorator fix and quarantine→silver cycle fix

Two defects surfaced once the unified pipeline was checked against the real Lakeflow DAG
validator, both fixed in `pipelines/ubereats_pipeline.py` without reopening Decisions 2/3:

**Bronze decorator for `source_mode=volume`:** `register_bronze()` decorated every Bronze
table with `@dp.table(...)` regardless of mode, on the assumption (Decision 2's rationale)
that a `@dp.table` function can return either a streaming or a static DataFrame and have
Lakeflow infer the table type from the body. In practice the static (`spark.read`) path used
by `volume` mode needs the explicit `@dp.materialized_view(...)` decorator — `@dp.table`
without it is reserved for the `spark.readStream` (`kafka` mode) path. Fixed by selecting the
decorator per-domain at registration time: `dp.materialized_view if SOURCE_MODE == "volume"
else dp.table`. This affects `dev`/`free_edition` (both `volume`) for all 20 Bronze tables;
`prod` (`kafka`) is unaffected.

**Quarantine→Silver cycle for `check: unique` domains:** `_unique_violations()` (Decision 3)
read `spark.read.table(silver_table)` — the *target* Silver table — from inside the
quarantine table's body, to compare the current candidate batch against already-persisted
Silver state. But `silver_table` is itself populated from `clean_view`, which reads from
`quarantine_table` (left-anti join) — so for any domain using `check: unique`
(`restaurants.cnpj`, `drivers.driver_id`; see `docs/adr/005_gold_dimension_join_integrity.md`),
this created `quarantine → silver_table → clean_view → quarantine`, a cycle in the DLT DAG.
Fixed by confining the uniqueness check entirely to the candidate batch: `_unique_violations()`
no longer takes a `silver_table` argument or reads any persisted table — it flags any `field`
value that maps to more than one distinct `merge_key` within the current candidate set via a
self-join (`groupBy(field).agg(countDistinct(merge_key))`). This preserves the inverse-predicate
quarantine convention (Silver and Quarantine both read only from `<domain>_silver_candidate`;
neither reads a same-level table) at the cost of not catching a duplicate that arrives in a
later run after its first occurrence already settled into Silver — accepted as defense-in-depth
already exists downstream (the `row_number()` guards in `register_gold_driver_performance()`/
`register_gold_revenue_per_restaurant()`).

Both fixes verified with `ruff check` (no new findings) and the existing 196-test suite
(`pytest tests/test_contracts.py tests/test_dlt_adapter.py`), which has no live-workspace
DAG-cycle check — the cycle itself was only observable via the real Lakeflow pipeline graph,
not unit tests.

## Addendum 3 (2026-06-19) — `timestampNtz` Delta feature error on Bronze tables (`source_mode=volume`)

A live `dev` run (`source_mode=volume`) failed Delta table creation with an error requiring
the `timestampNtz` table feature to be manually enabled. Root cause was a type mismatch
between two independently-correct-looking pieces of code:

**Where the bug actually was:** `contracts/spark_schema.py`'s `to_struct_type()` already maps
the contract's `timestamp` field type to PySpark's `TimestampType` correctly — but that
function is never called anywhere in `pipelines/ubereats_pipeline.py`; Bronze relies entirely
on schema inference (`from_avro` for `kafka` mode, `spark.read.format("parquet")` for `volume`
mode), so the correct mapping in `spark_schema.py` was dead code with no effect on the actual
bug. The real defect was in `scripts/export_kafka_to_volume.py`'s `_PYARROW_TYPE_MAP`:
`"timestamp": pa.timestamp("ms")` (no `tz=`) writes a timezone-naive Arrow/Parquet column
(`isAdjustedToUTC=false`), which Spark 3.4+ reads back as `TimestampNTZType` rather than
`TimestampType` — Delta rejects writing a `TIMESTAMP_NTZ` column unless the `timestampNtz`
table feature is explicitly enabled, hence the error on every Bronze table with a `timestamp`
contract field, in `volume` mode.

**Fix, two layers:**
1. `scripts/export_kafka_to_volume.py` — `pa.timestamp("ms")` → `pa.timestamp("ms", tz="UTC")`,
   so future exports write Parquet with `isAdjustedToUTC=true` and Spark infers `TimestampType`
   directly. Doesn't help data already exported to the landing Volume under the old schema.
2. `pipelines/ubereats_pipeline.py`'s `register_bronze()` — after building the Bronze
   DataFrame (either branch), explicitly `.cast("timestamp")` every field the contract declares
   `type: timestamp`, sourced from `contract["schema"]`. This makes Bronze's output type the
   contract's stated type regardless of how the upstream source (Avro logical type or Parquet
   metadata) happened to encode it — covers both `kafka` (`from_avro`'s `local-timestamp-*`
   logical types decode to `TimestampNTZType` the same way) and `volume`, and unblocks the
   already-exported Volume data without needing a re-export/re-upload cycle.

**Why not Option 2 (`TBLPROPERTIES 'delta.feature.timestampNtz' = 'supported'`):** that would
accept `TIMESTAMP_NTZ` as the column's actual stored type, which is a real semantic difference
(no timezone) from what every contract declares (`type: timestamp`, mapped to UTC-based
`TimestampType` everywhere else in the codebase, including `_ingested_at` via
`current_timestamp()`). Casting at the source keeps one consistent timestamp semantic across
Bronze regardless of source_mode, instead of letting two source modes produce two different
column types for the same logical field.

Verified with `ruff check` (no new findings beyond the pre-existing `F821 spark` baseline) and
the 196-test suite. Not verified against a live Delta write in this session — the next
`dev` deploy + run is what confirms the `timestampNtz` error is actually gone.

A live `dev` run after this fix confirmed `timestampNtz` was gone but surfaced a new failure
(see Addendum 4) caused by Addendum 2's `@dp.materialized_view()` change interacting badly
with `register_silver()`'s unconditional `dp.read_stream()` calls.

## Addendum 4 (2026-06-19) — `dp.read_stream()` on a non-append-only volume-mode Bronze table

Live `dev` run failed every one of the 10 generic Silver domains' quarantine tables with
`"we detected an update or delete to one or more rows in the source table. Streaming tables
may only use append-only streaming sources"` and `"Update ... has failed due to a non-append
only streaming source."` Root cause: `register_silver()`'s `_candidate()` called
`dp.read_stream(bronze_table)` unconditionally — fine for `kafka` mode, where `bronze_table` is
a genuine incremental Kafka streaming table, but broken for `volume` mode after Addendum 2
made `bronze_table` a `@dp.materialized_view()` (fully recomputed every run, not append-only).
Reading a non-append-only dataset via `dp.read_stream()` is exactly what Structured Streaming
forbids. `_quarantine()` and `_clean()` had the same `dp.read_stream()` calls one level down,
failing for the same reason. `register_silver_users()` was never affected — it already used
batch `dp.read()` throughout, for the same underlying reason (its FULL OUTER JOIN is a full
recompute by design).

**Fix:** `register_silver()` now picks `_read = dp.read_stream if SOURCE_MODE == "kafka" else
dp.read` once per domain, and `_candidate()`/`_quarantine()`/`_clean()` all call `_read(...)`
instead of hardcoding `dp.read_stream(...)`. `create_streaming_table`/`create_auto_cdc_flow`
for the `silver_table` target are left unchanged — Databricks' `create_auto_cdc_flow` docs
don't state a hard streaming-source requirement for the `source` argument (only
`create_auto_cdc_from_snapshot_flow` is documented as the dedicated batch/snapshot variant);
verifying empirically via the next `dev` run rather than switching APIs preemptively, to avoid
a second speculative architecture change without a confirmed need.

The next `dev` run confirmed the speculative need: `create_auto_cdc_flow` does require a
streaming source after all (see Addendum 5).

## Addendum 5 (2026-06-19) — `create_auto_cdc_flow` rejects `clean_view` as non-streaming in volume mode

Addendum 4's fix unblocked the quarantine tables, but the next `dev` run failed all 10
generic Silver targets with `pyspark.errors.exceptions.captured.AnalysisException: View
'<domain>_silver_clean' is not a streaming view and must be referenced using read` —
`create_auto_cdc_flow`'s `source` *does* require a streaming view, resolving Addendum 4's
open question. `clean_view` is a batch temporary view in `volume` mode (it reads
`candidate_view`/`quarantine_table` via `dp.read()`, per Addendum 4), so `create_auto_cdc_flow`
rejects it outright rather than silently degrading.

**Fix:** for `volume` mode, swap `create_auto_cdc_flow` for `create_auto_cdc_from_snapshot_flow`
— Databricks' documented batch/snapshot AUTO CDC variant. It takes the same `target`/`source`/
`keys`/`stored_as_scd_type` arguments but no `sequence_by`: instead of ordering CDC events by a
sequence column, it treats each pipeline run's `source` as one complete snapshot and diffs it
against the previous run's snapshot — which matches `volume` mode's actual semantics (full
recompute = one full snapshot per run) more precisely than `create_auto_cdc_flow` ever did.
`kafka` mode is unaffected — it keeps `create_auto_cdc_flow` with `sequence_by=col
("__source_ts_ms")`, since its `clean_view` genuinely is a streaming view fed by a real Kafka
stream. Rejected alternative: setting the pipeline-level Spark conf
`pipelines.incompatibleViewCheck.enabled = false` (named directly in the error message) to
suppress the check — would have "fixed" the symptom while keeping the semantic mismatch
(treating a full snapshot as an incremental CDC stream) the API itself warns against; the
snapshot-flow variant has no such mismatch to suppress.

Verified with `ruff check` (no new findings beyond the `F821 spark` baseline) and the
196-test suite. Confirmation that `create_auto_cdc_from_snapshot_flow` actually succeeds
against the real `dev` workspace is the next run's job — this is the third fix applied
without a successful end-to-end `dev` run yet, each prior one having unblocked exactly one
failure and surfaced the next.

## Addendum 6 (2026-06-19) — check: unique removed from Quarantine; Bronze reverted to a true streaming table via Auto Loader

Two more fixes landed before a clean `dev` run was achieved, both requested directly rather
than discovered from a live failure (the first pre-empts a real but not-yet-hit `kafka`-mode
bug; the second reverses Addendum 2's decorator choice):

**`check: unique` no longer gates Quarantine.** `_unique_violations()` (Addendum 2's self-join
version) calls `groupBy(field).agg(countDistinct(merge_key))` on `candidate_df` — in `kafka`
mode `candidate_df` is a genuine unbounded streaming DataFrame, and `COUNT(DISTINCT ...)`
inside a streaming `groupBy/agg` is rejected without a watermark
(`"COUNT(DISTINCT uuid) is not supported in streaming without watermark"`). This was latent —
`dev` never hit it because it runs in `volume` mode, where the aggregation was batch — but
real for `prod`'s `kafka` mode. Fixed by removing `_unique_violations()` and
`unique_check_fields()` entirely: `_quarantine()` now only applies the row-level predicate.
Architecturally this is the more correct shape anyway — `check: unique` is a Silver
merge-time property (Silver already dedupes by `merge_key`), not a Bronze→Silver streaming
quarantine gate; a row can look "duplicate" pre-merge and still resolve to a valid Silver row.
The `row_number()` guards in `register_gold_driver_performance()`/
`register_gold_revenue_per_restaurant()` remain the actual defense-in-depth for the Gold
dimension joins this contract rule protects (`docs/adr/005_gold_dimension_join_integrity.md`)
— unchanged by this fix. The YAML contracts (`drivers.yml`/`restaurants.yml`) still declare
`check: unique`; it's just no longer translated into pipeline enforcement.

**Bronze reverted from `@dp.materialized_view()` back to `@dp.table()` (true streaming) in
volume mode**, replacing Addendum 2's batch `spark.read.format("parquet")` with Auto Loader:
`spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet")`. Medallion
convention treats Bronze as append-only and immutable; a full-batch-recompute materialized
view technically violates that even though it was functionally adequate. Auto Loader lets
volume mode stream newly-arrived files the same way kafka mode streams newly-arrived Kafka
records, so Bronze no longer needs two different fundamental table types depending on mode —
`@dp.table()` is now unconditional in `register_bronze()`. Auto Loader's schema inference
needs its own checkpoint (`cloudFiles.schemaLocation`), separate from the streaming-table
checkpoint Lakeflow already self-manages — this is the first real use of the
`checkpoints/bronze` Volume `scripts/preflight_unity_catalog.sh` provisions (previously
documented as unused infrastructure, CLAUDE.md's Unity Catalog structure section). `register_silver()`'s
`SOURCE_MODE`-branched reads (Addendum 4) and CDC flow choice (Addendum 5) are **not**
reverted — `dp.read()` (batch) against a now-genuinely-streaming Bronze table is still valid
(Gold already does the same thing reading Silver's streaming tables elsewhere in this file),
so Silver keeps treating each `volume`-mode run as one full snapshot of Bronze's current
accumulated state. Reverting that too was judged out of scope for this fix and not asked for.

**Known follow-up, not fixed here:** `scripts/export_kafka_to_volume.py` always overwrites
the same `data.parquet` path per domain. Auto Loader's default file-tracking will not
reprocess a modified file at an already-seen path, so re-exporting after the first successful
Bronze ingestion would silently not propagate — would need either unique per-export file
names or `cloudFiles.allowOverwrites = true`. Not exercised by this session's single Volume
snapshot, so left as a flagged gap rather than fixed speculatively.

**Consequence for the next deploy:** this is the second decorator reversal in the same
session (Addendum 2 made Bronze `MATERIALIZED_VIEW`; this addendum makes it `STREAMING_TABLE`
again) — the 20 Bronze tables currently persisted in `ubereats_dev` as `MATERIALIZED_VIEW`
will hit the same `CANNOT_CHANGE_DATASET_TYPE` conflict already identified for the 10 generic
`quarantine.*` tables (Addendum 4 made those `MATERIALIZED_VIEW`; they're still persisted as
`STREAMING_TABLE` from before). Both sets need to be dropped before the next run for Lakeflow
to recreate them with the correct type — confirmed via `databricks tables get` against the
live `dev` catalog before writing this addendum, not assumed.

Verified with `ruff check` (12 findings, the same `F821 spark` baseline plus one for the new
`CHECKPOINTS_BASE` constant) and the 193-test suite (unchanged by this addendum). Not yet
verified against a live Auto Loader read — pending the drop + next run.

The drop + run did confirm Bronze and Quarantine's type conflicts were resolved (both
recreated with the correct type, observed live), but surfaced a new, unrelated failure in
Silver — see Addendum 7.

## Addendum 7 (2026-06-19) — create_auto_cdc_from_snapshot_flow rejects order_status's duplicate status_id keys; full revert to streaming Silver

The `dev` run after Addendum 6's drop got past Bronze and Quarantine cleanly (confirmed live:
`bronze.*` created as `STREAMING_TABLE`, `quarantine.*` as `MATERIALIZED_VIEW`, no dataset-type
conflicts) and failed in Silver instead:
`[APPLY_CHANGES_FROM_SNAPSHOT_ERROR.DUPLICATE_KEY_VIOLATION] Found 468 rows for key
'{"status_id":"1"}' ... Expected at most 1 row per key.` `contracts/order_status.yml` declares
`merge_key: status_id` with no `unique` rule — `status_id` is a low-cardinality status *code*
(pending/confirmed/etc., per the domain's name), not a unique row id, contrary to what
CLAUDE.md's domain-map table implies by labeling it "PK". `create_auto_cdc_from_snapshot_flow`
(Addendum 5) requires each snapshot to already have exactly one row per key and has no
`sequence_by` to break ties on duplicates — it surfaced this pre-existing data characteristic
for the first time, where `create_auto_cdc_flow` (used everywhere before Addendum 5, and by
`kafka` mode throughout) never had a problem with it: CDC flows tolerate multiple rows per key
within a batch, picking the latest via `sequence_by`.

**Fix:** full revert of Addendum 4 and 5. Now that Bronze is a true streaming table in both
modes (Addendum 6), there is no remaining reason for `register_silver()` to branch by
`SOURCE_MODE` at all — `_candidate()`/`_quarantine()`/`_clean()` go back to unconditional
`dp.read_stream()`, and the Silver flow goes back to unconditional `create_auto_cdc_flow(...,
sequence_by=col("__source_ts_ms"), stored_as_scd_type=1)`. This is the simpler, original
shape from before today's whole Bronze/Silver detour, now legitimately restorable because the
thing that originally forced the detour (Bronze being a batch materialized view in `volume`
mode) no longer exists.

**Consequence for the next deploy:** `_quarantine()`'s body reverting to `dp.read_stream()`
means the 10 generic `quarantine.<domain>` tables — just recreated as `MATERIALIZED_VIEW` by
the previous run — need to go back to `STREAMING_TABLE`, hitting `CANNOT_CHANGE_DATASET_TYPE`
a third time unless dropped first. `silver.*` tables' underlying type (`STREAMING_TABLE`, via
`create_streaming_table()`) is unchanged by this revert — only which flow feeds them changes
(`create_auto_cdc_from_snapshot_flow` → `create_auto_cdc_flow`), which is a flow redefinition,
not a dataset-type change, so it was judged unlikely to need its own drop; the next run is
what actually confirms this rather than a preemptive drop of `silver.*` too.

Verified with `ruff check` (no new findings) and the 193-test suite. Confirmation that
`create_auto_cdc_flow` cleanly takes over the existing `silver.*` streaming tables from
`create_auto_cdc_from_snapshot_flow` is the next run's job.

## See also

`.claude/sdd/features/DESIGN_PIPELINE_UNIFICATION.md` — full design, file manifest, and code
patterns behind the decisions above. `docs/adr/006_lakeflow_migration.md` — the ADR this one
supersedes the exclusions of.
