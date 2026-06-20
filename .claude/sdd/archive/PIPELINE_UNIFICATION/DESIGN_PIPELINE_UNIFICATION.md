# DESIGN: PIPELINE_UNIFICATION

> Technical design for consolidating Bronze, Silver, `silver_users`, Gold, and Quarantine into one Lakeflow pipeline (`ubereats_pipeline`), shared by `dev`/`prod`/`free_edition`

## Metadata

| Attribute | Value |
|---|---|
| **Feature** | PIPELINE_UNIFICATION |
| **Date** | 2026-06-19 |
| **Author** | design-agent |
| **DEFINE** | [DEFINE_PIPELINE_UNIFICATION.md](./DEFINE_PIPELINE_UNIFICATION.md) |
| **Status** | ✅ Shipped |

---

## Architecture Overview

```text
┌────────────────────────────────────────────────────────────────────────────────┐
│        pipelines/ubereats_pipeline.py  (ONE module, ONE pipeline resource,     │
│        shared by dev / prod / free_edition — only `source_mode` differs)      │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  [Kafka topic]  ──┐                                                            │
│  [Landing Volume] ─┴─ source_mode ─→ [Bronze @dp.table × 20] ──┐               │
│                                                                  │               │
│                              ┌───────────────────────────────────┘               │
│                              ▼                                                   │
│         [Silver @dp.table + create_auto_cdc_flow × 10 generic domains]          │
│         [quarantine.<domain> @dp.table × 10 — inverse-predicate]               │
│                              │                                                   │
│  bronze.users_mongo ──┐      │                                                   │
│  bronze.users_mssql ──┴──► [silver.users @dp.table — FULL OUTER JOIN,          │
│                              full batch recompute]                              │
│                            [quarantine.users @dp.table — missing_cpf +         │
│                              duplicate_user_id, inverse-predicate]              │
│                              │                                                   │
│                              ▼                                                   │
│         [Gold @dp.table × 6 — dp.read(silver_*), full batch recompute,         │
│          dependency graph inferred automatically from dp.read() calls]         │
│                                                                                  │
└────────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
              One Job, one pipeline_task, per target (dev / prod / free_edition)
              — exists purely for scheduling + email_notifications on failure
```

---

## Components

| Component | Purpose | Technology |
|-----------|---------|------------|
| `pipelines/ubereats_pipeline.py` | Registers all 37 data tables (Bronze ×20, Silver ×10 + `silver_users`, Quarantine ×11, Gold ×6) as one Lakeflow pipeline | PySpark, `pyspark.pipelines` (`@dp.table`, `@dp.temporary_view`, `create_auto_cdc_flow`) |
| `contracts/dlt_adapter.py` | Translates `contracts/*.yml` quality rules into `@dp.expect_all*`/quarantine predicates | Pure Python — reused unchanged for the 10 generic Silver domains, not used for `silver_users` (no YAML contract) |
| `contracts/loader.py` | Loads + validates `contracts/*.yml` | Pure Python — unchanged |
| `databricks.yml` | DABs bundle: one pipeline resource + one 1-task Job per target | YAML, Databricks Asset Bundles |
| `docs/adr/007_pipeline_unification.md` | Documents the reversal of `ADR-006`'s "Explicitly NOT migrated" section and the `free_edition`/Kafka-reachability premise CLAUDE.md/README cite as "ADR-05" | Markdown ADR |
| `.github/workflows/ci.yml` | `databricks bundle validate` gate | GitHub Actions — extended to validate `free_edition` too, since that target's resource shape changes most |

---

## Key Decisions

### Decision 1: Rename and extend the module in place, not a new file

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-19 |

**Context:** `pipelines/bronze_silver_dlt.py` already loops over `contracts/*.yml` registering Bronze+Silver. The brainstorm/DEFINE both frame this as "renamed to `ubereats_pipeline.py`," not a parallel module.

**Choice:** `git mv pipelines/bronze_silver_dlt.py pipelines/ubereats_pipeline.py`, then add `register_silver_users()` and 6 `register_gold_*()` functions to the same file, called after the existing Bronze/Silver `contracts/*.yml` loop.

**Rationale:** The existing loop, `CATALOG`/`KAFKA_BOOTSTRAP`/`MAX_OFFSETS_OVERRIDES` module-level config, and `_avro_schema_str`/`_unique_violations` helpers are all still needed by Bronze/Silver and should not be duplicated into a second file.

**Alternatives Rejected:**
1. New file `pipelines/gold_dlt.py` alongside the renamed Bronze/Silver file — rejected by DEFINE (Out of Scope: "Splitting Gold into multiple Python files").
2. Leave the filename as `bronze_silver_dlt.py` and just add functions — rejected: the name would misdescribe a module that now also owns Gold and `silver_users`, and the DABs resource name (`ubereats_pipeline`) should match the file driving it.

**Consequences:**
- One file grows to ~400-500 lines (vs. today's 149) — acceptable for 37 table registrations in one project-stage codebase; revisit only if it becomes genuinely unwieldy (DEFINE's "Could Add Later" escape hatch).
- `git mv` preserves file history; the rename shows as a rename in `git log --follow`, not a delete+create.

---

### Decision 2: Bronze gets a `source_mode` branch inside `register_bronze()`, not duplicated registration functions

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-19 |

**Context:** `free_edition` currently reads Bronze from a landing Volume (`pipeline_bronze.ipynb`'s `source_mode=volume` branch) because serverless compute may not reach the self-hosted Kafka broker (`ADR-05`, informally referenced in CLAUDE.md/README — no `docs/adr/0XX` file exists for it). Folding `free_edition` into the unified pipeline means `register_bronze()` must support both.

**Choice:** `register_bronze()` reads a pipeline-level `ubereats.source_mode` configuration value (`kafka` | `volume`) and branches inside the same `@dp.table` function body:

```python
SOURCE_MODE = spark.conf.get("ubereats.source_mode", "kafka")
VOLUME_BASE = spark.conf.get("ubereats.volume_base", "/Volumes/ubereats_dev/landing/kafka_export")

def register_bronze(contract: dict) -> str:
    domain = contract["table"]["name"]
    ...
    @dp.table(name=bronze_table, cluster_by=cluster_by, comment=f"Bronze: {domain}")
    @dp.expect_all_or_drop(to_reject_expectations(contract, scope="bronze"))
    def _bronze():
        if SOURCE_MODE == "kafka":
            avro_schema_str = _avro_schema_str(kafka_topic)
            raw_stream = (
                spark.readStream.format("kafka")
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
                spark.read.format("parquet")
                .load(f"{VOLUME_BASE}/{domain}")
                .withColumn("_ingested_at", current_timestamp())
            )
        raise ValueError(f"Unknown source_mode: {SOURCE_MODE!r}")
    return bronze_table
```

**Rationale:** A `@dp.table` function can return either a streaming DataFrame (Lakeflow registers it as an incrementally-appended streaming table) or a static one (Lakeflow registers it as a fully-recomputed materialized view) — both are legitimate within the same decorator, so one function covers both modes without a second registration path. `VOLUME_BASE`/domain-name interpolation replaces the 20 separate `volume_path: ${var.landing_base}/<domain>` YAML parameters that existed only because the notebook took `volume_path` as a per-task widget.

**Alternatives Rejected:**
1. Two functions (`register_bronze_kafka`, `register_bronze_volume`), called conditionally from the top-level loop — rejected: doubles the function surface for a 6-line branch, and the `@dp.expect_all_or_drop` decorator/cluster_by/comment would have to be duplicated too.
2. Resolve `source_mode` once at the top of the file and only ever define one registration path per deploy — rejected: would require regenerating/re-deploying the bundle to switch modes instead of just changing a pipeline `configuration` variable, which is how `bronze_source_mode` already works today.

**Consequences:**
- `kafka` mode keeps incremental, checkpointed-by-Lakeflow ingestion (no explicit `checkpointLocation` needed — Lakeflow self-manages it, unlike the legacy notebook's `checkpoint_path` widget).
- `volume` mode is a full materialized-view recompute of the entire Parquet snapshot on every pipeline run — simpler than the notebook's hand-written `MERGE INTO ... WHEN NOT MATCHED`, and equally idempotent (a fresh full recompute can't accumulate duplicates).
- This is the one part of the design that stays unverified until deploy (DEFINE Assumption A-001) — both branches must exist regardless of which one ultimately runs in `free_edition`.

---

### Decision 3: `silver.users` becomes one `@dp.table` (FULL OUTER JOIN, full recompute); `quarantine.users` derived as the inverse predicate of the same candidate set

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-19 |

**Context:** `notebooks/pipeline_users.ipynb` has no YAML contract (CLAUDE.md), so it can't reuse `contracts/dlt_adapter.py`'s rule-translation functions directly. It also quarantines for two distinct reasons (`missing_cpf`, pre-join; `duplicate_user_id`, post-join) where the 10 generic Silver domains only ever have one quarantine cause class derived from one contract.

**Choice:** Two `@dp.table` functions, no YAML contract, logic ported verbatim from the notebook's `normalize_cpf`/`dedup_by_cpf`/`to_quarantine_shape` helpers:

```python
def register_silver_users() -> None:
    silver_table = f"{CATALOG}.silver.users"
    quarantine_table = f"{CATALOG}.quarantine.users"
    bronze_mongo = f"{CATALOG}.bronze.users_mongo"
    bronze_mssql = f"{CATALOG}.bronze.users_mssql"

    def _prepped(bronze_table, source):
        return (
            dp.read(bronze_table)
            .filter(col("__op") != "d")
            .withColumn("cpf_key", regexp_replace(col("cpf"), r"[.\-]", ""))
        )

    @dp.temporary_view(name="users_joined_candidate")
    def _joined_candidate():
        mongo_raw = _prepped(bronze_mongo, "mongo")
        mssql_raw = _prepped(bronze_mssql, "mssql")
        mongo_missing = to_quarantine_shape(mongo_raw.filter(col("cpf_key").isNull()), "mongo")
        mssql_missing = to_quarantine_shape(mssql_raw.filter(col("cpf_key").isNull()), "mssql")

        mongo_df = dedup_by_cpf(mongo_raw.filter(col("cpf_key").isNotNull()))
        mssql_df = dedup_by_cpf(mssql_raw.filter(col("cpf_key").isNotNull()))
        joined = build_joined_users(mongo_df, mssql_df)  # FULL OUTER JOIN, same projection as today
        return joined.withColumn("_quarantine_reason", lit(None).cast("string")) \
            .unionByName(mongo_missing, allowMissingColumns=True) \
            .unionByName(mssql_missing, allowMissingColumns=True)

    @dp.table(name=quarantine_table, comment="Quarantine: users")
    def _quarantine_users():
        candidate = dp.read("users_joined_candidate")
        dup_user_ids = (
            candidate.filter(col("user_id").isNotNull() & col("_quarantine_reason").isNull())
            .groupBy("user_id").count().filter("count > 1").select("user_id")
        )
        missing_cpf = candidate.filter(col("_quarantine_reason").isNotNull())
        duplicate_user_id = (
            candidate.join(dup_user_ids, "user_id", "left_semi")
            .withColumn("_quarantine_reason", lit("duplicate_user_id"))
        )
        return missing_cpf.unionByName(duplicate_user_id, allowMissingColumns=True) \
            .withColumn("_quarantine_ts", current_timestamp())

    @dp.table(name=silver_table, cluster_by=["cpf"], comment="Silver: users")
    def _silver_users():
        candidate = dp.read("users_joined_candidate").filter(col("_quarantine_reason").isNull())
        bad = dp.read(quarantine_table).select("user_id").filter(col("user_id").isNotNull()).distinct()
        return candidate.join(bad, "user_id", "left_anti").withColumn("_merged_at", current_timestamp())
```

(`to_quarantine_shape`, `dedup_by_cpf`, `build_joined_users` are module-level helpers ported verbatim from `pipeline_users.ipynb`'s `normalize_cpf`/`dedup_by_cpf`/`to_quarantine_shape`/join-and-`coalesce` cell — omitted here for brevity, included in full in the Build phase.)

**Rationale:** This mirrors the existing inverse-predicate convention (`quarantine = candidate.filter(bad)`, `silver = candidate.left_anti.join(bad)`) even though the "bad" predicate here spans two different causes computed at two different pipeline stages (pre-join missing-CPF, post-join duplicate-`user_id`) — exactly the convention CLAUDE.md asks this migration to reuse, generalized rather than copied verbatim, since `silver_users` was never a single-rule case to begin with.

**Alternatives Rejected:**
1. Recompute `dup_user_ids` independently inside `_silver_users()` instead of reading `quarantine_table` back — rejected: works, but breaks the "quarantine is the single source of truth for what got excluded" invariant the other 10 domains rely on, and risks the two computations silently drifting apart later.
2. Force `silver_users` into `create_streaming_table`/`create_auto_cdc_flow` to match the other 10 domains structurally — rejected by DEFINE (Decision 4: doesn't fit a FULL OUTER JOIN + full-refresh).

**Consequences:**
- `quarantine.users` loses its append-only history (confirmed acceptable in DEFINE/brainstorm) — a `@dp.table` recompute only ever holds currently-quarantined rows.
- `_silver_users()` reading `dp.read(quarantine_table)` is a cross-table read inside a `@dp.table` body, the same technique `_unique_violations()` already uses (`spark.read.table(silver_table)`) — not a new pattern in this codebase.

---

### Decision 4: Gold becomes 6 bespoke `@dp.table` functions reading `dp.read(silver_*)` (static), logic ported verbatim

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-19 |

**Context:** All 6 Gold notebooks already compute a complete aggregation over their Silver source(s) and `MERGE INTO ... WHEN MATCHED UPDATE SET * / WHEN NOT MATCHED INSERT *` the result — which, since the aggregation recomputes from the full Silver table every run, is already behaviorally a full overwrite, not a true incremental upsert.

**Choice:** One `@dp.table(name=gold_table, cluster_by=[...])` function per Gold table, body = the notebook's aggregation logic verbatim, ending in a `return` instead of a `createOrReplaceTempView` + `MERGE INTO`. Worked example (`gold_payments_by_status`, the simplest of the 6):

```python
def register_gold_payments_by_status() -> None:
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
```

The 3 Gold tables with a `row_number()` dedup guard (`gold_user_behavior`, `gold_driver_performance`, `gold_revenue_per_restaurant` — `ADR-005`) keep that guard as the last transformation before `return`, unchanged in logic, just no longer followed by a `MERGE`.

**Rationale:** `dp.read()` (not `dp.read_stream()`) on `silver_*` gives a static, complete snapshot each run, which is exactly what these notebooks already compute against (`spark.table(silver_src)`, a batch read). Lakeflow infers each Gold table's upstream dependencies automatically from the `dp.read("...silver...")` calls inside the function body — the explicit `depends_on: [task_key: silver_*]` lists in today's `databricks.yml` Gold task anchors become unnecessary.

**Alternatives Rejected:**
1. A generic `register_gold(spec: dict)` loop mirroring Bronze/Silver's `contracts/*.yml` pattern — rejected by DEFINE: 6 genuinely non-uniform join/aggregation shapes, not worth a forced abstraction (YAGNI).
2. `dp.read_stream(silver_*)` with incremental aggregation — rejected: none of the 6 notebooks compute incrementally today (each does a full `groupBy` over the whole Silver table); switching to streaming aggregation would be a logic change, which DEFINE explicitly rules out ("packaging/lineage change, not a logic rewrite").

**Consequences:**
- Unity Catalog Lineage now shows Gold's upstream edges as direct `dp.read()` references to `silver.*` tables, satisfying AT-004 without any extra wiring.
- The 6 `gold_*` task entries and their `depends_on` lists disappear from `databricks.yml` entirely — not just simplified.

---

### Decision 5: `databricks.yml` collapses to one pipeline resource + one 1-task Job, identical across all 3 targets

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-19 |

**Context:** Today's `databricks.yml` (878 lines) carries: 37 `task_definitions` anchors (used by `free_edition`'s `serverless_tasks` and partially by `dev`/`prod`'s `lakeflow_tasks`), a `bronze_silver_pipeline_resource`/`_task` anchor pair, and per-target `resources:` blocks that already differ structurally (`dev`/`prod` = pipeline + 8-task Job; `free_edition` = 37-task Job, no pipeline).

**Choice:** Replace `task_definitions`, `lakeflow_tasks`, and `serverless_tasks` with:

```yaml
variables:
  _pipeline_anchors:
    default:
      pipeline_resource: &pipeline_resource
        name: ubereats_pipeline
        catalog: ${var.catalog}
        schema: bronze
        serverless: true
        continuous: false
        libraries:
          - file:
              path: pipelines/ubereats_pipeline.py
        configuration:
          ubereats.catalog: ${var.catalog}
          ubereats.kafka_bootstrap: ${var.kafka_bootstrap}
          ubereats.schema_registry_url: ${var.schema_registry_url}
          ubereats.starting_offsets: earliest
          ubereats.source_mode: ${var.bronze_source_mode}
          ubereats.volume_base: ${var.landing_base}

      pipeline_task: &pipeline_task
        task_key: ubereats_pipeline_task
        pipeline_task:
          pipeline_id: ${resources.pipelines.ubereats_pipeline.id}
          full_refresh: false

      email_notifications: &email_notifications
        on_failure:
          - christiandr@gmail.com
        no_alert_for_skipped_runs: true

targets:
  dev:
    variables:
      catalog: ubereats_dev
      bronze_source_mode: kafka
      landing_base: /Volumes/ubereats_dev/landing/kafka_export
    resources:
      pipelines:
        ubereats_pipeline: *pipeline_resource
      jobs:
        ubereats_pipeline:
          name: ubereats_pipeline
          email_notifications: *email_notifications
          tasks: [*pipeline_task]

  prod:
    variables:
      catalog: ubereats_prod
      bronze_source_mode: kafka
      landing_base: /Volumes/ubereats_prod/landing/kafka_export
    resources:
      pipelines:
        ubereats_pipeline: *pipeline_resource
      jobs:
        ubereats_pipeline:
          name: ubereats_pipeline
          email_notifications: *email_notifications
          tasks: [*pipeline_task]

  free_edition:
    variables:
      catalog: ubereats_dev
      bronze_source_mode: volume
      landing_base: /Volumes/ubereats_dev/landing/kafka_export
    resources:
      pipelines:
        ubereats_pipeline: *pipeline_resource
      jobs:
        ubereats_pipeline:
          name: ubereats_pipeline
          max_concurrent_runs: 1
          email_notifications: *email_notifications
          tasks: [*pipeline_task]
```

`checkpoint_base` and `workspace_root` variables are removed — both were only ever consumed by the now-deleted `task_definitions` anchors (`checkpoint_path: ${var.checkpoint_base}/...`, `contract_path: ${var.workspace_root}/contracts/...`); `pipelines/ubereats_pipeline.py` already resolves contracts via `Path(__file__).resolve().parent.parent / "contracts"` and needs no explicit checkpoint path (Lakeflow self-manages pipeline storage).

**Rationale:** Once Gold/`silver_users`/`free_edition` all run the same pipeline, the only thing distinguishing targets is `catalog`/`bronze_source_mode`/`landing_base` — exactly what `variables:` per-target overrides already express. The `databricks/cli#2872` constraint (can't exclude a root-level resource from one target) still applies, so each target still declares its own `resources.pipelines.ubereats_pipeline`/`resources.jobs.ubereats_pipeline` — but now they're identical aliases of the same anchor, not three different shapes.

**Alternatives Rejected:**
1. Keep `task_definitions` around "in case `free_edition` needs to fall back to notebooks later" — rejected: CLAUDE.md's anti-pattern guidance explicitly discourages backwards-compatibility shims for hypothetical futures; git history is the fallback if ever needed.
2. Give the pipeline resource a different name per target (e.g. `ubereats_pipeline_dev`) — rejected: no reason to diverge now that all 3 targets run the same code; matching names keeps `${resources.pipelines.ubereats_pipeline.id}` identical across the anchor.

**Consequences:**
- `databricks.yml` shrinks from 878 lines to an estimated ~120-150 lines.
- `scripts/preflight_unity_catalog.sh`'s `checkpoints` schema/Volumes (`bronze`/`silver`) become unused infrastructure once `free_edition` stops running the checkpointed notebooks — flagged as a follow-up cleanup, **not** addressed in this feature (out of DEFINE's stated scope; the script still works, it just provisions a Volume nothing reads from anymore).
- `.github/workflows/ci.yml`'s `bundle-validate` job currently only runs `databricks bundle validate --target dev`. Since `free_edition`'s resource shape is the one changing most (37 tasks → 1), this design adds a second validate step for `--target free_edition` to actually exercise the part of the bundle most likely to break.

---

### Decision 6: Legacy notebooks are deleted outright, not archived to a parallel directory

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-19 |

**Context:** DEFINE's success criteria require the 8 legacy notebooks be "deleted or explicitly archived, not left as dead orphaned files."

**Choice:** `git rm notebooks/pipeline_bronze.ipynb notebooks/pipeline_silver.ipynb notebooks/pipeline_users.ipynb notebooks/cross_domain/gold_*.ipynb` (8 files). No copy into a `notebooks/_archived/` directory.

**Rationale:** Git history already preserves every line of these notebooks at every prior commit (most recently `dee71b0`, `e94d8e3`); a parallel "archived" copy would be a second, immediately-stale source of the same content — exactly the kind of dead-weight duplication CLAUDE.md's project conventions ask to avoid.

**Alternatives Rejected:**
1. Move to `notebooks/_archived/` — rejected: redundant with git history, and an "archived" notebook still imports/references Bronze/Silver tables that may later diverge from the live pipeline, becoming actively misleading rather than just inert.

**Consequences:**
- Anyone needing the pre-migration notebook logic finds it via `git log --diff-filter=D -- notebooks/pipeline_users.ipynb` (or any of the other 7), not a live file in the tree.
- `notebooks/cross_domain/` becomes an empty directory after this change — also removed (git doesn't track empty directories, so no further action needed beyond deleting the 6 files in it).

---

### Decision 7: New `docs/adr/007_pipeline_unification.md` explicitly supersedes `ADR-006` and the CLAUDE.md-referenced "ADR-05"

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-19 |

**Context:** `docs/adr/006_lakeflow_migration.md`'s "Explicitly NOT migrated" section lists Gold, `silver_users`, and `free_edition` with specific reasons — those reasons are exactly what this feature reverses. Separately, CLAUDE.md/README cite a "`ADR-05`" for the `source_mode` dual-path decision that has no corresponding `docs/adr/0XX` file (a numbering gap a prior retrospective already flagged — `.claude/sdd/archive/GOLD_DIMENSION_JOIN_INTEGRITY/SHIPPED_2026-06-18.md`). Fixing that numbering gap project-wide is out of scope here; this ADR only needs to state which premise it supersedes, by description, not by chasing down the missing file.

**Choice:** `docs/adr/007_pipeline_unification.md`, structured like the existing ADRs (Context/Decision/Rationale/Alternatives/Consequences), explicitly stating:
- Supersedes `docs/adr/006_lakeflow_migration.md`'s "Explicitly NOT migrated" section (all three bullets).
- Supersedes the premise — informally cited elsewhere as "ADR-05," with no corresponding `docs/adr/0XX` file — that `free_edition` is uniquely unable to reach Kafka from serverless compute, now reframed as "unverified for any target, design supports both."

**Rationale:** Matches this project's existing convention of ADRs explicitly stating what prior ADR they revise (`006` already does this for `003`). Avoids inventing a fix for the unrelated `ADR-05` numbering-gap problem, which is a separate piece of cleanup a prior retrospective already scoped as its own future session.

**Alternatives Rejected:**
1. Edit `docs/adr/006_lakeflow_migration.md` in place to remove the "Explicitly NOT migrated" section — rejected: ADRs in this project are treated as an append-only decision log (`006` revises `003` via a new file, not an edit); doing otherwise would lose the historical record of why Gold/`silver_users`/`free_edition` were excluded in the first place.

**Consequences:**
- `docs/adr/006_lakeflow_migration.md` stays unchanged; readers must follow forward to `007` to learn its exclusions no longer hold — same pattern already established by `006` → `003`.

---

## File Manifest

| # | File | Action | Purpose | Agent | Dependencies |
|---|------|--------|---------|-------|--------------|
| 1 | `pipelines/ubereats_pipeline.py` | Rename + Modify | Add `register_silver_users()` + 6 `register_gold_*()` functions; add `source_mode` branch to `register_bronze()` | @lakeflow-pipeline-builder | None |
| 2 | `databricks.yml` | Modify | Collapse `task_definitions`/`lakeflow_tasks`/`serverless_tasks` into one `pipeline_resource`/`pipeline_task` anchor shared by all 3 targets; remove `checkpoint_base`/`workspace_root` variables | @ci-cd-specialist | 1 |
| 3 | `docs/adr/007_pipeline_unification.md` | Create | Supersede `ADR-006`'s exclusions + the informally-cited "ADR-05" Kafka-reachability premise | @lakeflow-architect | None |
| 4 | `notebooks/pipeline_bronze.ipynb` | Delete | Retired — logic ported into file 1 | (general) | 1 |
| 5 | `notebooks/pipeline_silver.ipynb` | Delete | Retired — logic already ported in the pre-existing `register_silver()` | (general) | 1 |
| 6 | `notebooks/pipeline_users.ipynb` | Delete | Retired — logic ported into `register_silver_users()` in file 1 | (general) | 1 |
| 7 | `notebooks/cross_domain/gold_payments_by_status.ipynb` | Delete | Retired — ported into file 1 | (general) | 1 |
| 8 | `notebooks/cross_domain/gold_payment_funnel.ipynb` | Delete | Retired — ported into file 1 | (general) | 1 |
| 9 | `notebooks/cross_domain/gold_payment_lifecycle.ipynb` | Delete | Retired — ported into file 1 | (general) | 1 |
| 10 | `notebooks/cross_domain/gold_driver_performance.ipynb` | Delete | Retired — ported into file 1 | (general) | 1 |
| 11 | `notebooks/cross_domain/gold_revenue_per_restaurant.ipynb` | Delete | Retired — ported into file 1 | (general) | 1 |
| 12 | `notebooks/cross_domain/gold_user_behavior.ipynb` | Delete | Retired — ported into file 1 | (general) | 1 |
| 13 | `.github/workflows/ci.yml` | Modify | Add `databricks bundle validate --target free_edition` alongside the existing `--target dev` step | @ci-cd-specialist | 2 |
| 14 | `CLAUDE.md` | Modify | Update "Bronze+Silver execution" row, notebook counts, domain map references, and add this feature's architecture decision summary | (general) | 1, 2, 3 |

**Total Files:** 14 (1 renamed+modified, 2 modified, 1 created, 8 deleted, 2 modified for docs/CI)

---

## Agent Assignment Rationale

> Agents discovered from `.claude/agents/` — Build phase invokes matched specialists.

| Agent | Files Assigned | Why This Agent |
|-------|----------------|-----------------|
| @lakeflow-pipeline-builder | 1 | `.claude/agents/data-engineering/lakeflow-pipeline-builder.md` — "Builds Databricks Lakeflow (DLT) pipelines for Medallion Architecture... Bronze/Silver/Gold tables, DLT notebooks, DABs configurations" — exact match for extending `pipelines/ubereats_pipeline.py` |
| @ci-cd-specialist | 2, 13 | `.claude/agents/cloud/ci-cd-specialist.md` — "DevOps expert for Azure DevOps, Terraform, and Databricks Asset Bundles... Builds CI/CD pipelines for Lambda and Lakeflow deployment with multi-environment promotion" — matches `databricks.yml` anchor consolidation and the CI validate-step addition |
| @lakeflow-architect | 3 | `.claude/agents/data-engineering/lakeflow-architect.md` — "Databricks Lakeflow expert for building Medallion architecture pipelines... Uses KB + MCP validation" — matches authoring the ADR that documents the architectural reversal |
| (general) | 4-12, 14 | File deletions and the CLAUDE.md doc-sync are mechanical follow-through on Decisions 6/7, not specialist work — Build phase handles directly |

**Agent Discovery:**
- Scanned: `.claude/agents/**/*.md`
- Matched by: file type, purpose keywords (Lakeflow/DLT, DABs/CI-CD), path patterns

---

## Code Patterns

### Pattern 1: `source_mode` branch inside a `@dp.table` Bronze function

See Decision 2 above for the full `register_bronze()` body.

### Pattern 2: Two-stage quarantine via a shared candidate temp view

See Decision 3 above for the full `register_silver_users()` body — the pattern generalizes the existing single-predicate quarantine convention (`contracts/dlt_adapter.py`'s `quarantine_row_level_predicate`) to a `_quarantine_reason` column carrying which of N causes applied, unioned across stages.

### Pattern 3: Gold aggregation as a static `@dp.table`

See Decision 4 above (`register_gold_payments_by_status` worked example). The other 5 follow identically: replace the notebook's final `createOrReplaceTempView(...)` + `spark.sql(f"MERGE INTO ...")` cell with a `return <same dataframe>` inside the `@dp.table`-decorated function, keeping every prior transformation cell's logic unchanged.

### Pattern 4: `databricks.yml` shared single-task-job anchor

See Decision 5 above for the full `pipeline_resource`/`pipeline_task`/`email_notifications` anchors and their per-target use.

---

## Data Flow

```text
1. register_bronze() per contracts/*.yml domain (×20)
   — source_mode=kafka: spark.readStream from the Debezium-fed Kafka topic
   — source_mode=volume: spark.read (batch) from the landing Volume Parquet snapshot
   │
   ▼
2. register_silver() per contracts/*.yml domain with layers=[bronze,silver] (×10, unchanged)
   — candidate temp view → quarantine.<domain> @dp.table → clean temp view →
     create_streaming_table + create_auto_cdc_flow (SCD1, keyed by merge_key)
   │
   ▼
3. register_silver_users() — new
   — bronze.users_mongo + bronze.users_mssql → candidate temp view (FULL OUTER JOIN
     on normalized CPF) → quarantine.users @dp.table (missing_cpf ∪ duplicate_user_id)
     → silver.users @dp.table (anti-join against quarantine.users)
   │
   ▼
4. register_gold_*() ×6 — new
   — dp.read(silver_*) → bespoke aggregation (ported verbatim from notebooks) →
     optional row_number() dedup guard → return (no MERGE; @dp.table recomputes in full)
   │
   ▼
5. databricks.yml's single pipeline_task triggers all of 1-4 as one Lakeflow pipeline run,
   per target (dev/prod/free_edition), differing only by catalog/source_mode/landing_base
```

---

## Integration Points

| External System | Integration Type | Authentication |
|-----------------|-------------------|-----------------|
| Kafka (`pg.public.*` topics) | Structured Streaming source (`kafka` source_mode) | None in this dataset (local broker, no SASL) — unchanged from today |
| Confluent Schema Registry | REST (`_avro_schema_str`) for Avro subject lookup | None — unchanged from today |
| Unity Catalog (`ubereats_dev`/`ubereats_prod`) | Lakeflow pipeline target catalog/schema | Databricks workspace auth (`DATABRICKS_HOST`/`DATABRICKS_TOKEN` in CI) — unchanged |
| Landing Volume (`/Volumes/<catalog>/landing/kafka_export/`) | Batch Parquet read (`volume` source_mode) | Unity Catalog Volume ACLs — unchanged |

---

## Testing Strategy

| Test Type | Scope | Files | Tools | Coverage Goal |
|-----------|-------|-------|-------|-----------------|
| Unit | Contract loading/validation (unaffected by this feature) | `tests/test_contracts.py` | pytest | Existing — no change needed |
| Unit | `dlt_adapter.py` rule translation (unaffected, still used by the 10 generic Silver domains) | `tests/test_dlt_adapter.py` | pytest | Existing — no change needed |
| Static | DABs bundle structural validity, all 3 targets | `databricks.yml` | `databricks bundle validate --target {dev,prod,free_edition}` | All 3 targets must validate — `free_edition` is newly added to CI per Decision 5 |
| Integration | Live pipeline run produces the 37 expected tables with correct row counts | N/A — manual/deploy-time | `databricks bundle deploy` + `databricks bundle run` against a real workspace | AT-001 — all 37 tables populated, 0 tasks referencing a legacy notebook path |
| Integration | Quarantine routing for `silver_users`'s two failure causes | N/A — manual/deploy-time, seed a duplicate `user_id` and a missing-CPF row in test data | Live workspace query against `quarantine.users` post-run | AT-002 |
| Integration | `free_edition` end-to-end under `source_mode=volume` | N/A — manual/deploy-time, after `scripts/export_kafka_to_volume.py` | Live workspace run + idempotent re-run check | AT-003 |
| Integration | Gold lineage shows only Silver upstream edges | N/A — manual, Unity Catalog Lineage UI/API | Live workspace | AT-004 |

---

## Error Handling

| Error Type | Handling Strategy | Retry? |
|------------|---------------------|--------|
| Schema Registry unreachable (`kafka` source_mode) | `_avro_schema_str()` raises on non-2xx via `resp.raise_for_status()` — pipeline run fails fast | No built-in retry — relies on Databricks Job/pipeline run-level retry policy if configured |
| Landing Volume path missing/empty (`volume` source_mode) | `spark.read.format("parquet").load(...)` raises `AnalysisException` — pipeline run fails fast; operator must run `scripts/export_kafka_to_volume.py` first | No |
| Duplicate dimension join key reaching Gold (`drivers.driver_id`, `restaurants.cnpj`, `users.user_id`) | Primary defense: Silver `check: unique` quarantines the offending row before Gold ever sees it (`ADR-005`); secondary defense: `row_number()` guard kept verbatim in the 3 affected Gold functions | N/A — rows are quarantined/deduped, not retried |
| Missing CPF / duplicate `user_id` in `silver_users` sources | Routed to `quarantine.users` with `_quarantine_reason`, never dropped or failing the pipeline | N/A |

---

## Configuration

| Config Key | Type | Default | Description |
|------------|------|---------|-------------|
| `ubereats.catalog` | string | `ubereats_dev` | Unity Catalog name — unchanged |
| `ubereats.kafka_bootstrap` | string | `localhost:9092` | Kafka bootstrap servers — unchanged |
| `ubereats.schema_registry_url` | string | `http://localhost:8081` | Confluent Schema Registry URL — unchanged |
| `ubereats.starting_offsets` | string | `earliest` | Kafka starting offsets — unchanged |
| `ubereats.source_mode` | string | `kafka` | **New at pipeline scope** — `kafka` or `volume`; previously a per-notebook-task widget, now one pipeline-level setting consumed by `register_bronze()` for all 20 domains |
| `ubereats.volume_base` | string | `/Volumes/ubereats_dev/landing/kafka_export` | **New** — base path for `volume` source_mode; per-domain path is `{volume_base}/{domain}`, computed in Python instead of 20 YAML `volume_path` parameters |

---

## Security Considerations

- No new external endpoints, credentials, or secrets introduced — same Kafka broker, Schema Registry, and Unity Catalog ACL model as today.
- `DATABRICKS_TOKEN`/`DATABRICKS_HOST` (CI secrets) usage is unchanged; the new `free_edition` validate step in `ci.yml` uses the same secrets already scoped to that workflow.
- Deleting the 8 legacy notebooks removes their `dbutils.widgets` parameter surface (no longer accepts arbitrary `bronze_table`/`silver_table`/`catalog` strings as job parameters) — slightly reduces the configuration-injection surface, though this was never a credible attack vector in this single-workspace setup.

---

## Observability

| Aspect | Implementation |
|--------|------------------|
| Logging | Lakeflow pipeline event log now covers Gold + `silver_users` too (previously only visible via individual notebook task stdout/Job run logs) |
| Lineage | Unity Catalog Lineage shows all 37 tables as distinct nodes under one pipeline, with Gold's upstream edges resolving directly to `silver.*` (AT-004) — extends the per-domain granularity `ADR-006` established for Bronze/Silver to the whole pipeline |
| Quality metrics | `@dp.expect_all`/`@dp.expect_all_or_drop` pass/fail counts, already available for the 10 generic Silver domains, now also implicitly available for `silver_users`'s quarantine counts via the `quarantine.users` table's row count per run |
| Alerting | `email_notifications.on_failure` stays at the Job level (1 task = 1 pipeline run), unchanged from today's intent, just now the only task in the Job instead of one of 8 |

---

## Pipeline Architecture

### DAG Diagram

```text
[Kafka topic | Landing Volume] ──register_bronze()──→ [Bronze @dp.table ×20]
                                                              │
                          ┌───────────────────────────────────┤
                          ▼                                   ▼
        [Silver @dp.table ×10 — create_auto_cdc_flow]   [bronze.users_mongo/_mssql]
        [quarantine.<domain> ×10]                              │
                          │                          register_silver_users()
                          │                                    ▼
                          │                      [silver.users @dp.table]
                          │                      [quarantine.users @dp.table]
                          │                                    │
                          └───────────────┬────────────────────┘
                                          ▼
                          [Gold @dp.table ×6 — dp.read(silver_*)]
```

### Partition Strategy

| Table | Cluster Key (`cluster_by`) | Rationale |
|-------|------------------------------|-----------|
| Bronze ×20, Silver ×10, Quarantine ×10 | Per `contracts/*.yml` `storage.cluster_by` | Unchanged — `ADR-004` |
| `silver.users` / `quarantine.users` | `[cpf]` | Ported from `pipeline_users.ipynb`'s `CLUSTER BY (cpf)` — `cpf` is the real identity, `ADR-004`/`ADR-005` |
| `gold.payments_by_status` | `[status]` | Ported from notebook `CLUSTER BY (status)` |
| `gold.payment_funnel` | `[event_name]` | Ported from notebook `CLUSTER BY (event_name)` |
| `gold.payment_lifecycle` | `[payment_id]` | Ported from notebook `CLUSTER BY (payment_id)` |
| `gold.driver_performance` | `[driver_id]` | Ported from notebook `CLUSTER BY (driver_id)` |
| `gold.revenue_per_restaurant` | `[restaurant_cnpj]` | Ported from notebook `CLUSTER BY (restaurant_cnpj)` |
| `gold.user_behavior` | `[user_id]` | Ported from notebook `CLUSTER BY (user_id)` |

### Incremental Strategy

| Layer | Strategy | Key Column | Notes |
|-------|----------|--------------|-------|
| Bronze (`kafka` mode) | Streaming append (Lakeflow-managed) | N/A | Unchanged |
| Bronze (`volume` mode) | Full recompute each run | N/A | New — replaces the notebook's hand-written `MERGE INTO ... WHEN NOT MATCHED` |
| Silver (10 generic) | `create_auto_cdc_flow`, SCD1 | `merge_key` per contract | Unchanged |
| `silver.users` | Full batch recompute (`@dp.table` materialized view) | N/A — full overwrite | Matches the notebook's `mode("overwrite")` semantics |
| Gold (×6) | Full batch recompute (`@dp.table` materialized view) | N/A — full overwrite | Matches the notebooks' actual behavior (complete `groupBy` recompute, even though previously wrapped in a `MERGE`) |

### Schema Evolution Plan

| Change Type | Handling | Rollback |
|-------------|----------|----------|
| New column (contract-driven domains) | `contracts/*.yml` `schema_evolution.new_fields: allowed` — unchanged | Drop column |
| New column (`silver_users`/Gold — no YAML contract) | Implicit, same as today — whatever the DataFrame projection produces; this migration does not add contract coverage for them (unchanged gap, not introduced by this feature) | Revert the projection in `pipelines/ubereats_pipeline.py` |
| Column removal | Deprecate in contract (10 generic domains) / code review (others), remove after confirmation | Re-add column |

### Data Quality Gates

| Gate | Tool | Threshold | Action on Failure |
|------|------|-----------|----------------------|
| Bronze reject rules | `@dp.expect_all_or_drop` | Per `contracts/*.yml` | Row dropped — unchanged |
| Silver warn rules | `@dp.expect_all` | Per `contracts/*.yml` | Logged, not blocked — unchanged |
| Silver quarantine (row-level + `check: unique`) | `quarantine.<domain>` `@dp.table` | Per `contracts/*.yml` | Row routed to quarantine, not dropped — unchanged |
| `silver_users` quarantine (`missing_cpf`, `duplicate_user_id`) | `quarantine.users` `@dp.table` | Hand-coded, ported from notebook | Row routed to quarantine — newly expressed in `@dp.table` form |
| Gold dimension-join dedup guard | `row_number()` window, kept verbatim | One row per merge key | Extra rows silently dropped before Gold table materializes — unchanged from `ADR-005` |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-06-19 | design-agent | Initial version — full unification design per `DEFINE_PIPELINE_UNIFICATION.md`, grounded against current `pipelines/bronze_silver_dlt.py`, all 6 Gold notebooks, `pipeline_users.ipynb`, `databricks.yml` (878 lines), `docs/adr/005`/`006`, and `.github/workflows/ci.yml` |
| 1.1 | 2026-06-19 | ship-agent | Shipped and archived — see `SHIPPED_2026-06-19.md` |

---

## Next Step

**Ready for:** `/build .claude/sdd/features/DESIGN_PIPELINE_UNIFICATION.md`
