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
validated. `scripts/preflight_unity_catalog.sh`'s `checkpoints` schema/Volumes
(`bronze`/`silver`) become unused infrastructure once `free_edition` stops running the
checkpointed legacy notebooks — left in place as a follow-up cleanup, not addressed here.

## See also

`.claude/sdd/features/DESIGN_PIPELINE_UNIFICATION.md` — full design, file manifest, and code
patterns behind the decisions above. `docs/adr/006_lakeflow_migration.md` — the ADR this one
supersedes the exclusions of.
