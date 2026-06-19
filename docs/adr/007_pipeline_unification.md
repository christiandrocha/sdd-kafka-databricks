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

## See also

`.claude/sdd/features/DESIGN_PIPELINE_UNIFICATION.md` — full design, file manifest, and code
patterns behind the decisions above. `docs/adr/006_lakeflow_migration.md` — the ADR this one
supersedes the exclusions of.
