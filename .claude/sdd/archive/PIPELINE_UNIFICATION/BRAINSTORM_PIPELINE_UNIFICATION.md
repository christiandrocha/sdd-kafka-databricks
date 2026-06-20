# BRAINSTORM: PIPELINE_UNIFICATION

> Exploratory session to clarify intent and approach before requirements capture

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | PIPELINE_UNIFICATION |
| **Date** | 2026-06-18 |
| **Author** | brainstorm-agent |
| **Status** | Ready for Define |

---

## Initial Idea

**Raw Input:** Expand the Lakeflow DLT migration (`ADR-006`, feature `LAKEFLOW_MIGRATION`) into a
single unified pipeline named `ubereats_pipeline`, covering Bronze, Silver, `silver_users`, Gold,
and Quarantine — replacing the current split where Gold (6 notebooks), `silver_users` (1 notebook),
and `free_edition` (37-task notebook job) sit outside the Lakeflow pipeline introduced by
`ADR-006`.

**Context Gathered:**
- `pipelines/bronze_silver_dlt.py` (current state) loops over `contracts/*.yml` registering one
  `@dp.table` Bronze + one Silver `create_streaming_table`/`create_auto_cdc_flow` pair per domain,
  for 20 Bronze + 10 of 11 Silver domains. `dev`/`prod` only.
- All 6 Gold notebooks (`notebooks/cross_domain/*.ipynb`) read **only from Silver tables**
  (`silver.driver_shifts`, `silver.drivers`, `silver.orders`, `silver.payment_events`,
  `silver.payments`, `silver.order_items`, `silver.restaurants`, `silver.search_events`,
  `silver.recommendations`, `silver.users`) — none read Bronze-only domains directly.
- `notebooks/pipeline_users.ipynb` (`silver_users` task) does: bronze read (mongo+mssql) → CPF
  normalize/dedup → missing-CPF quarantine → FULL OUTER JOIN → duplicate-`user_id` quarantine →
  full-refresh `mode("overwrite")` write to `silver.users`.
- `ADR-006` explicitly excluded Gold/`silver_users`/`free_edition` from the original Lakeflow
  migration, reasoning that (a) Gold/`silver_users` don't have the lineage-grouping bug since
  they're already 1-notebook-per-execution, and (b) Gold's imperative logic and `silver_users`'s
  full-refresh model don't fit DLT's incremental streaming-table pattern.
- Earlier in this session (`databricks.yml` fixes), the user confirmed the real workspace behind
  **all three** bundle targets (`dev`/`prod`/`free_edition`) is a single Free Edition account —
  which already required removing all `job_cluster_key`/classic-compute config from `dev`/`prod`.
  This directly undercuts `ADR-006`'s stated reason for excluding `free_edition`
  ("Lakeflow serverless support there hasn't been verified") and `ADR-05`'s premise that
  `free_edition` uniquely can't reach Kafka.

**Technical Context Observed (for Define):**

| Aspect | Observation | Implication |
|--------|-------------|--------------|
| Likely Location | `pipelines/ubereats_pipeline.py` (renamed from `bronze_silver_dlt.py`) | New Gold + `silver_users` registration functions added alongside existing Bronze/Silver loop |
| Relevant existing patterns | `contracts/dlt_adapter.py` (expectations/quarantine translation), quarantine-as-inverse-predicate-table convention | Reuse for `silver_users`'s new quarantine table instead of inventing a new convention |
| IaC Patterns | `databricks.yml` `_pipeline_anchors` scaffold (`task_definitions`, `serverless_tasks`, `lakeflow_tasks`) | Almost entirely deleted — only one pipeline resource + one 1-task job anchor remains, shared by all 3 targets |

---

## Discovery Questions & Answers

| # | Question | Answer | Impact |
|---|----------|--------|--------|
| 1 | Is Gold→Silver lineage actually broken today in Unity Catalog, or is the motivation architectural consistency? | Not a lineage bug — motivation is a single DABs resource (`ubereats_pipeline`) covering all layers | Reframes the goal: this is a consistency/simplification migration, not a correctness fix. Removes pressure to "prove" a lineage benefit that may not exist. |
| 2 | A standalone pipeline resource has no native schedule (only `continuous` mode or a Job's `pipeline_task`). How should the unified pipeline be triggered? | Keep a minimal Job — shrinks to exactly one `pipeline_task`, kept purely for scheduling + email-on-failure | "Job disappears completely" (as originally proposed) is revised to "Job shrinks to one task." `email_notifications` stays at the Job level, not pipeline-level. |
| 3 | `silver_users`'s quarantine table currently appends across runs (permanent history). A `@dp.table` recompute would only hold currently-quarantined rows. Acceptable? | Yes, acceptable | `quarantine.users` becomes a plain `@dp.table` (inverse-predicate pattern, matching the other 10 quarantine tables) instead of an append-only sink. History loss is a deliberate, confirmed trade-off, not an oversight. |
| 4 | Should `free_edition` also be folded into the same unified pipeline, given all 3 targets share one real Free Edition workspace? | Yes, fold it in too — retire the legacy notebooks entirely | This is now a 3-target unification, not just a `dev`/`prod` Gold migration. All 8 legacy notebooks (`pipeline_bronze`, `pipeline_silver`, `pipeline_users`, 6× `cross_domain/gold_*`) become candidates for retirement. |
| 5 | Can serverless compute reach the self-hosted Kafka broker directly, or does `free_edition`'s Volume/Parquet-snapshot workaround (`ADR-05`) still apply? | Unverified — design for both, decide at deploy | Bronze registration needs a `source_mode` (kafka \| volume) branch, controlled by pipeline `configuration`, so `free_edition` keeps working under either outcome. Verifying this against the live workspace is a pre-`/build`-completion task, not a brainstorm-time blocker. |

**Minimum Questions:** 3 — satisfied (5 asked)

---

## Sample Data Inventory

> Not applicable in the traditional LLM-grounding sense — this is an infrastructure/pipeline
> migration, not an extraction task. The equivalent "grounding" was verifying claims against
> Databricks' own documentation rather than against sample data:

| Type | Location | Notes |
|------|----------|-------|
| Authoritative API reference | Databricks CLI `bundle/schema/jsonschema.json` (pipelines resource) | Confirmed `schema`/`target` exclusivity in earlier session work; same source used to confirm no native pipeline schedule field exists |
| Existing code to port | `notebooks/cross_domain/*.ipynb` (6), `notebooks/pipeline_users.ipynb` | Read in full this session — aggregation logic, window-dedup guards, and quarantine routing to be ported verbatim into `@dp.table` functions |
| Prior ADRs | `docs/adr/003`, `docs/adr/005_*.md` (referenced, not yet re-read in full), `docs/adr/006_lakeflow_migration.md` | Establish the reasoning this migration revises; a new ADR-007 should explicitly supersede the "Explicitly NOT migrated" section of ADR-006 |

---

## Approaches Explored

### Approach A: Full unification ⭐ Recommended (Selected)

**Description:** One `pipelines/ubereats_pipeline.py` registers Bronze (20 domains, with a
`source_mode` kafka/volume branch), Silver (10 domains, unchanged loop), `silver_users` (new
`@dp.table` + sibling quarantine `@dp.table`), and 6 Gold tables (`@dp.table` + `dp.read(silver_*)`,
existing aggregation logic ported as-is). All 3 targets (`dev`/`prod`/`free_edition`) reference the
same pipeline resource (`resources.pipelines.ubereats_pipeline`) and a Job reduced to exactly one
`pipeline_task`, differing only by target `variables:` (catalog, `source_mode`, checkpoint/landing
paths). Retires all 8 legacy notebooks.

**Pros:**
- One mental model, one resource, one file — matches the stated goal directly.
- `databricks.yml`'s `_pipeline_anchors` scaffold (currently ~290 lines of `task_definitions` +
  `serverless_tasks`) shrinks drastically — most of it becomes dead weight once `free_edition`
  no longer needs 37 individual notebook task definitions.
- `dev`/`prod`/`free_edition` become structurally identical, differing only by variables —
  restores the symmetry `ADR-006` broke when it gave `dev`/`prod` different code from
  `free_edition` for Bronze/Silver.

**Cons:**
- Largest single change of the three approaches — touches all 3 targets at once.
- Kafka-reachability-on-serverless is still unverified; the `source_mode` branch is necessary
  scaffolding for an outcome that won't be confirmed until deploy time.
- Loses `silver_users`'s append-only quarantine history (confirmed acceptable).

**Why Recommended:** Directly matches all 5 confirmed decisions above, including explicitly
folding in `free_edition`. The alternatives (B, C) exist only to de-risk scope, and the user
selected full scope after seeing the trade-offs.

---

### Approach B: Dev/prod only, defer `free_edition`

**Description:** Migrate Gold + `silver_users` into the renamed `ubereats_pipeline.py` for `dev`/
`prod` only. Leave `free_edition`'s 37-task notebook job completely untouched until Kafka
reachability from serverless compute is actually tested.

**Pros:**
- Smaller blast radius — one target family at a time.
- Defers the `source_mode` dual-path complexity until it's known to be necessary.

**Cons:**
- Re-introduces the exact `dev`/`prod` vs. `free_edition` asymmetry `ADR-006` already created for
  Bronze/Silver — now for Gold/`silver_users` too.
- Two migration waves instead of one; `free_edition`'s legacy notebooks still need to be revisited
  later regardless.

**Not selected** — user confirmed full scope (Approach A) after reviewing this trade-off.

---

### Approach C: Rename + `silver_users` only, keep Gold as notebooks

**Description:** Rename `ubereats_bronze_silver` → `ubereats_pipeline`, fold `silver_users` in
(since its FULL OUTER JOIN fits a plain `@dp.table` batch recompute cleanly), but leave the 6 Gold
notebooks as separate Job tasks exactly as `ADR-006` originally argued — since Gold→Silver lineage
isn't actually broken today.

**Pros:**
- Lowest risk — Gold's imperative MERGE/window-dedup logic, recently shipped in
  `GOLD_DIMENSION_JOIN_INTEGRITY`, isn't touched at all.
- Still achieves naming consistency and removes one more notebook (`pipeline_users.ipynb`).

**Cons:**
- Doesn't achieve "one resource covering all layers" — the Job still carries 6 Gold tasks.
- Doesn't address `free_edition` at all.

**Not selected** — user explicitly wants Gold inside the same pipeline (item 2 of the original
request), confirmed this isn't about fixing a lineage bug but about having one resource for
everything.

---

## Data Engineering Context

### Source Systems
| Source | Type | Volume Estimate | Current Freshness |
|--------|------|-----------------|-------------------|
| Kafka (`pg.public.*` topics via Debezium) | Streaming (CDC) | 129,353 records total, see CLAUDE.md domain map | Near-real-time on `dev`/`prod` (if Kafka is reachable) |
| Landing Volume (`/Volumes/<catalog>/landing/kafka_export/`) | Batch (Parquet snapshot) | Same as above, exported via `scripts/export_kafka_to_volume.py` | Manually refreshed snapshot — used when Kafka isn't reachable |

### Data Flow Sketch
```text
[Kafka topic | Landing Volume] → [Bronze @dp.table, per domain]
                                       │
                                       ▼
                          [Silver @dp.table / create_auto_cdc_flow, 10 domains]
                          [silver_users @dp.table — FULL OUTER JOIN mongo+mssql] ← Bronze users_mongo/users_mssql
                                       │
                                       ▼
                          [Gold @dp.table × 6 — dp.read(silver_*) aggregations]

  Quarantine: one @dp.table per Silver domain (11 total, including users) — inverse predicate of
  the same upstream candidate view, never silently dropped.
```

### Key Data Questions Explored
| # | Question | Answer | Impact |
|---|----------|--------|--------|
| 1 | Does Gold read anything besides Silver tables? | No — confirmed by reading all 6 notebooks in full | Gold migration only needs `dp.read(silver_table)`, no Bronze-only domain wiring |
| 2 | Does a standalone pipeline resource support its own schedule? | No — only `continuous` mode or a Job's `pipeline_task` (confirmed via Databricks docs) | Forces "keep a minimal Job" decision (#2 above) rather than a job-free design |
| 3 | Is `@dp.table` (non-streaming) a legitimate full-batch-recompute pattern in Lakeflow? | Yes — it's the materialized-view model, distinct from `create_streaming_table` | Resolves `ADR-006`'s original objection to migrating `silver_users` — the objection conflated "doesn't fit incremental streaming" with "doesn't fit Lakeflow at all" |

---

## Selected Approach

| Attribute | Value |
|-----------|-------|
| **Chosen** | Approach A — Full unification |
| **User Confirmation** | 2026-06-18, via explicit choice after reviewing A/B/C trade-offs |
| **Reasoning** | Matches the original request directly (single resource, all layers, all targets); user confirmed folding in `free_edition` after the workspace-sharing fact came to light, and accepted the quarantine-history and Job-shrinkage trade-offs needed to make it work. |

---

## Key Decisions Made

| # | Decision | Rationale | Alternative Rejected |
|---|----------|-----------|----------------------|
| 1 | Rename `pipelines/bronze_silver_dlt.py` → `pipelines/ubereats_pipeline.py`, single resource `resources.pipelines.ubereats_pipeline` | Matches the stated goal of one DABs resource for all layers | Keeping `bronze_silver_dlt.py` name and adding a second pipeline resource for Gold |
| 2 | Goal reframed as architectural consistency, not a lineage fix | User confirmed Gold→Silver lineage likely already works (1 notebook : 1 execution, no parametrization) | Designing around an unverified lineage bug |
| 3 | Keep a minimal Job (one `pipeline_task`) per target instead of removing the Job entirely | Standalone pipelines have no native schedule; the Job is still needed for triggering + email-on-failure | Fully job-free design, or `continuous: true` (changes cost/semantics significantly) |
| 4 | `silver_users` becomes a plain `@dp.table` (batch, full recompute), not a streaming table | `@dp.table` without `create_streaming_table` is Lakeflow's materialized-view model — fits the FULL OUTER JOIN + full-refresh semantics natively | Forcing it into `create_streaming_table`/`create_auto_cdc_flow`, which doesn't fit a full-refresh join |
| 5 | `quarantine.users` becomes a plain `@dp.table`, losing append-only history | User confirmed acceptable; matches the existing inverse-predicate quarantine convention used by the other 10 domains | Preserving history via a streaming/append pattern, adding back complexity for a project-stage dataset (129k rows) |
| 6 | Fold `free_edition` into the same unified pipeline; retire all 37 legacy notebook tasks | All 3 targets share one real Free Edition workspace — the original `dev`/`prod` vs. `free_edition` compute distinction no longer holds | Leaving `free_edition` on the legacy notebook job (Approach B) |
| 7 | Bronze gets a `source_mode` (kafka \| volume) branch inside the unified pipeline | Kafka-reachability from serverless is unverified; design for both, confirm at deploy | Assuming Kafka works everywhere and dropping the volume path (risks breaking `free_edition` if wrong) |
| 8 | Gold stays as 6 bespoke functions in one file, not a generic contract-driven loop | Each Gold transform is genuinely bespoke (different joins, different dedup keys); forcing an abstraction over 6 cases isn't justified (YAGNI) | A generic `register_gold()` loop mirroring Bronze/Silver's `contracts/*.yml` pattern |

---

## Features Removed (YAGNI)

| Feature Suggested | Reason Removed | Can Add Later? |
|--------------------|-----------------|-----------------|
| Splitting Gold into `pipelines/gold/*.py` (one file per domain) | Premature structural complexity for 6 functions; one file is still far simpler than 8 notebooks + Job sprawl | Yes — DABs supports multiple `file:` library entries trivially if the single file becomes unwieldy |
| Generic contract-driven Gold registration loop | Gold transforms are not uniform like Bronze/Silver (different join keys, different dedup windows, different aggregation shapes per domain) | No — would need a redesign of `contracts/*.yml` schema to express arbitrary multi-table joins, out of proportion to the benefit |
| Append-only `quarantine.users` history preservation | Explicitly confirmed unnecessary for this project's scale/stage | Yes — would need a streaming-table/flow pattern instead of a plain `@dp.table`, deferred until actually needed |
| Native pipeline-level schedule (no Job at all) | Doesn't exist in the Databricks pipeline resource schema today | N/A — not a feature to build, a non-existent capability |
| `continuous: true` for the unified pipeline | Changes cost/semantics substantially (always-on compute) for no stated benefit over a scheduled Job trigger | Yes — reconsider only if near-real-time Gold/Silver freshness becomes an actual requirement |

---

## Incremental Validations

| Section | Presented | User Feedback | Adjusted? |
|---------|-----------|----------------|-----------|
| Lineage premise + triggering + quarantine history + free_edition scope (4 questions) | ✅ | Confirmed: consistency goal (not lineage fix), keep minimal Job, accept quarantine history loss, fold in free_edition | No — all four taken as given |
| Kafka reachability (dual source_mode) | ✅ | Confirmed: unverified, design for both | No |
| Final approach selection (A vs. B vs. C) | ✅ | Confirmed Approach A | No |

**Minimum Validations:** 2 — satisfied (3 rounds)

---

## Suggested Requirements for /define

### Problem Statement (Draft)
The current Lakeflow migration (`ADR-006`) only covers Bronze+Silver for `dev`/`prod`, leaving
Gold, `silver_users`, and all of `free_edition` on a fragmented mix of 37+ notebook tasks across
3 different Job/target shapes — `PIPELINE_UNIFICATION` consolidates all layers and all 3 targets
into one Lakeflow pipeline resource (`ubereats_pipeline`) and a one-task Job per target.

### Target Users (Draft)
| User | Pain Point |
|------|------------|
| Anyone auditing the pipeline in Unity Catalog | Currently must reason about 3 structurally different target configurations (classic-compute notebook job, Lakeflow pipeline + notebook hybrid, serverless 37-task job) for what is conceptually the same pipeline |
| Future maintainer of `databricks.yml` | Currently maintains a ~290-line anchor scaffold (`task_definitions`, `serverless_tasks`) purely to keep `free_edition` symmetric with a job structure `dev`/`prod` no longer fully uses |

### Success Criteria (Draft)
- [ ] `pipelines/ubereats_pipeline.py` registers Bronze (20), Silver (10), `silver_users`, and
      Gold (6) — 37 tables total, one Python module
- [ ] `databricks.yml`'s three targets (`dev`/`prod`/`free_edition`) each define exactly one
      pipeline resource + one Job with exactly one `pipeline_task`
- [ ] All 8 legacy notebooks (`pipeline_bronze`, `pipeline_silver`, `pipeline_users`,
      6× `cross_domain/gold_*`) are either deleted or explicitly archived, not left as dead
      orphaned files
- [ ] `source_mode` (kafka \| volume) is configurable per target and verified to actually work for
      at least one mode before this feature ships
- [ ] A new ADR (007) documents the reversal of `ADR-006`'s "Explicitly NOT migrated" section and
      `ADR-05`'s `free_edition`-specific Kafka-reachability premise

### Constraints Identified
- Kafka reachability from serverless compute is unverified — must be tested, not assumed, before
  `/build` is considered complete for the `kafka` source_mode path
- `dp.create_auto_cdc_flow` / `create_streaming_table` semantics (used for the 10 generic Silver
  domains) are unchanged by this feature — only `silver_users` and Gold get new registration
  functions
- Existing Gold aggregation logic (join order to avoid fan-out bias, `row_number()` dedup guards)
  must be ported verbatim — this feature is a packaging/lineage change, not a logic rewrite

### Out of Scope (Confirmed)
- Fixing or proving a Gold→Silver lineage bug (none confirmed to exist)
- Preserving `quarantine.users`'s append-only history
- A native pipeline-level schedule (doesn't exist in the platform)
- Splitting Gold into multiple Python files (one file is sufficient for now)
- A generic contract-driven Gold registration loop

---

## Session Summary

| Metric | Value |
|--------|-------|
| Questions Asked | 6 |
| Approaches Explored | 3 |
| Features Removed (YAGNI) | 5 |
| Validations Completed | 3 |
| Duration | Single session, same conversation as the `databricks.yml` serverless/schema fixes that surfaced the shared-workspace fact |

---

## Next Step

**Ready for:** `/define .claude/sdd/features/BRAINSTORM_PIPELINE_UNIFICATION.md`
