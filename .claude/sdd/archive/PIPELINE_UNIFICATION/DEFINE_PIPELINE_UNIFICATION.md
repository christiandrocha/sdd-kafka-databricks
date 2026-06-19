# DEFINE: PIPELINE_UNIFICATION

> Consolidate Bronze, Silver, `silver_users`, Gold, and Quarantine into one Lakeflow pipeline resource (`ubereats_pipeline`), shared by `dev`/`prod`/`free_edition`, retiring all 8 legacy notebooks.

## Metadata

| Attribute | Value |
|---|---|
| **Feature** | PIPELINE_UNIFICATION |
| **Date** | 2026-06-19 |
| **Author** | define-agent |
| **Status** | ✅ Shipped |
| **Clarity Score** | 14/15 |

---

## Problem Statement

`ADR-006` migrated only Bronze+Silver to Lakeflow Declarative Pipelines, and only for the `dev`/`prod` targets. Gold (6 notebooks), `silver_users` (1 notebook), and the entirety of `free_edition` (a 37-task notebook job) remain outside that pipeline, so the same conceptual pipeline exists today as three structurally different implementations — a hybrid Lakeflow-pipeline-plus-notebook job for `dev`/`prod`, and a fully separate 37-task classic notebook job for `free_edition`. This asymmetry was deliberate under `ADR-006`'s original assumptions, but a `databricks.yml` session earlier the same day established that all three targets actually run against one shared Free Edition workspace, which removes the premise (`ADR-05`) that `free_edition` is uniquely Kafka-unreachable and therefore must stay separate.

---

## Target Users

| User | Role | Pain Point |
|------|------|------------|
| Anyone auditing the pipeline in Unity Catalog | Platform/data engineer reviewing lineage or job runs | Must reason about 3 structurally different target configurations (classic-compute notebook job, Lakeflow-pipeline+notebook hybrid, serverless 37-task job) for what is conceptually a single Bronze→Silver→Gold pipeline |
| Future maintainer of `databricks.yml` | Whoever next edits the DABs bundle | Maintains a ~290-line `task_definitions`/`serverless_tasks` anchor scaffold purely to keep `free_edition` symmetric with a job structure `dev`/`prod` no longer fully uses |

---

## Goals

| Priority | Goal |
|----------|------|
| **MUST** | One Python module (`pipelines/ubereats_pipeline.py`) registers all 37 tables: Bronze (20), Silver (10 generic + `silver_users`), Gold (6) |
| **MUST** | All 3 `databricks.yml` targets (`dev`/`prod`/`free_edition`) reference exactly one pipeline resource and exactly one Job with exactly one `pipeline_task` each |
| **MUST** | All 8 legacy notebooks (`pipeline_bronze`, `pipeline_silver`, `pipeline_users`, 6× `cross_domain/gold_*`) are deleted or explicitly archived — none left as orphaned dead files |
| **SHOULD** | `source_mode` (`kafka` \| `volume`) for Bronze stays configurable per target via pipeline `configuration`, and is verified working for at least one mode before this feature is considered shippable |
| **SHOULD** | A new `docs/adr/007_*.md` documents the reversal of `ADR-006`'s "Explicitly NOT migrated" section and of `ADR-05`'s `free_edition`-specific Kafka-reachability premise |
| **COULD** | `databricks.yml`'s `_pipeline_anchors` scaffold (`task_definitions`, `serverless_tasks`) is pruned down once no longer referenced, rather than left dead in the file |

---

## Success Criteria

- [ ] `pipelines/ubereats_pipeline.py` contains registration logic for 20 Bronze + 10 generic Silver + 1 `silver_users` + 6 Gold + 11 quarantine tables (37 data tables total, one module)
- [ ] `databricks.yml` defines exactly 1 `resources.pipelines.ubereats_pipeline` block and exactly 1 Job per target, each Job containing exactly 1 `pipeline_task`, for all 3 targets (`dev`, `prod`, `free_edition`)
- [ ] 0 of the 8 legacy notebooks remain referenced by any DABs job task after migration (verified by `databricks bundle validate` showing no task referencing `notebooks/pipeline_bronze.ipynb`, `pipeline_silver.ipynb`, `pipeline_users.ipynb`, or any `cross_domain/gold_*.ipynb`)
- [ ] Existing Gold aggregation logic (join order, `row_number()` dedup guards from `GOLD_DIMENSION_JOIN_INTEGRITY`) is ported with zero logic changes — same join keys, same dedup windows, same quarantine routing as today
- [ ] `silver_users` and `quarantine.users` become `@dp.table` functions using the existing `contracts/dlt_adapter.py` quarantine convention (inverse-predicate pattern), matching the other 10 Silver domains
- [ ] At least one `source_mode` (`kafka` or `volume`) is confirmed working end-to-end on the real Free Edition workspace before the feature is marked done
- [ ] `docs/adr/007_pipeline_unification.md` exists and explicitly supersedes the relevant sections of `ADR-006` and `ADR-05`

---

## Acceptance Tests

| ID | Scenario | Given | When | Then |
|----|----------|-------|------|------|
| AT-001 | Happy path — unified pipeline deploys and runs on `dev` | `pipelines/ubereats_pipeline.py` registers all 37 tables and `databricks.yml`'s `dev` target has 1 pipeline resource + 1-task Job | `databricks bundle deploy -t dev` then the Job is run | All 20 Bronze, 11 Silver (incl. `silver_users`), 11 quarantine, and 6 Gold tables populate in `ubereats_dev` with no task referencing a legacy notebook |
| AT-002 | Error case — duplicate `user_id` still quarantines correctly post-migration | `silver_users` is now a `@dp.table` (not the old notebook) reading Bronze `users_mongo`/`users_mssql` | A duplicate `user_id` exists across the two Bronze sources after the FULL OUTER JOIN | The row lands in `quarantine.users` (inverse-predicate `@dp.table`), not in `silver.users`, matching the old notebook's behavior minus append-only history |
| AT-003 | Edge case — `free_edition` runs the same pipeline under `source_mode: volume` | `free_edition` target's pipeline `configuration` sets `source_mode: volume` and the landing Volume is pre-populated via `scripts/export_kafka_to_volume.py` | The `free_edition` Job's single `pipeline_task` is run | Bronze tables populate from the Volume snapshot (not Kafka), and Silver/Gold populate identically to the `kafka` source_mode path, with idempotent re-runs producing no duplicate rows |
| AT-004 | Edge case — Gold reads only Silver, never Bronze-only domains | Gold `@dp.table` functions are ported from the 6 `cross_domain/gold_*.ipynb` notebooks | The unified pipeline runs Gold registration | Each Gold table's lineage in Unity Catalog shows upstream edges only to `silver.*` tables, never directly to `bronze.*` |

---

## Out of Scope

- Fixing or proving a Gold→Silver Unity Catalog lineage bug — none was confirmed to exist; the motivation is architectural consistency (one resource, one mental model), not a correctness fix
- Preserving `quarantine.users`'s append-only history — the plain `@dp.table` recompute model only holds currently-quarantined rows, and this loss was explicitly confirmed acceptable
- A native pipeline-level schedule — the Databricks pipeline resource has no such field; a minimal one-task Job remains the trigger mechanism
- Splitting Gold into multiple Python files (`pipelines/gold/*.py`) — one file is sufficient for 6 bespoke functions; can be revisited later if it becomes unwieldy
- A generic, contract-driven Gold registration loop mirroring Bronze/Silver's `contracts/*.yml` pattern — Gold transforms have non-uniform join keys/dedup windows/aggregation shapes; forcing a shared abstraction over 6 cases is not justified
- `continuous: true` pipeline mode — changes cost/semantics substantially (always-on compute) for no stated freshness requirement today

---

## Constraints

| Type | Constraint | Impact |
|------|------------|--------|
| Technical | Kafka reachability from Free Edition serverless compute is unverified | Bronze registration must keep a `source_mode` (`kafka` \| `volume`) branch; design for both, confirm at least one at deploy time — this is a pre-ship gate, not a brainstorm-time blocker |
| Technical | `dp.create_auto_cdc_flow`/`create_streaming_table` semantics for the 10 generic Silver domains are unchanged | Only `silver_users` and Gold get new registration functions; the existing 10-domain loop in `bronze_silver_dlt.py` is preserved as-is inside the renamed module |
| Technical | Existing Gold aggregation logic (join order to avoid fan-out bias, `row_number()` dedup guards) must be ported verbatim | This feature is a packaging/lineage consolidation, not a logic rewrite — any behavior change to Gold output would be a regression, not a goal |
| Technical | `databricks.yml` cannot exclude a root-level resource from one target (`databricks/cli#2872`) | Each target still needs its own `resources.jobs.ubereats_pipeline`/pipeline block, even though all 3 are now structurally identical and differ only by `variables:` |

---

## Technical Context

| Aspect | Value | Notes |
|--------|-------|-------|
| **Deployment Location** | `pipelines/ubereats_pipeline.py` (renamed from `pipelines/bronze_silver_dlt.py`) | Single module; new Gold + `silver_users` registration functions added alongside the existing Bronze/Silver `contracts/*.yml` loop |
| **KB Domains** | Lakeflow/DLT pipeline patterns, DABs job/pipeline resource schema, quarantine-as-inverse-predicate convention (`contracts/dlt_adapter.py`) | Reuse `dlt_adapter.py`'s existing expectations/quarantine translation for `silver_users`'s new quarantine table rather than inventing a new convention |
| **IaC Impact** | Modify existing — `databricks.yml`'s `_pipeline_anchors` scaffold (`task_definitions`, `serverless_tasks`, `lakeflow_tasks`) shrinks to one pipeline resource + one 1-task Job anchor, shared by `dev`/`prod`/`free_edition`, differing only by target `variables:` (catalog, `source_mode`, checkpoint/landing paths) | Confirmed current state: `databricks.yml` is 878 lines with `source_mode: ${var.bronze_source_mode}` already wired per-domain at lines 55–378, and a `bronze_silver_pipeline_task` anchor already exists at line 621 — this feature extends that anchor to cover Gold/`silver_users` too and removes the now-dead 37-task `serverless_tasks` anchor for `free_edition` |

**Why This Matters:**

- **Location** → Confirms `pipelines/bronze_silver_dlt.py` is the correct rename target, not a new file — Design phase should treat this as an extension/rename, not a greenfield module
- **KB Domains** → Design phase must reuse `contracts/dlt_adapter.py`'s quarantine pattern for `silver_users`, not invent a parallel mechanism
- **IaC Impact** → `databricks.yml` already has the `source_mode` variable and a pipeline-task anchor in place from the prior Lakeflow migration; Design should plan a deletion/consolidation of the `serverless_tasks`/37-task `task_definitions` anchors rather than treating this as new IaC

---

## Data Contract

### Source Inventory
| Source | Type | Volume | Freshness | Owner |
|--------|------|--------|-----------|-------|
| Kafka (`pg.public.*` topics via Debezium) | Streaming (CDC) | 129,353 records across 20 domains | Near-real-time on `dev`/`prod` when Kafka is reachable from serverless |
| Landing Volume (`/Volumes/<catalog>/landing/kafka_export/`) | Batch (Parquet snapshot) | Same 129,353 records | Manually refreshed via `scripts/export_kafka_to_volume.py`; used when Kafka isn't reachable (`free_edition`, possibly all 3 targets if Kafka-from-serverless is confirmed unreachable) |
| Silver tables (`silver.*`, 11 domains) | Internal (Delta) | N/A — derived | Read-only input to Gold; no new Gold→Bronze edges introduced |

### Schema Contract
No new columns or tables are introduced — this feature repackages existing Bronze/Silver/Gold/Quarantine schemas (defined in `contracts/*.yml` and the existing notebook logic) into one pipeline module. Schema-level details remain governed by the existing per-domain `contracts/*.yml` files; `silver_users`/`quarantine.users` follow the schema already implemented in `notebooks/pipeline_users.ipynb`, ported as-is.

### Freshness SLAs
| Layer | Target | Measurement |
|-------|--------|-------------|
| Bronze/Silver | Unchanged from current Lakeflow pipeline behavior | Pipeline run completion in Unity Catalog Lineage |
| Gold | Unchanged from current notebook behavior — runs after Silver completes within the same pipeline DAG | Pipeline run completion (now a single DAG instead of a separate post-Silver Job task) |

### Completeness Metrics
- Zero rows silently dropped: every quarantine table (11, including `quarantine.users`) continues to capture the inverse-predicate of its Silver table's quality rules
- Idempotent re-runs: re-running the unified pipeline in either `source_mode` must not duplicate rows (existing `MERGE`/`create_auto_cdc_flow` idempotency guarantees, ported unchanged)

### Lineage Requirements
- Unity Catalog Lineage must show each Gold table's upstream edges pointing only to `silver.*` tables, never to `bronze.*` (verified by AT-004)
- Each of the 37 tables should appear as its own lineage node under the single `ubereats_pipeline` pipeline resource, preserving the per-domain lineage granularity `ADR-006` originally established for Bronze/Silver

---

## Assumptions

| ID | Assumption | If Wrong, Impact | Validated? |
|----|------------|------------------|------------|
| A-001 | Serverless compute can reach the self-hosted Kafka broker for at least one of `dev`/`prod`/`free_edition` | If Kafka is unreachable from serverless everywhere, all 3 targets must run `source_mode: volume` permanently, making the `kafka` branch dead code until Enterprise-tier networking is available | [ ] |
| A-002 | `@dp.table` (non-streaming, materialized-view model) is a legitimate full-batch-recompute pattern for `silver_users`'s FULL OUTER JOIN | If Lakeflow's `@dp.table` can't express a full outer join across two Bronze sources cleanly, `silver_users` may need a different DLT primitive or stay a notebook | [ ] |
| A-003 | A standalone Lakeflow pipeline resource has no native schedule, only `continuous` mode or a Job's `pipeline_task` | If a native pipeline schedule exists in a Databricks CLI/schema version newer than what was checked, the "minimal Job" design (Goal MUST #2) could be simplified further | [ ] |
| A-004 | Gold's existing aggregation logic ports into `@dp.table` + `dp.read(silver_*)` without needing streaming semantics | If any Gold notebook implicitly relies on incremental/streaming behavior not visible in a full-table read, output could differ after migration | [ ] |

**Note:** A-001 and A-002 were already explored during brainstorming (Discovery Q5, Key Data Question 3) and are considered low-risk but formally unverified — both are gated as MUST/SHOULD success criteria above, not blockers to starting Design.

---

## Clarity Score Breakdown

| Element | Score (0-3) | Notes |
|---------|-------------|-------|
| Problem | 3 | Specific, grounded in current file structure (3 ADRs, 8 notebooks, 878-line `databricks.yml`), with a confirmed root cause (shared Free Edition workspace) for why the original split no longer holds |
| Users | 3 | Two concrete personas (UC lineage auditor, `databricks.yml` maintainer) each with a specific, observable pain point tied to real artifacts |
| Goals | 3 | Prioritized MUST/SHOULD/COULD list, each goal stated as a concrete file/resource-count outcome |
| Success | 3 | All criteria are numerically checkable (37 tables, 1 resource, 1 Job, 1 task per target, 0 legacy references) |
| Scope | 2 | Out-of-scope list is explicit and well-reasoned, but the `source_mode` Kafka-reachability outcome remains genuinely open and could reshape Design's IaC approach depending on which mode is confirmed working |
| **Total** | **14/15** | Exceeds the 12/15 gate; the one-point deduction reflects a real unresolved technical unknown (A-001), not a clarity gap in the requirements themselves |

**Minimum to proceed: 12/15**

---

## Open Questions

None blocking Design. One item carries forward as a pre-ship gate rather than a Design blocker:

- Which `source_mode` (`kafka` or `volume`) actually works on the shared Free Edition workspace — Design should produce a pipeline that supports both, with the live verification happening during/after `/build` (per brainstorm Decision #7 and Constraint above).

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-06-19 | define-agent | Initial version, extracted directly from `BRAINSTORM_PIPELINE_UNIFICATION.md` (Approach A, all 8 key decisions, all 5 success criteria) |
| 1.1 | 2026-06-19 | ship-agent | Shipped and archived — see `SHIPPED_2026-06-19.md` |

---

## Next Step

**Ready for:** `/design .claude/sdd/features/DEFINE_PIPELINE_UNIFICATION.md`
