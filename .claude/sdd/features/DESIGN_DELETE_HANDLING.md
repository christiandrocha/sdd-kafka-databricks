# DESIGN: Delete Handling (C08 fix)

> Technical design for making a Postgres `DELETE` actually remove rows from Silver/Gold instead of NULL-corrupting them.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | DELETE_HANDLING |
| **Date** | 2026-06-20 |
| **Author** | design-agent (Claude) |
| **DEFINE** | [DEFINE_DELETE_HANDLING.md](./DEFINE_DELETE_HANDLING.md) |
| **Status** | Ready for Build |

---

## Architecture Overview

```text
┌───────────────────────────────────────────────────────────────────────────┐
│                    BEFORE (confirmed bug, C08)                            │
├───────────────────────────────────────────────────────────────────────────┤
│ Postgres DELETE                                                           │
│   └─▶ REPLICA IDENTITY DEFAULT → before-image = PK only, rest NULL       │
│         └─▶ Debezium rewrite → Kafka record: {merge_key, ...all NULL...} │
│               └─▶ register_silver(): candidate_view                      │
│                     ├─▶ quarantine predicate matches (a non-PK field      │
│                     │     the contract checks not_null IS null)          │
│                     │     → row → quarantine.<domain>  [SWALLOWED HERE]  │
│                     └─▶ (if it had passed) clean_view → create_auto_cdc  │
│                           _flow() with NO apply_as_deletes                │
│                           → WHEN MATCHED UPDATE SET *  → NULLs the row   │
│                                                                            │
│ RESULT: row stays in Silver/Gold forever, either quarantined-and-ignored │
│ or NULL-corrupted — never actually removed.                              │
└───────────────────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────────────────┐
│                     AFTER (this design)                                   │
├───────────────────────────────────────────────────────────────────────────┤
│ Postgres DELETE                                                           │
│   └─▶ REPLICA IDENTITY FULL (all 20 tables) → before-image = full row   │
│         └─▶ Debezium rewrite → Kafka record: {all real last-known values,│
│               __op='d', __deleted='true'}                                │
│               └─▶ register_silver(): candidate_view                      │
│                     ├─▶ quarantine predicate: passes normally (fields    │
│                     │     aren't NULL anymore — it's a real last-known   │
│                     │     row, same as any other valid row)              │
│                     └─▶ clean_view → create_auto_cdc_flow(               │
│                           apply_as_deletes=expr("__op = 'd'"))            │
│                           → target row DELETED, matched by merge_key     │
│                                                                            │
│ users (separate path, register_silver_users()):                          │
│   _prepped_users() stops filtering __op != 'd' BEFORE dedup;             │
│   dedup_by_cpf() now sees the delete row too (cpf survives thanks to     │
│   REPLICA IDENTITY FULL), picks it as "latest" by __source_ts_ms, and    │
│   the cpf is then excluded from silver.users entirely.                   │
│                                                                            │
│ RESULT: row is actually absent from Silver. Gold (full-recompute,        │
│ no code change) reflects it on the next run automatically.              │
└───────────────────────────────────────────────────────────────────────────┘
```

---

## Components

| Component | Purpose | Technology |
|-----------|---------|------------|
| Postgres `REPLICA IDENTITY FULL` | Makes the DELETE before-image carry every column, not just the PK | PostgreSQL DDL, applied via `sql/init.sql` + a one-time `ALTER TABLE` for already-running environments |
| `register_silver()`'s `create_auto_cdc_flow()` call | Actually deletes the target row instead of upserting a NULL-filled one | `pyspark.pipelines` (`dp.create_auto_cdc_flow`, `apply_as_deletes` argument) |
| `register_silver_users()`'s dedup logic | Excludes a `cpf` from `silver.users` once its latest known event is a delete | Existing PySpark window-function dedup, modified |
| `tests/test_dlt_adapter.py` | Regression coverage for the new delete-routing behavior | pytest |

No new files, no new infrastructure beyond the Postgres DDL change — this is a fix within 3 existing modules.

---

## Key Decisions

### Decision 1: `REPLICA IDENTITY FULL` on all 20 source tables, not a subset

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-20 |

**Context:** DEFINE's exhaustive check (all 10 generic Silver domains' `quality.rules` vs. each table's real Postgres PK) found that **every one** has a `quarantine`-routing rule on a non-PK field. Under `REPLICA IDENTITY DEFAULT`, a delete-rewrite row has that field `NULL` and gets quarantined before ever reaching `create_auto_cdc_flow` — the delete signal never arrives. `users_mongo`/`users_mssql` independently need it because their join key (`cpf`) isn't the Postgres PK (`uuid`) either, so a delete-rewrite event can't even be matched to a user without it.

**Choice:** Set `REPLICA IDENTITY FULL` on all 20 CDC-source tables uniformly, not just the ones currently known to need it.

**Rationale:** A per-table "does this one need it" analysis has to be re-run every time a contract adds a new `quarantine`-routing rule on a non-PK field — it's a hidden coupling between contract authoring and Postgres DDL that's easy to violate silently in the future. A uniform rule removes that coupling entirely: it never matters which field a quality rule checks, because delete-rewrite rows never have NULL business fields in the first place.

**Alternatives Rejected:**
1. `REPLICA IDENTITY FULL` only on `users_mongo`/`users_mssql` (structurally required) + bypass quarantine routing for `__op='d'` rows on the other 10 domains (no Postgres change needed there) — rejected because it leaves two different fix mechanisms for what is conceptually the same bug, and re-introduces the "will this break if a future contract adds a quarantine rule on a different field" risk for the 10 generic domains specifically (the bypass approach also works regardless of which field is null, but mixing two strategies for arbitrary historical reasons adds cognitive load for no real benefit once accepted as a non-issue at this scale).
2. Leave `REPLICA IDENTITY DEFAULT` everywhere, fix only via quarantine-bypass-for-deletes on all domains including `users` — rejected because it cannot work for `users_mongo`/`users_mssql` at all: their `cpf` (needed to identify which user to remove) is not the PK and would be `NULL` regardless of quarantine routing, since `_prepped_users()`'s `cpf_key` derivation happens directly off the raw `cpf` column, not through a contract quality rule.

**Consequences:**
- Trade-off accepted: `REPLICA IDENTITY FULL` increases WAL volume (Postgres logs the full old row, not just the PK, on every `UPDATE`/`DELETE`) — explicitly a non-issue at this project's 129k-row "architectural microcosm" scale (`CLAUDE.md`), revisit if data volume targets ever change.
- Benefit gained: one Postgres-side rule, zero pipeline-code special-casing per domain, robust to future contract changes.

---

### Decision 2: `apply_as_deletes=expr("__op = 'd'")` in `create_auto_cdc_flow()`, not a `__deleted='true'` quarantine route

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-20 |

**Context:** Two ways to stop the NULL-corruption were on the table: (a) actually delete the row via Lakeflow's native CDC delete support, or (b) intercept `__deleted='true'` rows and route them to quarantine instead of letting them reach `create_auto_cdc_flow` at all.

**Choice:** Use `apply_as_deletes`, Lakeflow's purpose-built mechanism for this exact case (confirmed via `docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes`: accepts a string or `expr()` boolean condition; Lakeflow retains the deleted row as a tombstone temporarily to handle out-of-order events, then removes it).

**Rationale:** Option (b) only stops the corruption — it doesn't make the deletion happen. The row would just freeze at its last good state forever, same failure mode `users` already has today (silently stale, not obviously wrong, never alerts anyone). That's not actually a fix for what DEFINE's MUST goals ask for ("a DELETE actually removes the row"), just a smaller version of the same bug.

**Alternatives Rejected:**
1. Quarantine on `__deleted='true'` (Option B from the original investigation) — rejected as insufficient per Rationale above; kept on record in `kb/anti-patterns.md`/`CLAUDE.md` as the "if `apply_as_deletes` turns out to be infeasible" fallback, not the primary plan.
2. Use `__deleted='true'` instead of `__op='d'` as the `apply_as_deletes` condition — rejected because `__deleted` is an undeclared field (not in any contract's `schema:`), already flagged as a recurring source of bugs (`kb/schema-registry.md`'s "undeclared field reaching the registry" section, `export_kafka_to_volume.py`'s `_cast_record()` history). `__op` is declared in every contract and already relied upon elsewhere (`_prepped_users()`) — prefer the field with a stable, documented contract presence.

**Consequences:**
- Trade-off accepted: relies on Lakeflow's tombstone-retention internals for out-of-order delete/update sequencing — not independently re-verified beyond the public doc fetch in this session (Assumption A-001 in DEFINE).
- Benefit gained: matches the semantics the contract's `merge_key`/CDC model already promises elsewhere (Silver as current-state-of-truth, not an append log) — `silver.<domain>` actually means "exists right now," consistently.

---

### Decision 3: Full-refresh deployment, verified in `dev` before `prod`

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-20 |

**Context:** Databricks' public docs (two targeted fetches this session) don't state whether changing `apply_as_deletes` on an existing streaming table reprocesses history or only affects new incoming records (DEFINE's Assumption A-002, unresolved).

**Choice:** Treat a `full_refresh=true` run as required, and verify this empirically in `dev` (trigger one, confirm previously-NULL-corrupted-if-any or stale rows resolve correctly) before deploying to `prod`.

**Rationale:** Given the project's "Dataset framing" (129k rows, microcosm, not production volume — `CLAUDE.md`), a full refresh is cheap here regardless of whether it's strictly required, so defaulting to "do it and verify" costs little and removes the risk of silently leaving old corrupted rows uncorrected.

**Alternatives Rejected:**
1. Assume incremental reprocessing is sufficient without testing — rejected, since it directly risks DEFINE's Success Criteria (deleted rows must actually be absent, not just newly-deleted ones going forward).

**Consequences:**
- Trade-off accepted: one extra manual verification step in the Build/Ship checklist before this can be marked done.
- Benefit gained: confidence the fix actually reprocesses any already-corrupted history, not just future deletes.

---

## File Manifest

| # | File | Action | Purpose | Agent | Dependencies |
|---|------|--------|---------|-------|--------------|
| 1 | `sql/init.sql` (or wherever `CREATE TABLE` statements for the 20 source tables live) | Modify | Add `ALTER TABLE <t> REPLICA IDENTITY FULL;` for all 20 tables, so fresh environments get it from creation | (direct — plain SQL DDL, no specialist agent matches) | None |
| 2 | A one-time migration step (documented in `docs/adr/` or a `scripts/` snippet, not necessarily a new permanent script) | Create | Apply `REPLICA IDENTITY FULL` retroactively to any already-running Postgres instance (local dev, anyone's existing `make up` stack) | (direct) | None |
| 3 | `pipelines/ubereats_pipeline.py` — `register_silver()` | Modify | Add `apply_as_deletes=expr("__op = 'd'")` to the `create_auto_cdc_flow()` call (10 generic domains) | @lakeflow-expert | 1, 2 |
| 4 | `pipelines/ubereats_pipeline.py` — `register_silver_users()` / `_prepped_users()` / `_dedup_by_cpf()` | Modify | Stop filtering `__op != 'd'` before dedup; dedup by latest `__source_ts_ms` per `cpf_key` (now including delete rows), then exclude any `cpf_key` whose winning row has `__op='d'` from `silver.users` | @lakeflow-expert | 1, 2 |
| 5 | `tests/test_dlt_adapter.py` | Modify | Add coverage: a delete-rewrite-shaped row (merge key + all real values, `__op='d'`) is routed to `clean_view`, not swallowed by an unrelated `not_null` rule on a non-key field | @test-generator | 3 |
| 6 | New test (same file or a new `tests/test_silver_users_delete.py`) | Create | Verify the `users` dedup logic correctly excludes a `cpf` once its latest event is `__op='d'`, and does NOT regress the existing `duplicate_user_id` quarantine behavior | @test-generator | 4 |
| 7 | `CLAUDE.md`, `.claude/kb/anti-patterns.md` (C08), `.claude/kb/data-quality.md`, `.claude/kb/medallion.md`, `.claude/kb/kafka-cdc.md` | Modify | Update all "open gap" language to "fixed as of {date}", once Decisions 1-3 are actually implemented and verified — not before | (direct — documentation) | 3, 4, plus the dev full-refresh verification from Decision 3 |
| 8 | `docs/adr/` | Create | New ADR recording this design (matches the project's existing convention of one ADR per significant architecture decision) | (direct) | None |

**Total Files:** 8 (2 new, 6 modified)

---

## Agent Assignment Rationale

> Agents discovered from `.claude/agents/` — Build phase invokes matched specialists.

| Agent | Files Assigned | Why This Agent |
|-------|----------------|-----------------|
| @lakeflow-expert | 3, 4 | Specializes in Lakeflow/DLT CDC operations (`docstring`: "troubleshooting Lakeflow pipelines... CDC implementation with APPLY CHANGES") — exact match for `apply_as_deletes` wiring |
| @test-generator | 5, 6 | "Test automation expert for Python. Generates pytest unit tests" — matches `tests/test_dlt_adapter.py` conventions already in this repo |
| (general/direct) | 1, 2, 7, 8 | No specialist agent for raw Postgres DDL, doc-sync, or ADR authoring in this project's `.claude/agents/` roster — Build handles these directly, same as most of this session's own doc/config fixes |

**Agent Discovery:**
- Scanned: `.claude/agents/**/*.md` (69 agent files)
- Matched by: purpose keywords ("Lakeflow", "CDC", "APPLY CHANGES", "pytest")

---

## Code Patterns

### Pattern 1: `apply_as_deletes` in the generic Silver registration

```python
# pipelines/ubereats_pipeline.py, register_silver() — current call:
dp.create_streaming_table(name=silver_table, cluster_by=cluster_by)
dp.create_auto_cdc_flow(
    target=silver_table,
    source=clean_view,
    keys=[merge_key],
    sequence_by=col("__source_ts_ms"),
    stored_as_scd_type=1,
)

# Fixed:
dp.create_streaming_table(name=silver_table, cluster_by=cluster_by)
dp.create_auto_cdc_flow(
    target=silver_table,
    source=clean_view,
    keys=[merge_key],
    sequence_by=col("__source_ts_ms"),
    apply_as_deletes=expr("__op = 'd'"),
    stored_as_scd_type=1,
)
```

### Pattern 2: `users` dedup-then-exclude-deletes

```python
# Current _prepped_users() — filters deletes out BEFORE dedup, so the
# second-to-latest (live) row always wins instead:
def _prepped_users(bronze_table: str):
    return (
        dp.read(bronze_table)
        .filter(col("__op") != "d")   # <- removes this filter
        .withColumn("cpf_key", regexp_replace(col("cpf"), r"[.\-]", ""))
    )

# Fixed — keep delete rows through dedup, so the LATEST event (including a
# delete) determines the outcome:
def _prepped_users(bronze_table: str):
    return (
        dp.read(bronze_table)
        .withColumn("cpf_key", regexp_replace(col("cpf"), r"[.\-]", ""))
    )

def _dedup_by_cpf(df):
    """Keep the latest row per cpf_key (highest __source_ts_ms). If the
    winning row is a delete (__op='d'), the cpf_key is dropped entirely —
    the user is gone, not frozen at their last live state."""
    w = Window.partitionBy("cpf_key").orderBy(desc("__source_ts_ms"))
    return (
        df.withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .filter(col("__op") != "d")   # <- moved here, AFTER picking the latest
        .drop("_rn")
    )
```

`REPLICA IDENTITY FULL` (Decision 1) is what makes this safe — without it,
`cpf` is `NULL` on the delete row and `cpf_key` derivation fails before any of
this logic runs.

### Pattern 3: Postgres migration

```sql
-- sql/init.sql (or equivalent) — added once per table at creation time:
ALTER TABLE payment_events REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE payments REPLICA IDENTITY FULL;
-- ... all 20 tables (mechanical, same line repeated per CREATE TABLE block)

-- One-time, for any already-running Postgres instance (local dev stacks
-- started before this fix shipped):
-- (run once via psql "$DATABASE_URL", not part of any repeated script)
```

---

## Data Flow

```text
1. Postgres DELETE on e.g. `payments` (REPLICA IDENTITY FULL)
   │
   ▼
2. Debezium logs full before-image to WAL; ExtractNewRecordState SMT rewrites
   it as a normal-shaped record: all real last-known field values + __op='d'
   + __deleted='true'
   │
   ▼
3. register_bronze(): flows into bronze.payments unchanged (Bronze is an
   append-only log of every event — this part is already correct)
   │
   ▼
4. register_silver(): candidate_view sees a row that passes every quality
   rule normally (values aren't NULL) — reaches clean_view
   │
   ▼
5. create_auto_cdc_flow(apply_as_deletes=expr("__op = 'd'")): matches the row
   in silver.payments by `payment_id` (keys=[merge_key]) and deletes it
   │
   ▼
6. Next Gold run (dp.read(silver.payments), full recompute, no code change):
   the deleted payment is simply absent from whatever aggregate read it
```

---

## Integration Points

| External System | Integration Type | Authentication |
|-----------------|-----------------|-----------------|
| PostgreSQL | DDL change (`ALTER TABLE ... REPLICA IDENTITY FULL`) | Existing `DATABASE_URL` credentials, no new auth |
| Databricks Lakeflow | `apply_as_deletes` parameter on an existing API call | No change — same pipeline, same auth |

No new external system, no new credential, no new network path.

---

## Testing Strategy

| Test Type | Scope | Files | Tools | Coverage Goal |
|-----------|-------|-------|-------|-----------------|
| Unit | `quarantine_row_level_predicate()`/`apply_as_deletes` expression construction | `tests/test_dlt_adapter.py` | pytest | A delete-shaped row (all real values, `__op='d'`) passes every existing quality rule and is not misrouted |
| Unit | `users` dedup-then-exclude logic | New test in `tests/test_dlt_adapter.py` or a new file | pytest | AT-003 from DEFINE: a `cpf` whose latest event is a delete is excluded from the joined output |
| Integration (manual, AT-001/AT-002) | Real Postgres DELETE → Bronze → Silver, via the local docker-compose stack (same method used to confirm C08 this session) | Manual `psql` DELETE + Databricks Connect or a `dev` pipeline run | `psql`, Databricks CLI | Row is verifiably absent from `silver.<domain>` after a re-run, not NULLed |
| Integration (manual, full-refresh check) | Decision 3's open assumption | A `dev` full-refresh pipeline run | Databricks CLI/UI | Confirms whether `full_refresh=true` is actually required — resolves DEFINE's Assumption A-002 |
| Regression | All existing contract/adapter tests | `tests/test_contracts.py`, `tests/test_dlt_adapter.py` | pytest | 193/193 still passing (current baseline) |

---

## Error Handling

| Error Type | Handling Strategy | Retry? |
|------------|--------------------|--------|
| Delete-rewrite row still fails an unrelated quality rule even with full values (e.g., a genuinely malformed historical row) | Falls through to the existing quarantine path as today — acceptable, since `apply_as_deletes` only matters for rows that reach `clean_view` in the first place; a row that's quarantined for an unrelated reason was already a data-quality problem independent of this fix | No — same as existing quarantine behavior |
| `apply_as_deletes` misfires on a row that isn't actually a delete (e.g., `__op` typo/corruption) | `__op` is a Debezium-controlled field already relied upon elsewhere (`_prepped_users()`); no new failure surface introduced — same trust boundary as today | No |
| Full refresh required but skipped in `prod` | Document explicitly in the Build/Ship checklist (Decision 3) — this is a deployment-process risk, not a code-level one | N/A — process control, not runtime retry |

---

## Configuration

No new configuration keys. `apply_as_deletes` is a literal `expr("__op = 'd'")` in code, not a tunable — every domain uses the same condition, consistent with how `sequence_by=col("__source_ts_ms")` is already hardcoded identically across all 10 domains in `register_silver()`.

---

## Security Considerations

- No new PII exposure — this fix makes deleted PII actually disappear from Silver/Gold instead of lingering (NULL-corrupted or stale), which is a net improvement adjacent to `kb/governance.md`'s open PII-masking gap (not a substitute for it — masking still matters for rows that haven't been deleted).
- `REPLICA IDENTITY FULL` means Postgres logs full row contents (including PII fields) to the WAL on every UPDATE/DELETE, not just the PK — already true for INSERTs and the Kafka topic itself, so this doesn't introduce a new place PII is exposed, just extends an existing exposure surface (WAL) to also cover delete/update before-images. Worth a one-line note in `kb/governance.md` once implemented.

---

## Observability

| Aspect | Implementation |
|--------|-----------------|
| Logging | None added — Lakeflow's own pipeline event log already records `apply_as_deletes`-driven row removals; no custom logging needed |
| Metrics | Not addressed by this design — `kb/observability.md` already flags that no Prometheus alert would catch a recurrence of this bug (it's a data-correctness gap, not an infra failure); a data-quality metric (e.g., a scheduled query counting unexpectedly-NULL-heavy rows per `merge_key`) is explicitly **out of scope** here, tracked as a follow-up in `kb/observability.md` |
| Tracing | N/A |

---

## Pipeline Architecture

### DAG Diagram

```text
[Postgres DELETE] ──CDC(REPLICA IDENTITY FULL)──→ [Kafka, full before-image]
                                                          │
                                                          ▼
                                              [Bronze: bronze.<domain>]
                                                          │
                                                          ▼
                                          [candidate_view → quarantine | clean_view]
                                                          │
                                                          ▼
                                [silver.<domain>: create_auto_cdc_flow,
                                 apply_as_deletes="__op = 'd'" → row DELETED]
                                                          │
                                                          ▼
                                  [gold.*: full recompute, no code change —
                                   deletion reflected automatically]
```

### Schema Evolution Plan

| Change Type | Handling | Rollback |
|-------------|----------|----------|
| `REPLICA IDENTITY FULL` (Postgres) | Apply via `ALTER TABLE` — no data migration, no downtime, takes effect for subsequent WAL records only | `ALTER TABLE ... REPLICA IDENTITY DEFAULT` — reverts to the pre-fix (buggy) behavior, no data loss from the revert itself |
| `apply_as_deletes` added to `create_auto_cdc_flow()` | Code change + `full_refresh=true` run (Decision 3) | Remove the argument + another `full_refresh=true` to undo the reprocessing — **this would re-introduce the NULL-corruption for any deletes that occurred while the fix was live**, so rollback is not free |

### Data Quality Gates

| Gate | Tool | Threshold | Action on Failure |
|------|------|-----------|---------------------|
| All existing contract tests still pass | `tests/test_contracts.py` | 0 failures | Block merge |
| New adapter tests for delete routing | `tests/test_dlt_adapter.py` | 0 failures | Block merge |
| Manual AT-001/AT-002/AT-003 verification in `dev` | Manual, per Testing Strategy | All 3 pass | Block `prod` deploy — do not ship without this, since it's the only check that exercises the actual Lakeflow runtime behavior this whole fix depends on |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-06-20 | design-agent (Claude) | Initial version, from DEFINE_DELETE_HANDLING.md |

---

## Next Step

**Ready for:** `/build .claude/sdd/features/DESIGN_DELETE_HANDLING.md`
