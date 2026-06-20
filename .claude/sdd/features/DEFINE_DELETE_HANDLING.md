# DEFINE: Delete Handling (C08 fix)

> Fix the confirmed gap where a Postgres `DELETE` does not remove a row from Silver/Gold — it NULLs out every non-key column instead, permanently.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | DELETE_HANDLING |
| **Date** | 2026-06-20 |
| **Author** | Claude (investigation session) |
| **Status** | Ready for Design |
| **Clarity Score** | 14/15 |

---

## Problem Statement

A Postgres `DELETE` on any of the 10 generic Silver domains is not propagated as
a delete anywhere downstream. Confirmed live against the local stack
(2026-06-20): no table sets `REPLICA IDENTITY FULL`, so the Debezium
before-image of a `DELETE` only has the row's primary key populated — every
other column is `NULL`. `delete.handling.mode=rewrite` turns that into a Kafka
record with the merge key populated and every other field `NULL` (plus
`__deleted="true"`, confirmed present in the registered Avro schema).
`register_silver()` doesn't filter on `__op`/`__deleted`, so this record flows
into `create_auto_cdc_flow()` (no `apply_as_deletes`), which applies it as
`WHEN MATCHED THEN UPDATE SET *` — overwriting every non-key column of the
existing Silver row with `NULL`, permanently, while the row stays visible in
Gold. `users` (handled separately by `register_silver_users()`) takes a
different, less destructive path: it filters `__op != 'd'` before dedup, so
the deleted user's last live state persists forever instead of being NULLed —
stale, not corrupted, but still never actually removed. See
`CLAUDE.md`'s "DELETE is not propagated..." entry and `.claude/kb/anti-patterns.md`
(C08) for the full trace.

This has never been observed in a real pipeline run because
`tests/load_to_postgres.py` only does `INSERT ... ON CONFLICT DO UPDATE` — the
path is real but unexercised by this project's own test data.

---

## Target Users

| User | Role | Pain Point |
|------|------|------------|
| Pipeline operator | Runs/monitors `ubereats_pipeline` | A real upstream DELETE would silently corrupt Silver/Gold instead of erroring — no alert fires, nothing looks obviously wrong until someone notices NULLs in a "should-be-stable" entity |
| Downstream Gold consumer | Reads `gold.driver_performance`/`gold.revenue_per_restaurant`/`gold.user_behavior` | A deleted driver/restaurant/user keeps showing up in aggregates indefinitely — either as stale (pre-fix `users` path) or NULL-corrupted (the 10 generic domains) |

---

## Goals

| Priority | Goal |
|----------|------|
| **MUST** | A Postgres `DELETE` on any of the 10 generic Silver domains actually removes the corresponding row from `silver.<domain>` (not a NULL-out) |
| **MUST** | A Postgres `DELETE` on `users_mongo`/`users_mssql` actually removes the user from `silver.users` once the delete is the latest known event for that `cpf` (today it freezes on the last live state instead) |
| **MUST** | Gold tables (full-recompute `@dp.table`) reflect the deletion automatically on their next run, with no Gold-side code change required |
| **SHOULD** | The fix does not require quarantining/dropping a delete signal due to unrelated `not_null` quality rules on non-key fields (several domains, e.g. `drivers.driver_id`, declare `on_failure: quarantine` on a field that is not the Postgres primary key) |
| **COULD** | Quarantine/audit log retains a record of what was deleted and when, for traceability |

---

## Success Criteria

- [ ] A manual `DELETE` on a `payments`/`drivers`/`orders` (etc.) row in Postgres results in that row being absent from `silver.<domain>` after the next pipeline run — not NULLed, actually absent
- [ ] A manual `DELETE` on a `users_mongo` or `users_mssql` row (where it's the only/last source for that `cpf`) results in that `cpf` being absent from `silver.users` after the next run
- [ ] No existing `tests/test_contracts.py` / `tests/test_dlt_adapter.py` test regresses (193 passing today)
- [ ] `tests/test_dlt_adapter.py` gains coverage for whatever new quarantine/CDC routing logic is added for `__op='d'` rows

---

## Acceptance Tests

| ID | Scenario | Given | When | Then |
|----|----------|-------|------|------|
| AT-001 | Delete on a generic Silver domain | A `payments` row exists in `silver.payments` | The corresponding Postgres row is `DELETE`d and the pipeline re-runs | The row is absent from `silver.payments` (not NULL-filled) |
| AT-002 | Delete on a domain with a non-key quarantine rule | A `drivers` row exists in `silver.drivers` (contract has `driver_id not_null, on_failure: quarantine`, and `driver_id` is not the Postgres PK) | The corresponding Postgres row is `DELETE`d | The delete signal still reaches `create_auto_cdc_flow`'s `apply_as_deletes` — it is not swallowed by the `driver_id not_null` quarantine rule |
| AT-003 | Delete on users (single source) | A `users_mongo` row exists and is the only source for a given `cpf` | The Postgres row is `DELETE`d | That `cpf` is absent from `silver.users` after the next run (today: it persists with its last live state forever) |
| AT-004 | Update still works (no regression) | A `payments` row is updated (not deleted) | The pipeline re-runs | `silver.payments` reflects the update normally — `apply_as_deletes`/quarantine changes do not affect non-delete rows |
| AT-005 | Gold reflects the deletion with no Gold code change | AT-001 has happened | The next Gold run executes | `gold.revenue_per_restaurant`/`gold.driver_performance`/etc. no longer include the deleted entity, with zero changes to `register_gold_*()` functions |

---

## Out of Scope

- Fixing `prod`'s `kafka` source_mode being unverified/unreachable (separate, pre-existing gap — `CLAUDE.md`)
- PII masking (`kb/governance.md`'s open gap — unrelated to delete propagation)
- Backfilling/repairing rows already NULL-corrupted by this bug before the fix ships (a one-time data-repair task, not part of this design — flag separately if real corrupted rows exist in `dev`/`prod` today)
- Soft-delete/tombstone retention UX (e.g., "show deleted drivers in a separate audit view") — only correctness of removal is in scope, not a richer delete-audit feature

---

## Constraints

| Type | Constraint | Impact |
|------|------------|--------|
| Technical | `users_mongo`/`users_mssql`'s Postgres PK is `uuid`, not `cpf` — under `REPLICA IDENTITY DEFAULT` a delete-rewrite event for these tables has `cpf = NULL`, breaking `_prepped_users()`'s `cpf_key` derivation entirely | `users` cannot be fixed without either `REPLICA IDENTITY FULL` on these two tables, or a different uuid-keyed lookup mechanism |
| Technical | Several contracts (e.g. `drivers.yml`) declare a `quarantine`-routing `not_null` rule on a field that is not the Postgres primary key | Under `REPLICA IDENTITY DEFAULT`, a delete-rewrite row would have that field `NULL` and get caught by the existing quarantine predicate before ever reaching `create_auto_cdc_flow` — the delete signal must not be swallowed this way |
| Technical | Lakeflow's `apply_as_deletes` parameter and CDC-flow tombstone retention behavior is publicly documented (`docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes`), but whether changing it on an *existing* streaming table requires a `full_refresh=true` to correctly reprocess history is **not** documented — must be verified empirically in `dev` before relying on it for `prod` | Deployment plan must include a `dev` full-refresh test, not just a code change |
| Dataset | 129k records total, "architectural microcosm" framing (`CLAUDE.md`) | Any WAL-volume increase from `REPLICA IDENTITY FULL` is a non-issue at this scale — would need re-evaluation if this project's data volume target changes |

---

## Technical Context

| Aspect | Value | Notes |
|--------|-------|-------|
| **Deployment Location** | `pipelines/ubereats_pipeline.py` (`register_silver()`, `register_silver_users()`), `contracts/dlt_adapter.py`, possibly `scripts/init.sql` or a new migration for `REPLICA IDENTITY` | No new top-level directory needed — this is a fix within the existing pipeline/contracts modules |
| **KB Domains** | `kb/medallion.md` (Bronze/Silver/Gold + CDC flow conventions), `kb/data-quality.md` (contract quality rules, quarantine routing), `kb/anti-patterns.md` (C08 — this gap), `kb/kafka-cdc.md` (Debezium delete.handling.mode mechanics) | All four need their "open gap" language updated to "fixed" once this ships |
| **IaC Impact** | Possibly a Postgres schema change (`ALTER TABLE ... REPLICA IDENTITY FULL`) — not Databricks IaC, but does touch `sql/init.sql`-equivalent setup | Confirm exactly which tables need it before designing the migration |

---

## Assumptions

| ID | Assumption | If Wrong, Impact | Validated? |
|----|------------|------------------|------------|
| A-001 | Lakeflow's `apply_as_deletes` deletes the target row by `keys` match alone, without needing the other columns of the incoming row to be non-null | If wrong, `REPLICA IDENTITY FULL` becomes mandatory for all 10 generic domains, not just to avoid the quarantine-swallowing problem | [ ] — inferred from how `keys`-based matching works in Auto CDC, not explicitly confirmed in Databricks docs |
| A-002 | Changing `create_auto_cdc_flow()`'s `apply_as_deletes` argument requires a `full_refresh=true` run to correctly reprocess Bronze history (so already-NULL-corrupted rows, if any exist, get recomputed correctly) | If wrong (i.e. it "just works" incrementally), the deployment plan can skip the full-refresh step; if it's required and skipped, old corrupted rows stay corrupted | [ ] — not found in Databricks docs after 2 targeted lookups; must be tested in `dev` |
| A-003 | `REPLICA IDENTITY FULL` is required for `users_mongo`/`users_mssql` specifically (PK ≠ join key `cpf`), but the other 10 generic domains can be fixed without it if quarantine routing is also patched to not swallow `__op='d'` rows | If wrong, every domain needs `REPLICA IDENTITY FULL`, which is simpler to design (one rule, not two) but increases WAL volume across all 20 source tables instead of 2 | [ ] — derived this session from reading each contract's quarantine rules against each table's real Postgres PK; not exhaustively checked for all 20 |

**Note:** A-003 in particular should be checked exhaustively for all 10 generic Silver domains before Design finalizes the approach — this DEFINE only spot-checked `drivers` (confirmed: `driver_id` is quarantine-checked but is not the Postgres PK, `uuid` is).

---

## Clarity Score Breakdown

| Element | Score (0-3) | Notes |
|---------|-------------|-------|
| Problem | 3 | Confirmed live against the local stack, not inferred — exact mechanism traced end-to-end |
| Users | 2 | Real but generic ("pipeline operator", "Gold consumer") — no named stakeholder, this is an internal correctness fix |
| Goals | 3 | MUST/SHOULD/COULD clearly prioritized, each goal independently testable |
| Success | 3 | Concrete, verifiable against a live `DELETE` + pipeline re-run |
| Scope | 3 | Out-of-scope explicitly excludes the easy-to-conflate adjacent gaps (PII, prod kafka mode, historical data repair) |
| **Total** | **14/15** | Docked 1 point on Users — acceptable for an internal correctness fix, not a user-facing feature |

---

## Open Questions

1. ~~Does `REPLICA IDENTITY FULL` need to go on all 20 source tables or just a subset?~~ **Resolved during DEFINE (2026-06-20):** exhaustively checked all 10 generic Silver domains' `quality.rules` against each table's real Postgres PK (`sql/init.sql`). **Every single one** has at least one `on_failure: quarantine` rule on a non-PK field (`payment_events`: PK=`event_id`, quarantine on `payment_id`; `orders`: PK=`order_id`, quarantine on `user_key`/`restaurant_key`/`total_amount`; `drivers`: PK=`uuid`, quarantine on `driver_id`; `restaurants`: PK=`uuid`, quarantine on `cnpj`; `order_items`/`driver_shifts`/`order_status`/`search_events`/`recommendations` likewise). There is no domain where the quarantine-swallowing problem doesn't apply, and `users_mongo`/`users_mssql` need it regardless (their join key `cpf` ≠ PK `uuid`). **Recommendation entering Design: set `REPLICA IDENTITY FULL` uniformly on all 20 source tables** — one Postgres-side rule, no special-casing, and it doesn't need re-verification every time a new quarantine rule is added to a contract later (a quarantine-bypass-for-deletes code fix would).
2. Is a `full_refresh=true` pipeline run acceptable operationally for `prod` once this ships (re-reads all of Bronze, re-applies CDC from scratch)? At 129k records this is cheap, but confirm there's no reason it'd be disruptive.
3. Should historical rows already NULL-corrupted (if any exist in `dev`/`prod` right now from prior testing) be repaired as part of this work, or tracked as a separate follow-up? (Currently Out of Scope — confirm that's the right call.)

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-06-20 | Claude (investigation session) | Initial version, based on live-verified C08 finding |

---

## Next Step

**Ready for:** `/design .claude/sdd/features/DEFINE_DELETE_HANDLING.md`
