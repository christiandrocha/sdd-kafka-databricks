# ADR 008 — DELETE Propagation Requires REPLICA IDENTITY FULL + apply_as_deletes

**Status:** Accepted (Postgres/Debezium side verified live; Databricks/Lakeflow side implemented, pending dev verification)
**Date:** 2026-06-20

## Context

A live test against the local Postgres/Kafka/Debezium stack (`docker compose up`,
a manual `psql DELETE` on `payments`, inspecting the resulting Kafka message)
confirmed a real gap, tracked as `kb/anti-patterns.md` C08: no table set
`REPLICA IDENTITY FULL`, so Postgres only logged primary-key columns in a
`DELETE`'s before-image. With `delete.handling.mode=rewrite`
(`connectors/debezium.json`), the rewritten Kafka record had the merge key
populated and **every other field `NULL`**. `register_silver()` had no
`apply_as_deletes` on its `create_auto_cdc_flow()` call, so this record was
applied as `WHEN MATCHED THEN UPDATE SET *` — **a Postgres `DELETE` NULLed
every non-key column of the Silver row instead of removing it, permanently.**
`users_mongo`/`users_mssql` (handled separately by `register_silver_users()`)
took a less destructive but still wrong path: `_prepped_users()` filtered
`__op != 'd'` before dedup, so the deleted user's last live state persisted
forever instead of being NULLed — stale, not corrupted, but never removed.

This was never observed in a real pipeline run because
`tests/load_to_postgres.py` only issues `INSERT ... ON CONFLICT DO UPDATE` —
the path was real but unexercised by this project's own test data.

Full requirements and an exhaustive check of all 10 generic Silver domains'
`quality.rules` against each table's real Postgres PK:
`.claude/sdd/features/DEFINE_DELETE_HANDLING.md`. Full design with 3 decisions,
alternatives rejected, and a file manifest:
`.claude/sdd/features/DESIGN_DELETE_HANDLING.md`.

## Decision

1. **`REPLICA IDENTITY FULL` on all 20 source tables**, not a subset
   (`sql/init.sql` for fresh environments, `scripts/migrate_replica_identity.sh`
   for already-running ones).
2. **`apply_as_deletes=expr("__op = 'd'")`** added to `create_auto_cdc_flow()`
   in `register_silver()` (the 10 generic Silver domains).
3. **`register_silver_users()`'s dedup logic changed**: `_prepped_users()` no
   longer filters `__op != 'd'` before dedup; `_dedup_by_cpf()` now picks the
   latest row per `cpf_key` first (delete rows included), then excludes the
   `cpf_key` entirely if that latest row is a delete.

## Rationale

**Why `REPLICA IDENTITY FULL` on all 20 tables, not just the 2 that strictly
need it (`users_mongo`/`users_mssql`, whose join key `cpf` ≠ Postgres PK
`uuid`):** every one of the 10 generic Silver domains has at least one
`quarantine`-routing `not_null` rule on a field that is *also* not the
Postgres PK (`payment_events`: PK=`event_id`, quarantine on `payment_id`;
`orders`: PK=`order_id`, quarantine on `user_key`/`restaurant_key`/
`total_amount`; `drivers`: PK=`uuid`, quarantine on `driver_id`; `restaurants`:
PK=`uuid`, quarantine on `cnpj`; and so on for the rest — checked
exhaustively, see DEFINE). With `REPLICA IDENTITY DEFAULT`, a delete-rewrite
row would have that field `NULL` and get quarantined before ever reaching
`create_auto_cdc_flow` — the delete signal never arrives, regardless of
domain. A uniform rule removes the need to re-check this every time a
contract adds a new quarantine rule on a non-PK field in the future.

**Why `apply_as_deletes` over routing `__deleted='true'` to quarantine
instead:** the quarantine-route alternative only stops the NULL-corruption —
it doesn't make the row disappear, just freezes it at its last good state
forever (the same failure mode `users` already had). `apply_as_deletes` is
Lakeflow's purpose-built mechanism for this exact case (confirmed via
`docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes`:
accepts a string or `expr()` boolean condition; Lakeflow retains the deleted
row as a tombstone temporarily for out-of-order handling, then removes it).

**Why `__op = 'd'` and not `__deleted = 'true'` as the condition:** `__op` is
declared in every contract and already relied upon elsewhere
(`_prepped_users()`); `__deleted` is undeclared schema drift
(`kb/schema-registry.md`'s "undeclared field reaching the registry" section)
already linked to one prior bug in `export_kafka_to_volume.py`'s
`_cast_record()`. Prefer the field with a stable, contract-declared presence.

## Verification

**Postgres/Debezium side — verified live (2026-06-20):** after
`scripts/migrate_replica_identity.sh`, a test `INSERT` + `DELETE` on
`payments` produced a delete-rewrite Kafka record with every real field
populated (`method`, `status`, `amount`, `currency`, `country`, ...) matching
the preceding insert exactly — not just the merge key, confirming the
before-image fix works as designed.

**Databricks/Lakeflow side — implemented, not yet verified:** whether
`apply_as_deletes` requires a `full_refresh=true` run to correctly reprocess
already-ingested Bronze history (vs. only affecting newly-arriving records)
is not addressed in Databricks' public docs (checked twice during DESIGN).
Requires a real Databricks workspace to test — unavailable in this session.
Treat this as implemented-but-unverified until a `dev` deployment confirms it.

## Alternatives considered

- **`REPLICA IDENTITY FULL` only on `users_mongo`/`users_mssql` + bypass
  quarantine routing for `__op='d'` rows on the other 10 domains** — rejected:
  leaves two different fix mechanisms for the same underlying bug, and the
  bypass approach still needs re-verification whenever a new quarantine rule
  is added, which the uniform `REPLICA IDENTITY FULL` rule avoids entirely.
- **Quarantine `__deleted='true'` rows instead of deleting** — rejected: only
  prevents corruption, doesn't fix deletion; trades visible NULL-corruption
  for invisible staleness, arguably a worse failure mode since nothing about
  it looks obviously wrong.

## Consequences

**Positive:** a Postgres `DELETE` on any of the 20 source tables now actually
removes the corresponding row from Silver (verified for the Postgres/Kafka
half; Databricks half implemented per the documented API, pending workspace
verification) instead of corrupting or freezing it. Gold tables (full
recompute, no code change) reflect deletions automatically on their next run.

**Negative:** `REPLICA IDENTITY FULL` increases Postgres WAL volume (full
old-row logging on every `UPDATE`/`DELETE`, not just the PK) — explicitly a
non-issue at this project's 129k-row "architectural microcosm" scale
(`CLAUDE.md`'s "Dataset framing"), but would need re-evaluation if data volume
targets change. Rolling back `apply_as_deletes` after it has been live is not
free — it would require another `full_refresh=true` and would re-introduce
NULL-corruption for any deletes that occurred while the fix was active.

## See also

`.claude/sdd/features/DEFINE_DELETE_HANDLING.md`,
`.claude/sdd/features/DESIGN_DELETE_HANDLING.md`,
`.claude/sdd/features/BUILD_REPORT_DELETE_HANDLING.md` — full requirements,
design, and build trace. `kb/anti-patterns.md` (C08), `kb/kafka-cdc.md`,
`kb/schema-registry.md`, `kb/medallion.md` — KB entries updated alongside this
fix.
