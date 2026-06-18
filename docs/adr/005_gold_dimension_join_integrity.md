# ADR 005 — Gold Dimension Joins Must Target a Column Enforced Unique in Silver

**Status:** Accepted
**Date:** 2026-06-18

## Context

A lineage audit of all 6 `notebooks/cross_domain/` Gold notebooks found that 3 of them
join a Silver dimension table on a column that is **not** that table's `merge_key`:

| Notebook | JOIN on | Real `merge_key` (contract) |
|---|---|---|
| `gold_user_behavior` | `silver.users.user_id` | `cpf` |
| `gold_driver_performance` | `silver.drivers.driver_id` | `uuid` |
| `gold_revenue_per_restaurant` | `silver.restaurants.cnpj` | `uuid` |

`merge_key` is the only column a Silver contract guarantees unique (it is the `MERGE INTO
... ON` column). Nothing guaranteed `user_id`, `driver_id`, or `cnpj` were unique within
their tables. The `user_id` case already failed in practice with
`DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` in `gold_user_behavior`; the other
two were latent, unconfirmed risks of the same shape.

Two fixes were proposed: (A) realign `merge_key` to the column the Gold layer joins on, or
(B) add a `row_number()` guard in each Gold notebook right before its `MERGE`.

## Decision

Neither in isolation. A new quality-rule type, `check: unique`, is added to the contract
schema and applied in Silver on the actual join-key column (`driver_id` in
`contracts/drivers.yml`, `cnpj` in `contracts/restaurants.yml`; `user_id` by hand in
`pipeline_users.ipynb`, which has no YAML contract). `merge_key` itself is left unchanged.
The `row_number()` guard (Approach B) is kept everywhere this join pattern exists — not just
`gold_user_behavior` — as defense-in-depth, not as the fix.

## Rationale

**Why not Approach A as proposed (realign `merge_key`):**
1. It would violate ADR-004 — `cluster_by` must contain `merge_key`, and the ADR-004
   alignment table already fixes `silver.users` at `cluster_by=[cpf]`/`merge_key=cpf`.
   Changing `merge_key` means a Liquid Clustering rewrite, not a YAML edit.
2. `merge_key` is the CDC identity, not a business attribute. For `drivers`/`restaurants`,
   `uuid` is literally the Debezium-sourced Postgres/MySQL primary key driving
   `MERGE INTO ... ON t.merge_key = s.merge_key` every micro-batch. Redefining "which update
   replaces which row" around a business attribute introduces a new risk larger than the one
   being fixed.
3. For `users`, `cpf` is the correct key by design — CLAUDE.md documents CPF as the hub
   table's canonical FK to users; `user_id` is a secondary attribute carried only because
   `search_events`/`recommendations` don't have CPF. Changing `merge_key` to `user_id` would
   invert an already-deliberate architecture decision, not correct it.

**Why Approach B alone is insufficient:** a `row_number()` guard picks an arbitrary survivor
silently. That is the same anti-pattern this project already moved away from for missing-CPF
users (commit `158b2bd`, "Quarantine users missing CPF instead of silently dropping them") —
a real data-integrity problem becomes invisible instead of visible.

**Why `check: unique` + quarantine is the right mechanism:** it keeps the contract YAML as
the single source of truth for what "valid" means (alongside `not_null`/`allowed_values`/
`not_future`), and routes violations to quarantine — consistent with the project's existing
convention — instead of silently resolving them. It is evaluated via an anti-join against
the existing Silver table plus the incoming batch (`pipeline_silver.ipynb`), which is
affordable at this project's stated scale (hundreds of rows for `drivers`/`restaurants`;
CLAUDE.md already frames the dataset as "an architectural microcosm, not a production
volume").

**Why keep the guard anyway:** the new rule only protects rows ingested after it is
deployed; it does not retroactively re-validate rows already in Silver. The
`row_number()` guard in Gold is what protects against that historical gap (and any future
rule/contract drift), at near-zero cost.

## Alignment table (this ADR's scope)

| Gold notebook | Dimension joined | Join column | Silver `merge_key` | Silver-side guarantee | Gold-side guard |
|---|---|---|---|---|---|
| `gold_user_behavior` | `silver.users` | `user_id` | `cpf` | `pipeline_users.ipynb` quarantines duplicate `user_id` within each full-refresh batch | `row_number()` on `user_id` (pre-existing) |
| `gold_driver_performance` | `silver.drivers` | `driver_id` | `uuid` | `contracts/drivers.yml` `check: unique` on `driver_id` | `row_number()` on `driver_id` (added) |
| `gold_revenue_per_restaurant` | `silver.restaurants` | `cnpj` | `uuid` | `contracts/restaurants.yml` `check: unique` on `cnpj` | `row_number()` on `restaurant_cnpj` (added) |

## Alternatives considered

- **Realign `merge_key`** — rejected (see Rationale above).
- **`row_number()` guard only, no Silver-side rule** — rejected: silently resolves a data
  integrity problem instead of surfacing it; inconsistent with the project's quarantine
  convention.
- **Uniqueness enforced only in CI (`test_contracts.py`), after the fact** — rejected: would
  catch the problem only after bad data already reached Silver/Gold, not before.

## Consequences

**Positive:** the actual join-key used by each affected Gold notebook is now guaranteed
unique going forward, with violations surfaced in quarantine rather than silently dropped or
arbitrarily resolved; the `row_number()` pattern is now applied consistently across all 3
notebooks instead of just one.

**Negative:** `apply_quality_rules` in `pipeline_silver.ipynb` is no longer purely row-level
— the `unique` check requires one extra read of the target Silver table per micro-batch.
`pipeline_users.ipynb` needed a hand-written equivalent since `users` has no YAML contract
(pre-existing technical debt, not introduced by this change).

## See also

`.claude/sdd/features/DESIGN_GOLD_DIMENSION_JOIN_INTEGRITY.md` — full design with file
manifest and code patterns.
