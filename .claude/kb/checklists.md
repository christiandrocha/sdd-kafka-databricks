# KB: Definition of Done — sdd-kafka-databricks
# Objective acceptance criteria before declaring a pipeline/contract change done.
# Present this checklist to the user before saying a task is complete —
# don't just claim success because tests pass.

## Level 1 — Required for any change to contracts/ or pipelines/ubereats_pipeline.py

### Code & contracts

- [ ] **Contract validates** — `pytest tests/test_contracts.py -v` passes for the touched domain
- [ ] **Adapter translation correct** — if `quality.rules` changed, `pytest tests/test_dlt_adapter.py -v` passes
- [ ] **`merge_key` ∈ `cluster_by`** — ADR-04, enforced by `test_06_merge_key_in_cluster_by`
- [ ] **No new bare table names** — every read/write uses `f"{CATALOG}.<layer>.<domain>"`
- [ ] **No hardcoded secrets** — credentials via `spark.conf.get(...)` / `.env`, never literals
- [ ] **Lint clean** — `make lint` (ruff + yamllint) passes

### Data & validation

- [ ] **Idempotency unaffected** — re-running the pipeline (either `source_mode`) doesn't duplicate rows; if you touched `register_silver()`/`register_bronze()`, re-derive why this still holds (see `kb/medallion.md`)
- [ ] **Quarantine routing checked** — if you added/changed a `quality.rules` entry, confirm which scope (`bronze`/`silver`) and `on_failure` (`reject`/`quarantine`/`warn`) you actually want — `check: unique` does **not** enforce at runtime for the 10 generic Silver domains (`kb/data-quality.md`)
- [ ] **PII reviewed** — if the change touches `cpf`, `cnpj`, `email`, `phone_number`, `license_number`, or any field in `kb/governance.md`'s PII table, note it explicitly even though no masking exists yet

### Documentation

- [ ] **ADR referenced or written** — if the change is an architecture decision, link it from `CLAUDE.md`'s relevant section, not just the commit message
- [ ] **CLAUDE.md still accurate** — re-read the section your change touches; this project has already had one doc/code drift (`check: unique`) caught and fixed — don't add another

## Level 2 — Recommended before merging to main

- [ ] **CI green** — `lint`, `test-contracts`/`test` (whichever job name is current), `bundle-validate` (dev + free_edition) all pass
- [ ] **`databricks bundle validate --target prod`** run locally if the change touches `databricks.yml`
- [ ] **`.claude/05_implementation_log.md` updated** — per `CLAUDE.md`'s "Continuous improvement" section
- [ ] **Liquid Clustering alignment re-checked** — any new domain's `cluster_by` matches its `merge_key`, not just copy-pasted from a similar contract

## Level 3 — Before a prod cutover (not relevant at current 129k-row dataset scale)

- [ ] **Load tested at realistic volume** — this project is explicitly a microcosm (`CLAUDE.md`'s "Dataset framing"); don't skip this step when it stops being one
- [ ] **`kafka` source_mode verified reachable from Databricks** — currently undocumented/unverified for `prod` (`CLAUDE.md`)
- [ ] **PII masking applied** — `kb/governance.md`'s Unity Catalog column-mask patterns, not just documented as a gap
- [ ] **Delta retention properties set explicitly** — `delta.logRetentionDuration`/`delta.deletedFileRetentionDuration` (currently unset, `kb/anti-patterns.md` M06)

## References

- `kb/medallion.md` — Bronze/Silver/Gold conventions this checklist assumes
- `kb/data-quality.md` — contract anatomy, what `check: unique` actually does today
- `kb/governance.md` — PII fields and masking patterns (not yet implemented)
- `kb/anti-patterns.md` — what looks risky but is intentional here vs. a real gap
