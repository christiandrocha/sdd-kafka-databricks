# BUILD REPORT: Free Edition Bronze — modo Volume além de Kafka streaming

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | FREE_EDITION_BRONZE |
| **Date** | 2026-06-17 |
| **Author** | build-agent |
| **DEFINE** | [DEFINE_FREE_EDITION_BRONZE.md](../features/DEFINE_FREE_EDITION_BRONZE.md) |
| **DESIGN** | [DESIGN_FREE_EDITION_BRONZE.md](../features/DESIGN_FREE_EDITION_BRONZE.md) |
| **Status** | Complete |

---

## Summary

| Metric | Value |
|--------|-------|
| **Tasks Completed** | 8/8 |
| **Files in DESIGN manifest** | 6/6 created/modified |
| **Files added mid-build (approved)** | 2 (`docker-compose.yml`, +6 `contracts/*.yml` type fixes) |
| **Pre-existing bugs found** | 2 (Kafka advertised listener, contract type mismatches) |
| **Pre-existing bugs fixed** | 2 (both, fully) |
| **Live verification** | Export script ran against the real local Kafka stack — 20/20 domains, 127,892 records, real Parquet output validated |

---

## Task Execution with Agent Attribution

| # | Task | Agent | Status | Notes |
|---|------|-------|--------|-------|
| 1 | Add `pyarrow` to `pyproject.toml` | (direct) | ✅ Complete | Installed and verified locally (`pip install --break-system-packages`) |
| 2 | Create `scripts/export_kafka_to_volume.py` | @streaming-engineer | ✅ Complete | 3 real bugs found and fixed during live testing — see Issues |
| 3 | Modify `notebooks/pipeline_bronze.ipynb` | @spark-engineer | ✅ Complete | Widget + branch added; consolidated 2 cells into 1 to keep each `if/elif` self-contained per Jupyter cell |
| 4 | Restructure `databricks.yml` | @ci-cd-specialist | ✅ Complete | YAML anchors verified structurally (37 unique tasks/target, correct `job_cluster_key` presence per target) |
| 5 | Extend `scripts/preflight_unity_catalog.sh` | @shell-script-specialist | ✅ Complete | `ensure_volume()` generalized to take a schema param instead of hardcoding `checkpoints` |
| 6 | Update `CLAUDE.md` | (direct) | ✅ Complete | Documents both `source_mode`s, the new `landing` schema, ADR-05/ADR-06 |
| 7 | Run verification | (direct) | ✅ Complete | ruff, `bash -n`, YAML/JSON parsing, 141 contract tests, live export against real Kafka |
| 8 | Write this BUILD_REPORT | (direct) | ✅ Complete | — |

**Legend:** ✅ Complete | 🔄 In Progress | ⏳ Pending | ❌ Blocked

---

## Agent Contributions

| Agent | Files | Specialization Applied |
|-------|-------|--------------------------|
| @streaming-engineer | 1 | Kafka consumer (`confluent-kafka`), Avro/Schema Registry decoding, Parquet export |
| @spark-engineer | 1 | PySpark notebook restructuring (conditional batch vs. streaming read, reused `merge_to_bronze`) |
| @ci-cd-specialist | 1 | Databricks Asset Bundles — YAML anchors, per-target resource scoping |
| @shell-script-specialist | 1 | Idempotent bash, generalized an existing helper function |
| (direct) | 2 + 7 contract fixes | Dependency bump, documentation, verification, and narrow data-contract type fixes discovered live |

---

## Files Created / Modified

| File | Action | Agent | Verified |
|------|--------|-------|----------|
| `pyproject.toml` | Modified | (direct) | ✅ `pip install`, `import pyarrow` |
| `scripts/export_kafka_to_volume.py` | Created | @streaming-engineer | ✅ live run, 20/20 domains, 127,892 records, Parquet schema spot-checked |
| `notebooks/pipeline_bronze.ipynb` | Modified | @spark-engineer | ✅ JSON valid, all code cells `ast.parse` clean |
| `databricks.yml` | Modified | @ci-cd-specialist | ✅ YAML valid, anchors resolve, 37 unique tasks × 3 targets, correct compute split |
| `scripts/preflight_unity_catalog.sh` | Modified | @shell-script-specialist | ✅ `bash -n` clean |
| `CLAUDE.md` | Modified | (direct) | ✅ reviewed |
| `docker-compose.yml` | Modified (not in manifest) | (direct) | ✅ live (export script connects successfully after fix) |
| `contracts/driver_shifts.yml`, `drivers.yml`, `routes.yml`, `gps_events.yml`, `ratings.yml` | Modified (not in manifest) | (direct) | ✅ 141/141 contract tests still pass; export script processes all affected domains without error |

---

## Verification Results

### Lint / Syntax Checks

```text
ruff check .                                    → All checks passed!
bash -n scripts/preflight_unity_catalog.sh      → OK
bash -n scripts/register_connectors.sh          → OK
python3 -c "import yaml; ...databricks.yml"     → valid YAML, anchors resolve
python3 -c "import yaml; ...contracts/*.yml"    → all 20 valid
python3 -c "import json; ...pipeline_bronze.ipynb" → valid JSON
ast.parse() on every code cell in the notebook  → all valid Python
```

**Status:** ✅ Pass

### Contract Tests

```text
PYTHONPATH=. pytest tests/test_contracts.py -q  → 141 passed
```

**Status:** ✅ Pass (re-run after the 6 contract type fixes — no regressions)

### Live Integration Test — `scripts/export_kafka_to_volume.py`

Ran against the user's real local stack (`make up`, `make register-connectors`,
`tests/load_to_postgres.py --batch initial`):

| Check | Result |
|-------|--------|
| 20/20 domains processed, no crash | ✅ |
| 16/20 domains export real records (127,892 total) | ✅ |
| 4/20 domains (`drivers`, `inventory`, `ratings`, `restaurants`) — topic not found, empty Parquet written gracefully | ✅ (AT-010 — known TD-10 from v1.0.1, not a regression introduced here) |
| Parquet schema matches contract exactly (spot-checked `payment_events`, `driver_shifts`) | ✅ (AT-002) |
| `__op`/`__source_ts_ms` present and correctly typed in output | ✅ |
| Timestamp fields correctly converted from Debezium's ISO-8601 strings to `datetime` | ✅ (after fix — see Issues) |
| `--domain` flag (single-domain debug mode) | ✅ |

**Status:** ✅ Pass — this is real, live evidence, not a syntax-only check (AT-001 satisfied with actual data)

### Not Executed (same restriction as v1.0.1)

| Test | Reason |
|------|--------|
| Bronze `source_mode=volume` against a real Databricks workspace | Requires a live workspace — out of scope for `/build`, user must verify manually |
| `databricks bundle validate -t free_edition` | A real, authenticated `.databrickscfg` profile exists locally (`dbc-f3701868-1581`) — per the standing instruction after the v1.0.1 incident, no live calls against it were made |
| Bronze `source_mode=kafka` regression test inside the notebook itself | Notebook code can't run outside a Databricks/Spark runtime; verified by inspection (the `kafka` branch is byte-for-byte the same logic as before, just re-indented under `if`) |

---

## Issues Encountered

| # | Issue | Resolution | Scope |
|---|-------|------------|-------|
| 1 | DABs has no way to exclude a root-level resource from one target ([databricks/cli#2872](https://github.com/databricks/cli/issues/2872)) — found during `/design`, before any code was written | Moved both job definitions (classic, serverless) entirely inside `targets.*.resources`, sharing the 37 task bodies via YAML anchors | Already incorporated into DESIGN Decision 3 — not a build-time surprise |
| 2 | `KAFKA_ADVERTISED_LISTENERS` advertised `PLAINTEXT_HOST://localhost:9094`, but only port 9092 is host-mapped (`docker-compose.override.yml`: `"9092:9094"`) — any Kafka client running outside Docker (our new export script, the first of its kind in this repo) got `Connection refused` reconnecting to the advertised port | Changed the advertised listener to `localhost:9092` (the actually-mapped port); `kafka` container recreated and re-verified healthy | Outside manifest — approved mid-build |
| 3 | 6 contracts declared the wrong primitive type for a field relative to the actual PostgreSQL column (`driver_id` as `integer` when it's `VARCHAR(20)` in 2 contracts; 3 fields declared `integer` where Postgres has `NUMERIC` with decimals; 1 declared `double` where Postgres has plain `INTEGER`) — found because this is the first code to ever cast Avro-decoded values against the contract's declared type | Fixed all 6: `drivers.driver_id`, `routes.driver_id` → `string`; `gps_events.speed_kph`, `ratings.rating`, `routes.estimated_duration_min` → `double`; `driver_shifts.shift_duration_min` → `integer`. Verified `ratings`'s `allowed_values: [1,2,3,4,5]` quality rule still matches (`4.0 in [1,2,3,4,5]` is `True`; actual fixture data is integer-valued ratings anyway) | Outside manifest — approved mid-build, one consolidated approval for all 6 |
| 4 | Debezium emits `TIMESTAMPTZ` columns as ISO-8601 strings (`io.debezium.time.ZonedTimestamp`), not epoch-millis longs as the DESIGN's Pattern 1 assumed — every domain's `_cast_record()` call failed on every timestamp field | Added a `str` branch to `_cast_record()`'s timestamp handling (`datetime.fromisoformat`), alongside the existing `int` branch (still needed for plain `TIMESTAMP` columns under `time.precision.mode=connect`) | Own bug — fixed directly, in manifest (`scripts/export_kafka_to_volume.py`) |
| 5 | `__deleted` (added by the Debezium SMT) isn't in any contract's schema, and `_cast_record()` originally copied the whole record including unknown keys, which would have broken `pa.Table.from_pylist` once the timestamp issue was also fixed | Changed `_cast_record()` to only keep keys present in the contract's schema, dropping anything else | Own bug — fixed directly, in manifest |

---

## Deviations from Design

| Deviation | Reason | Impact |
|-----------|--------|--------|
| `docker-compose.yml` modified (not in the DESIGN manifest) | The advertised-listener bug (#2 above) was a genuine, pre-existing, 100%-reproducible blocker for any Kafka client running outside Docker — confirmed with `nc -zv` against both ports before touching anything | `export_kafka_to_volume.py` now actually works against the real stack; any future host-side Kafka client benefits too |
| 6 `contracts/*.yml` files modified (not in the DESIGN manifest) | Same category as the listener bug: real, pre-existing, evidence-backed type mismatches between the contract and the actual PostgreSQL DDL, discovered only because this is the first code to cast against the contract's declared types | Fixes apply to both `source_mode`s — `pipeline_bronze.ipynb`'s Kafka-streaming path would have hit the same kind of insert failure had it ever processed `driver_shifts`/`drivers`/`routes`/`gps_events`/`ratings` live, since `to_create_table_ddl()` uses the same contract |
| Pattern 1's `_cast_record()` timestamp assumption (int-only) extended to also handle ISO-8601 strings | Discovered live — Debezium's actual wire format for `TIMESTAMPTZ` differs from what DESIGN assumed | No interface change, no impact on other files; purely an internal correction within `export_kafka_to_volume.py` |

---

## Acceptance Test Verification

| ID | Scenario | Status | Evidence |
|----|----------|--------|----------|
| AT-001 | Export completo dos 20 domínios | ✅ Pass | Live run: 20/20 processed, 127,892 records across 16 populated domains |
| AT-002 | Fidelidade ao schema Avro pós-SMT | ✅ Pass | Parquet schema for `payment_events`/`driver_shifts` matches contract exactly, incl. `__op`/`__source_ts_ms` |
| AT-003 | Provisionamento do schema/Volume novo | ✅ Pass (script logic) | `ensure_schema "landing"` + `ensure_volume "landing" "kafka_export"` added, same idempotent pattern as `checkpoints`; **not run live** against a real workspace (see Not Executed) |
| AT-004 | Idempotência do preflight script | ✅ Pass (by construction) | Same `get`/`read`-before-`create` pattern already validated in v1.0.1, just parameterized |
| AT-005 | Bronze em modo `volume` — primeira execução | ⚠️ Not verified live | Requires a real Databricks workspace — out of `/build` scope |
| AT-006 | Bronze em modo `volume` — idempotência | ⚠️ Not verified live | Same as AT-005; logic reviewed — `merge_to_bronze()` is unchanged and already idempotent via `MERGE ... WHEN NOT MATCHED` |
| AT-007 | Bronze em modo `kafka` sem regressão | ✅ Pass (by inspection) | The `kafka` branch is the exact original code, just re-indented under `if source_mode == "kafka":` — no logic changed |
| AT-008 | `databricks.yml` repassa o modo corretamente | ✅ Pass | Verified via `yaml.safe_load`: every Bronze task in every target has `source_mode: ${var.bronze_source_mode}` in `base_parameters`; `dev`/`prod` resolve to `kafka`, `free_edition` to `volume` |
| AT-009 | Contrato/DDL inalterados entre os dois modos | ✅ Pass | `to_create_table_ddl()` call site in the notebook is outside both branches — unaffected by `source_mode` |
| AT-010 | Domínio sem dados no Volume | ✅ Pass | Export script: 4 domains with no Kafka topic → empty Parquet written, no crash; Bronze's existing `batch_df.isEmpty()` check (unchanged) handles the rest |

---

## Final Status

### Overall: ✅ COMPLETE

**Completion Checklist:**

- [x] All 6 files from the DESIGN manifest completed
- [x] 2 additional pre-existing bugs found and fixed (approved mid-build): Kafka advertised listener, 6 contract type mismatches
- [x] All verification checks pass for files in this feature's scope
- [x] Live integration test of the new export script against the real Kafka stack — real data, real Parquet output, schema-verified
- [x] 141/141 contract tests still passing after the type fixes
- [ ] AT-005/AT-006 (Bronze `source_mode=volume` against a real workspace) not verified live — needs manual run
- [x] Ready for `/ship`

**Known follow-up (not blocking, pre-existing):** TD-10 from v1.0.1 (`tests/load_to_postgres.py` insert-count bug) is why 4/20 domains have no Kafka topic to export — unrelated to this feature, already tracked for v1.0.2.

---

## Next Step

**Ready for:** `/ship .claude/sdd/features/DEFINE_FREE_EDITION_BRONZE.md`

**Before first real use on Free Edition:**
1. `scripts/preflight_unity_catalog.sh --target dev` against the real Free Edition workspace (creates `landing`/`kafka_export` too now)
2. `python3 scripts/export_kafka_to_volume.py` locally, then `databricks fs cp -r kafka_export/<domain> dbfs:/Volumes/ubereats_dev/landing/kafka_export/<domain>` per domain
3. `databricks bundle deploy -t free_edition` and run the `ubereats_pipeline` job — confirms AT-005/AT-006 for real
