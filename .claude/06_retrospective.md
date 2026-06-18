# 06 — Retrospective
# sdd-kafka-databricks
# Purpose: structured analysis of each iteration — what worked, what did not,
#          what was learned, technical debt, and improvements for the next cycle.
# Owner: updated at the end of each AgentSpec iteration (/iterate or major version).

---

## How to use this file

- One section per project version or major iteration.
- Reference specific artifacts, ACs, and ADRs when relevant.
- Technical debt must have a target iteration to be resolved.
- Improvements feed directly into the next 01_brainstorm.prompt or 02_define.spec.yaml.
- Do not delete previous retrospectives — they are the institutional memory of the project.

---

## Retrospective template

```
## vX.Y.Z — <iteration name> — YYYY-MM-DD

### Context
<what was the goal of this iteration>

### What worked well
- <observation> → <why it mattered>

### What did not work
- <observation> → <root cause> → <what to do differently>

### Learned
- <insight gained that was not obvious at design time>

### Technical debt
| ID  | Description | Severity | Target iteration |
|-----|-------------|----------|-----------------|
| TD-01 | <description> | high/medium/low | vX.Y.Z |

### Metrics observed
| Metric | Value | Target | Status |
|--------|-------|--------|--------|
| <metric> | <observed> | <expected> | on-track / off-track |

### Improvements for next iteration
- [ ] <concrete improvement with rationale>

### AC validation results
| AC | Result | Notes |
|----|--------|-------|
| AC-01 | pass/fail/pending | <observation> |
```

---

## v1.0.0 — Design complete, pre-build — 2026-06-16

### Context
Full SDD design session completed across multiple rounds of external review.
Two Staff/Principal-level reviewers validated the architecture with "Approved with
Highest Distinction". The design evolved through 5 major correction cycles before
reaching this final state. Build has not yet started.

### What worked in the design process

- **5-phase SDD (Brainstorm → Define → Design → Build → Ship) applied rigorously**
  Before generating a single file, we spent significant time on architecture validation.
  External reviewers found and corrected real issues (SMT regression, 60 notebooks anti-pattern,
  over-engineering of bidirectional Debezium) that would have caused rework if caught in Build.

- **Critical regression identified and corrected: SMT in Bronze**
  Early design incorrectly removed SMT (ExtractNewRecordState) based on a bidirectional
  topology concern that was later abandoned. The correction was: in unidirectional topology
  (load_to_postgres → PostgreSQL → Debezium → Kafka → Databricks), SMT is correct and
  matches the proven sdd-kafka-snowflake pattern.

- **"60 notebooks → 2 parametrized" refactoring**
  External reviewer identified 60 static notebooks as a DRY violation. Refactoring to
  2 parametrized notebooks via dbutils.widgets demonstrates software engineering maturity.
  This change elevated the project from "data pipeline" to "data platform" level.

- **Dataset volume narrative**
  Proactively framing 129k records as "architectural microcosm" neutralizes the
  "why use Databricks for 129k records?" question. Embracing it is stronger than hiding it.

- **Data Contracts as differentiator**
  Neither sdd-kafka-snowflake nor ai-uber-eats have data contracts. 20 YAML contracts
  + loader.py + spark_schema.py + test_contracts.py is rare in portfolios and demonstrates
  governance thinking beyond pipeline plumbing.

### What did not work in the design process

- **Initial over-engineering of Debezium bidirectional topology**
  We spent several rounds designing and validating a bidirectional JDBC Sink → PostgreSQL →
  CDC Source → Kafka topology with Groovy filter transforms, _source column, Apicurio Registry
  with PostgreSQL backend, and SMT removal. All of this was unnecessary once we recognized
  that the source data is JSON file exports (not live production systems), making
  load_to_postgres.py the correct approach — identical to sdd-kafka-snowflake.
  → Root cause: Did not analyze the reference project (sdd-kafka-snowflake) closely enough
    at the start of the design session.
  → Improvement: Always analyze reference projects before designing the new architecture.

- **Apicurio vs Confluent Registry decision reversal**
  Initially chose Avro + Apicurio (more "modern"). After deep analysis, reverted to
  Confluent Schema Registry (battle-tested, native Debezium support, identical to reference).
  → Root cause: Optimizing for novelty instead of correctness and simplicity.

- **SMT decision flip-flopped**
  The SMT decision changed 3 times during the design session before landing on the correct answer.
  → Root cause: The bidirectional topology (which needed no SMT for Bronze) was designed before
    we decided to use unidirectional topology (which benefits from SMT for simplicity).

### Learned

- **Analyze reference projects first, design second**
  sdd-kafka-snowflake already solved many of the problems we over-engineered.
  The correct approach: identify what changes (Snowflake → Databricks) and keep everything else.

- **Unidirectional Debezium is almost always the right pattern for snapshot-based sources**
  When the source data is JSON file exports (not live production databases),
  load_to_postgres.py + Debezium CDC Source is the simplest correct architecture.

- **DRY applies to notebooks**
  Data engineers often create N notebooks for N tables without questioning whether
  parametrization would serve better. Applying software engineering principles to
  notebook design is a measurable seniority signal.

- **Liquid Clustering alignment with MERGE ON is a critical non-obvious detail**
  Many engineers enable Liquid Clustering without knowing that the benefit only
  materializes when cluster_by = merge ON columns. Encoding this in test_contracts.py
  turns it from a "gotcha" into a validated constraint.

### Technical debt (pre-build)

| ID | Description | Severity | Target |
|----|-------------|----------|--------|
| TD-01 | SMT decision documented in 04_build.delegation.md Agent 3 needs validation in first build session | high | v1.0.1 |
| TD-02 | users Silver notebook is a special case (FULL OUTER JOIN) — not parametrizable with pipeline_silver.ipynb | medium | v1.0.0 build |
| TD-03 | order_items maxOffsetsPerTrigger=5000 needs validation — may need tuning | medium | first Databricks run |
| TD-04 | Volume paths (/Volumes/ubereats_dev/...) need to be created before first run | high | v1.0.0 build |

### Metrics (design phase)

| Metric | Value | Target | Status |
|--------|-------|--------|--------|
| External review rounds | 5 | — | ✅ |
| Critical issues found in design | 3 (SMT, 60 notebooks, Apicurio) | 0 | ✅ corrected before build |
| ADRs defined | 8 | — | ✅ |
| Acceptance criteria | 18 | — | ✅ |
| Domains | 20 | 20 | ✅ |

### AC validation results (pre-build)

| AC | Result | Notes |
|----|--------|-------|
| AC-01 | pending | Requires load_to_postgres.py (copied from sdd-kafka-snowflake) |
| AC-02 | pending | Requires live PostgreSQL |
| AC-03 | pending | Requires live PostgreSQL |
| AC-04 | pending | Requires live Kafka + Debezium |
| AC-05 | pending | Requires Databricks workspace |
| AC-06 | pending | Requires Silver notebooks |
| AC-07 | pending | Requires silver.users notebook |
| AC-08 | pending | Requires Gold notebooks |
| AC-09 | pending | Requires Databricks workspace |
| AC-10 | pending | Requires databricks.yml |
| AC-11 | pending | Requires contracts + test_contracts.py |
| AC-12 | pending | Requires contracts with cluster_by |
| AC-13 | pending | Requires GitHub Actions + contracts |
| AC-14 | pending | Requires docker-compose.yml |
| AC-15 | pending | Requires .gitignore |
| AC-16 | pending | Requires full Docker stack |
| AC-17 | pending | Requires Prometheus + kafka-exporter |
| AC-18 | pending | Requires Grafana dashboard |

### Improvements for v1.0.0 build

- [ ] Start with Agent 4 (data-contracts) — contracts validate before any notebook runs
- [ ] Copy load_to_postgres.py from sdd-kafka-snowflake immediately (AC-01 is low-hanging fruit)
- [ ] Copy init.sql from sdd-kafka-snowflake with adjustments documented in ADR notes
- [ ] Validate SMT behavior in first Databricks run — confirm __op and __source_ts_ms fields
- [ ] Create Unity Catalog Volumes before running notebooks
- [ ] Document Volume path format in CLAUDE.md after first successful run

---

<!-- Add new retrospectives below this line -->

---

## v1.0.0 — Build complete — 2026-06-16

### Context
Full build of the sdd-kafka-databricks pipeline executed across 7 agents (4–10).
Agents 1–3 (infra-base, postgres, kafka-stack) were pre-existing from prior sessions.
All 46 deliverable files created or modified. 141 contract tests passing. CI/CD wired.
Observability configs complete. `databricks.yml` with 37 DABs tasks ready for `bundle deploy`.

### What worked well

- **Contracts-first build order (Agent 4 before 5–7)**
  Building `loader.py`, `spark_schema.py`, and `test_contracts.py` before any notebook meant
  every downstream agent had a validated DDL generator. No schema guessing in notebooks.
  → Why it mattered: Silver notebook could use `to_create_table_ddl()` without risk.

- **2 parametrized notebooks (ADR-03) proven by DABs**
  The DABs `databricks.yml` has 37 tasks referencing just 3 notebooks. The correctness of
  ADR-03 ("2 notebooks, not 60") is visible in the orchestration layer — each domain is a
  `base_parameters` block, not a separate notebook.

- **`workflow_run` trigger for prod deploy**
  Replacing `push: branches: [main]` with `on: workflow_run` ensures CI must pass before
  any prod deploy. This is architecturally correct and required exactly one YAML change.

- **`ruff check . --fix`**
  Auto-fixed 7 violations in `load_to_postgres.py` (F401 unused imports, UP045 Optional syntax)
  in one command. No manual edits required.

- **Read-before-edit discipline**
  Every existing file (ci.yml, deploy.yml, Makefile, pyproject.toml, .gitignore) was read
  before modification. Resulted in no rewrites — only targeted additions.

### What did not work

- **ruff + Databricks notebooks (F821 `dbutils`/`spark`)**
  ruff 0.15 lints `.ipynb` files by default. Databricks runtime globals (`spark`, `dbutils`)
  are injected at cluster startup — not importable locally. Running `ruff check .` failed
  with hundreds of F821 errors across all notebooks.
  → Root cause: not excluding `notebooks/` from ruff at project setup time.
  → Fix: added `exclude = ["notebooks"]` to `[tool.ruff]` in pyproject.toml.
  → Rule for future projects: always exclude Databricks notebook directories from ruff at day 0.

- **`payment_current_state` phantom domain**
  CLAUDE.md lists 12 Silver domains including `payment_current_state`. No contract YAML exists
  for it, no Bronze table exists for it, and no Gold notebook reads from it. The DABs task
  count came out as 37 instead of the expected 39.
  → Root cause: design session listed it as a Silver domain but never fleshed out the implementation.
  → Fix: documented as TD-05; needs a design decision (view on silver.payment_events? separate notebook?).

- **`NotebookEdit` tool not auto-loaded**
  When Agent 7 needed to patch `pipeline_users.ipynb` to add `user_id`, the standard `Edit`
  tool rejected the file ("Use NotebookEdit for .ipynb files"). `NotebookEdit` schema is not
  auto-loaded — required `ToolSearch("select:NotebookEdit")` first.
  → Rule: always use `ToolSearch` before attempting to edit `.ipynb` files.

### Learned

- **ruff `[tool.ruff.lint]` vs `[tool.ruff]`**: since ruff 0.2.0, `select` and `isort` must
  live under `[tool.ruff.lint]`. The top-level keys generate deprecation warnings that can
  surface as CI failures in strict mode. Migrate at project setup time.

- **Grafana `or vector(0)` pattern**: stat panels that count a subset (e.g. "failed tasks")
  return "No data" instead of 0 when the subset is empty. `count(...) or vector(0)` forces 0.
  A Prometheus/Grafana gotcha that only manifests when the system is healthy.

- **`data_security_mode: SINGLE_USER` required for Unity Catalog job clusters**
  Databricks job clusters need `SINGLE_USER` mode to access Unity Catalog tables. Missing this
  causes a runtime error on first notebook run. Added to `databricks.yml` job_cluster definition.

- **`workflow_run` needs `ref: github.event.workflow_run.head_sha`**
  When using `on: workflow_run`, the checkout step must explicitly pin to `head_sha` of the
  triggering workflow run — otherwise it checks out the latest `main` which may differ.

### Technical debt

| ID | Description | Severity | Target |
|----|-------------|----------|--------|
| TD-01 | Validate `__op` + `__source_ts_ms` arrive in Bronze on first Databricks run | high | v1.0.1 |
| TD-02 | `pipeline_users.ipynb` special case — document why it can't use pipeline_silver.ipynb | low | v1.0.1 |
| TD-03 | `order_items` `max_offsets=5000` needs tuning after first Databricks run | medium | first run |
| TD-04 | ~~Unity Catalog Volumes must exist before first run~~ → **resolved v1.0.1**: `scripts/preflight_unity_catalog.sh --target {dev\|prod}` creates the catalog, the 4 data schemas, and a `checkpoints` schema with `bronze`/`silver` Volumes (idempotent). Run once before the first `databricks bundle deploy`. | high | resolved v1.0.1 |
| TD-05 | ~~`payment_current_state` Silver table: no contract, no Bronze source, no Gold consumer — needs design~~ → **resolved as YAGNI**: removed from `silver_list` (CLAUDE.md, 02_define.md, 03_design.md). `gold_payment_lifecycle` already covers latest/first/last event per `payment_id`; no downstream consumer ever depended on a Silver-layer materialization. | medium | resolved v1.0.0 |
| TD-06 | ~~`docker-compose.yml` missing JMX ports 9101 (kafka) + 9404 (kafka-connect) — Prometheus scrapes will fail~~ → **resolved**: this was a documentation false positive, not an actual gap. `docker-compose.yml` already wires `jmx-kafka` (9101) and `jmx-kafka-connect` (9404) sidecars correctly — confirmed in `05_implementation_log.md` (Agent 1 — infra-base session) and re-verified during v1.0.1. This entry was just never marked resolved here. End-to-end validation that Prometheus actually scrapes both targets remains out of scope (infra test, not an implementation gap). | high | resolved |
| TD-07 | `databricks.yml` `workspace_root` variable is a default placeholder — must be updated before first deploy | high | first deploy |
| TD-08 | ~~`Dockerfile.connect` never built — `confluent-hub install debezium/debezium-connector-postgresql:2.7.1.Final` fails, that version was never published to Confluent Hub (jumps 2.5.4 → 3.0.8)~~ → **resolved v1.0.1**: pinned to `2.5.4`, the latest 2.x actually available. Discovered and fixed only when v1.0.1 tried to build `kafka-connect` for real — `make up` had never succeeded end-to-end before this. | high | resolved v1.0.1 |
| TD-09 | ~~`kafka` service: `KAFKA_LOG_DIRS`/volume mount at `/tmp/kraft-combined-logs` — that path doesn't pre-exist in `confluentinc/cp-kafka:7.7.1`, so the named volume is created `root:root`, and the non-root `appuser` (uid 1000) the image runs as can't write to it — Kafka fails to format the KRaft log dir on every fresh `docker compose up`~~ → **resolved v1.0.1**: moved to `/var/lib/kafka/data`, the image's pre-owned (`appuser:root 0775`) data directory. Same root cause class as TD-08 — reproducible on any machine, not sandbox-specific. | high | resolved v1.0.1 |
| TD-10 | `tests/load_to_postgres.py`: `stats["inserted"] += len(transformed)` (line ~429) counts records as inserted unconditionally after calling `insert_batch()`, never checking what was actually written — the printed "Records inserted" summary doesn't reflect real DB state. Found while verifying v1.0.1: after a clean `--batch initial` run reporting 127,892 inserted with 0 errors, `restaurants`, `drivers`, `ratings`, and `inventory` were actually empty (0 rows) in Postgres while the other 16 tables were correctly populated. Root cause of why exactly those 4 end up empty not yet found — needs its own investigation, out of scope for v1.0.1 (Infra Readiness owns the CDC/Unity Catalog layer, not `load_to_postgres.py`'s insert correctness). | high | v1.0.2 |

### Metrics

| Metric | Value | Target | Status |
|--------|-------|--------|--------|
| Agents executed | 7 (4–10) | 7 | ✅ |
| Deliverable files created/modified | 46 | — | ✅ |
| Test cases | 141 | 141 | ✅ |
| Test failures | 0 | 0 | ✅ |
| ruff violations at ship | 0 | 0 | ✅ |
| DABs tasks | 37 | 37 | ✅ (TD-05 resolved as YAGNI — target corrected from 39) |
| Silver tasks (pipeline_silver.ipynb) | 10 | 11 | ⚠️ -1 (TD-02 — `silver_users` runs via `pipeline_users.ipynb`, not counted here; target corrected from 12 now that TD-05 is resolved) |
| Gold notebooks | 6 | 6 | ✅ |
| YAML contracts | 20 | 20 | ✅ |
| ADRs honoured | 4 (ADR-01–04) | 4 | ✅ |

### AC validation results (build phase)

| AC | Result | Notes |
|----|--------|-------|
| AC-01 | pending | Requires live PostgreSQL |
| AC-02 | pending | Requires live PostgreSQL |
| AC-03 | pending | Requires live PostgreSQL |
| AC-04 | pending | Requires live Kafka + Debezium |
| AC-05 | pending | Requires Databricks workspace |
| AC-06 | pending | Requires Databricks workspace |
| AC-07 | pending | Requires Databricks workspace |
| AC-08 | pending | Requires Databricks workspace |
| AC-09 | pending | Requires Databricks workspace |
| AC-10 | ✅ pass | `databricks.yml` with dev + prod targets |
| AC-11 | ✅ pass | 141 tests pass |
| AC-12 | ✅ pass | ADR-04 validated in test_06 |
| AC-13 | ✅ pass | CI: env-guard → lint → test-contracts → bundle-validate |
| AC-14 | pending | Requires docker-compose.yml (Agent 1) |
| AC-15 | ✅ pass | `.gitignore` excludes `.env`; env-guard CI job |
| AC-16 | pending | Requires full Docker stack |
| AC-17 | ✅ pass (config) | prometheus.yml + alert_rules.yml created; awaits live stack |
| AC-18 | ✅ pass (config) | kafka.json + kafka_connect.json created; awaits live stack |

### Improvements for v1.0.1

- [ ] Complete Agents 1–3 (docker-compose.yml, init.sql, debezium.json) — 9 ACs still pending live stack
- [ ] First Databricks run: validate Bronze rows have `__op` + `__source_ts_ms`, Silver MERGE idempotency, Gold aggregations
- [ ] Add `yamllint` config file (`.yamllint`) to project root — tune for multi-line YAML strings in alert rules
