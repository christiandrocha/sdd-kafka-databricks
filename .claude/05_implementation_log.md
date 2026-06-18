# 05 — Implementation Log
# sdd-kafka-databricks v1.0.0 — SHIPPED 2026-06-16
# Purpose: chronological record of what was built, problems encountered,
#          solutions applied, and decisions made during /build that were
#          not captured in the design phase.
# Owner: updated by the agent or engineer executing each build task.
#
# BUILD SUMMARY
# Agents: 1 (infra-base), 4 (data-contracts), 5 (spark-bronze), 6 (spark-silver),
#         7 (spark-gold), 8 (orchestration), 9 (ci-cd), 10 (observability)
# Files:  54 created or modified
# Tests:  141 passed, 0 failed
# Tasks:  37 DABs (20 Bronze + 10 Silver + 1 Users + 6 Gold)
# Status: ✅ Shipped — archive: .claude/sdd/archive/DATA_CONTRACTS/
#         Agent 1 (infra-base) completed post-ship in v1.0.1 session

---

## How to use this file

- Add an entry for each build session or significant implementation milestone.
- Be specific: name the file, the error message, the solution.
- If a decision diverges from the manifest (03_design.manifest.json), record it here
  and flag it for review in the next retrospective.
- Do not clean up entries — this is a chronological log, not a summary.

---

## Entry template

```
## YYYY-MM-DD — <component or task name>

### Implemented
- <artifact created or modified>

### Problems encountered
- <problem description>
  → Solution: <what was done to resolve it>
  → Status: resolved | open | workaround

### Decisions made during build
- <decision not captured in design, with rationale>

### Divergences from manifest
- <what was planned vs what was actually built, and why>

### Open questions
- <anything that needs follow-up or investigation>
```

---

## 2026-06-16 — v1.0.0 — Design complete, build not yet started

### Context
Full SDD design session completed. All 8 ADRs validated by two Staff/Principal-level
external reviewers with distinction. Build agents defined and delegation document ready.

### Design artifacts completed
- CLAUDE.md — project context and domain map
- .claude/01_brainstorm.prompt — multi-source exploration, 8 sections
- .claude/02_define.spec.yaml — v1.0.0, 18 ACs, 20 domains
- .claude/03_design.manifest.json — v1.0.0, 8 ADRs, domain_map
- .claude/04_build.delegation.md — 10 agents, 20-domain scope
- docs/adr/001 through 004 — key architectural decision records
- README.md — portfolio narrative, architecture diagram, tradeoffs table

### Key decisions locked before build
- ADR-02: Bronze uses SMT ExtractNewRecordState (post-SMT flat + __op + __source_ts_ms)
  NOTE: Earlier in the design session we discussed removing SMT for raw envelope.
  After reviewing the sdd-kafka-snowflake proven pattern and unidirectional topology,
  SMT is correct here. The "no SMT" decision was specific to a bidirectional topology
  that was not adopted. See 04_build.delegation.md Agent 3 for clarification.
- ADR-03: 2 parametrized notebooks (pipeline_bronze + pipeline_silver) via DABs
- ADR-04: cluster_by MUST equal merge_key — validated by test_contracts.py
- Confluent Schema Registry (not Apicurio)
- load_to_postgres.py unidirectional (not JDBC Sink)

### Open questions before build
- Exact Databricks Runtime version available in workspace (target: 14.1+)
- Unity Catalog catalog names confirmed: ubereats_dev / ubereats_prod
- Volume path format for checkpoints: /Volumes/{catalog}/{schema}/{volume}/...
- order_items checkpoint Volume needs to be created before first run

---

## 2026-06-16 — Agent 4 — data-contracts (Python files)

### Implemented
- `contracts/__init__.py` — package init
- `contracts/loader.py` — parse + validação semântica (zero deps além de PyYAML)
- `contracts/spark_schema.py` — to_struct_type (deferred PySpark import), to_tblproperties, to_cluster_by_sql, to_create_table_ddl
- `contracts/pydantic_models.py` — 20 modelos Pydantic v2 gerados dinamicamente via create_model()
- `tests/test_contracts.py` — 7 testes parametrizados, 141 casos, 0 failures

### Test results
- `pytest tests/test_contracts.py`: **141 passed in 1.37s**
- `ruff check`: All checks passed

### Decisions made during build
- `pydantic_models.py` usa `Path(__file__).parent` para localizar YAMLs (independente do CWD)
- `_yaml_type_to_annotation` usa `python_type | None` (PEP 604, Python 3.11+)
- Campos com prefix `_` (Bronze metadata: `__op`, `__source_ts_ms`, `_ingested_at`) excluídos dos modelos Pydantic

### Divergences from manifest
- Nenhuma

<!-- Add new build entries below this line -->

---

## 2026-06-16 — Agent 5 — spark-bronze (pipeline_bronze.ipynb)

### Implemented
- `notebooks/pipeline_bronze.ipynb` — parametrized Bronze ingestion notebook

### Notebook structure (7 cells)
1. Markdown: title + ADR references
2. Widget declarations (9 widgets, defaults for payment_events)
3. Widget reads + type conversion (max_offsets → int)
4. sys.path setup (`..`) + `load_contract()` + `to_create_table_ddl()` + `spark.sql(ddl)`
5. Schema Registry fetch via `requests.get` (subject = `{kafka_topic}-value`)
6. `readStream` from Kafka → `substring(value, 6)` → `from_avro(avro_bytes, avro_schema_str)`
7. `foreachBatch(merge_to_bronze)` with `trigger(availableNow=True)` + `awaitTermination()`

### Key design choices
- **Confluent wire format**: `substring(value, 6)` strips 5-byte header (1 magic + 4 schema ID) before `from_avro`
- **Bronze immutability**: MERGE uses `WHEN NOT MATCHED THEN INSERT *` only — no UPDATE clause
- **PK null rejection**: `batch_df.filter(col(merge_key).isNotNull())` before MERGE (Bronze quality rule, on_failure: reject)
- **_ingested_at**: added via `withColumn("_ingested_at", current_timestamp())` in foreachBatch — matches contract schema
- **sys.path**: `sys.path.insert(0, "..")` makes `contracts/` importable from `notebooks/`
- **view name**: `bronze_batch_{table_name}` — scoped per domain to avoid cross-notebook collisions

### ADRs honoured
- ADR-002: SMT used → Bronze receives flat records with `__op` + `__source_ts_ms` (no envelope navigation)
- ADR-003: Single parametrized notebook, 9 widgets → DABs runs it 20× with domain-specific params
- ADR-004: MERGE ON uses `merge_key` from contract → Liquid Clustering accelerates file pruning

### Divergences from delegation doc
- `_ingested_date` and `_source_file` metadata not added: not defined in any contract YAML (contracts are source of truth; schema_evolution.new_fields=allowed if needed later)

### Open questions / next steps (resolved by Agent 6)
- Agent 6: `notebooks/pipeline_silver.ipynb` ✅
- Agent 6 (special): `notebooks/pipeline_users.ipynb` ✅
- DABs `databricks.yml` must pass absolute `contract_path` values (not relative) when deploying to workspace

---

## 2026-06-16 — Agent 6 — spark-silver (pipeline_silver.ipynb + pipeline_users.ipynb)

### Implemented
- `notebooks/pipeline_silver.ipynb` — parametrized Silver notebook (Bronze CDF → quality routing → MERGE)
- `notebooks/pipeline_users.ipynb` — Silver users special case (FULL OUTER JOIN by CPF, full refresh)

### pipeline_silver.ipynb structure (7 cells: 1 markdown + 6 code)
1. Widget declarations (table_name, bronze_table, silver_table, quarantine_table, contract_path, checkpoint_path)
2. Widget reads
3. sys.path + load_contract + `to_create_table_ddl` → Silver + Quarantine CREATE TABLE IF NOT EXISTS
4. Quality helpers: `_rule_fail_expr(rule)` + `apply_quality_rules(df, contract)` → (clean_df, quarantine_df)
5. Bronze CDF readStream: `readChangeFeed=true`, filter `_change_type == "insert"`, drop CDF meta cols
6. `process_silver_batch`: quarantine append + Silver MERGE (UPDATE when `__source_ts_ms` newer, INSERT if new)

### pipeline_users.ipynb structure (7 cells: 1 markdown + 6 code)
1. Widgets (bronze_users_mongo, bronze_users_mssql, silver_users_table)
2. Widget reads
3. `CREATE TABLE IF NOT EXISTS silver.users` — inline DDL (no contract for silver.users)
4. `normalize_cpf()` + `dedup_by_cpf()` (Window row_number by `desc(__source_ts_ms)`) + read both Bronze tables
5. FULL OUTER JOIN on `cpf_key` + `coalesce` field selection (mssql for names, mongo for location)
6. `mode("overwrite")` + `saveAsTable(silver_users_table)` — full refresh

### Key design choices
- **Quality routing**: `on_failure: quarantine` rules → `quarantine_df`; `on_failure: warn` rules → pass through (no action)
- **`_rule_fail_expr`**: not_null → `isNull()`; allowed_values → `isNotNull() & ~isin(values)`; not_future → `isNotNull() & ts > current_timestamp()`
- **CDF columns**: `{_change_type, _commit_version, _commit_timestamp}` dropped before MERGE
- **Silver MERGE condition**: `WHEN MATCHED AND s.__source_ts_ms > t.__source_ts_ms` — idempotent, fresher wins
- **Quarantine schema**: same DDL as Silver (`quarantine/` mirrors Silver — CLAUDE.md)
- **CPF normalization**: `regexp_replace(cpf, r'[.\-]', '')` — handles both `000.000.000-00` and `00000000000`
- **users dedup**: `Window.partitionBy("cpf_key").orderBy(desc("__source_ts_ms"))` + `row_number() == 1`
- **users overwrite**: `mode("overwrite")` with `overwriteSchema=true` — safe for ~700 records full refresh

### ADRs honoured
- ADR-02: Silver reads Bronze (not Kafka directly) — strict medallion lineage
- ADR-04: MERGE ON merge_key aligns with Liquid Clustering from contract `storage.cluster_by`

### Divergences from delegation doc
- Nenhuma

### Open questions / next steps (resolved by Agent 7)
- Agent 7: 6 Gold notebooks ✅
- Agent 8: `databricks.yml` DABs — 20 Bronze + 12 Silver + 1 users + 6 Gold tasks

---

## 2026-06-16 — Agent 7 — spark-gold (6 Gold notebooks)

### Pre-requisite fix
- `notebooks/pipeline_users.ipynb` — added `user_id INT` to DDL and `coalesce(m.user_id, s.user_id)` to select; required for gold_user_behavior join on user_id

### Implemented
| Notebook | Sources | Merge Key | Grain |
|---|---|---|---|
| `notebooks/cross_domain/gold_payments_by_status.ipynb` | silver.payments | status | per status value |
| `notebooks/cross_domain/gold_payment_funnel.ipynb` | silver.payment_events | event_name | per funnel step |
| `notebooks/cross_domain/gold_payment_lifecycle.ipynb` | silver.payment_events | payment_id | per payment |
| `notebooks/cross_domain/gold_driver_performance.ipynb` | silver.driver_shifts × orders × drivers | driver_id | per driver |
| `notebooks/cross_domain/gold_revenue_per_restaurant.ipynb` | silver.order_items × orders × restaurants | restaurant_cnpj | per restaurant |
| `notebooks/cross_domain/gold_user_behavior.ipynb` | silver.search_events × recommendations × users | user_id | per user |

### Common pattern for all 6 Gold notebooks (5 code cells each)
1. Widget: `catalog` (default: "ubereats_dev") — all table FQNs derived from it
2. Params: derive `gold_table`, `silver_*` source paths
3. DDL: `CREATE TABLE IF NOT EXISTS … USING DELTA CLUSTER BY (merge_key) TBLPROPERTIES ('delta.enableChangeDataFeed'='true')`
4. Transform: read Silver → aggregate/join → `withColumn("_computed_at", current_timestamp())`
5. MERGE: `createOrReplaceTempView` → `MERGE INTO gold WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *`

### Key design choices
- **Never Bronze directly**: all Gold notebooks read from Silver tables only
- **ADR-04**: CLUSTER BY = merge key for every Gold table; MERGE ON that same key
- **Gold MERGE**: always `WHEN MATCHED THEN UPDATE SET *` (no timestamp guard; full Silver recompute each run)
- **payment_events.event field**: `coalesce(get_json_object(event, '$.event_name'), event)` — handles both JSON string and plain string event names
- **driver_key type**: `orders.driver_key.cast("integer")` to join with `driver_shifts.driver_id` (INT)
- **CNPJ join**: `orders.restaurant_key = restaurants.cnpj` (CLAUDE.md hub table doc)
- **user_behavior join**: `search_agg FULL OUTER JOIN rec_agg ON user_id` then `LEFT JOIN users ON user_id`
- **recommendation pivot**: conditional `sum(when(event_type == X, 1).otherwise(0))` for view/click/purchase counts

### ADRs honoured
- ADR-02: Gold reads Silver (which already unwrapped Debezium envelope) — no Bronze access
- ADR-04: all Gold CLUSTER BY = merge_key for MERGE file pruning

### Divergences from delegation doc
- Nenhuma

### Open questions / next steps
- All agents complete ✅

---

## 2026-06-16 — Agent 10 — data-platform-engineer (Observability)

### Implemented
| File | Panels / Rules | Status |
|------|----------------|--------|
| `observability/prometheus/prometheus.yml` | 4 scrape jobs | ✅ |
| `observability/prometheus/alert_rules.yml` | 5 alert rules | ✅ |
| `observability/jmx/kafka-jmx-exporter.yml` | 8 JMX rule groups | ✅ |
| `observability/grafana/dashboards/kafka.json` | 7 panels | ✅ |
| `observability/grafana/dashboards/kafka_connect.json` | 6 panels | ✅ |

### Prometheus scrape jobs (4)
| Job | Target | Source |
|---|---|---|
| `kafka-jmx` | `kafka:9101` | jmx_prometheus_javaagent inside Kafka JVM |
| `kafka-consumer-lag` | `kafka-exporter:9308` | danielqsj/kafka_exporter (Go binary) |
| `kafka-connect-jmx` | `kafka-connect:9404` | jmx_prometheus_javaagent inside Connect JVM |
| `prometheus` | `localhost:9090` | self-scrape |

### Alert rules (5)
| Alert | Condition | Severity |
|---|---|---|
| `KafkaConsumerLagHigh` | `lag > 1000` per topic, 5 min | warning |
| `KafkaConsumerLagCritical` | `sum lag > 10000` per topic, 10 min | critical |
| `ConnectorTaskFailed` | `running_ratio < 1` for 2 min | critical |
| `BrokerDown` | `kafka_brokers < 1` for 1 min | critical |
| `UnderReplicatedPartitions` | `underreplicatedpartitions > 0` for 2 min | warning |

### Grafana dashboards
- `kafka.json`: broker count, total consumer lag, under-replicated partitions, active controller (stat row) + consumer lag by topic (20 topics timeseries) + msg/s by topic + bytes in/out
- `kafka_connect.json`: running tasks, failed tasks, min running ratio (stat row) + task running ratio over time + error rate + connector status table

### Key design decisions
- **Scope boundary honoured**: Prometheus scrapes Kafka + Debezium only; Databricks monitoring uses DABs Notifications + Unity Catalog System Tables (per 04_build.delegation.md)
- **kafka_exporter (port 9308)**: separate Go binary for consumer lag — more reliable than JMX for consumer group metrics
- **JMX exporter on port 9404 for Kafka Connect**: standard port for connect worker started with `-javaagent:jmx_prometheus_javaagent.jar=9404:kafka-jmx-exporter.yml`
- **`topic!~"__.*"`**: all consumer lag queries exclude internal Kafka topics
- **`or vector(0)` on Failed Tasks panel**: prevents "no data" gaps when all connectors are healthy
- **Grafana schemaVersion 38**: Grafana 9.x/10.x; uses `timeseries` (not deprecated `graph`)
- **`__inputs` + `DS_PROMETHEUS`**: portable import pattern — works regardless of datasource name

### Divergences from delegation doc
- Grafana provisioning YAML not included (not in scope; wired up when docker-compose is built by Agent 1)
- Alertmanager config not included — marked optional in delegation doc

### Validation
```
kafka.json: 7 panels, uid=kafka-overview
kafka_connect.json: 6 panels, uid=kafka-connect-debezium
pytest: 141 passed — ruff: All checks passed
```

---

## 2026-06-16 — Agent 9 — ci-cd-specialist (CI/CD)

### Implemented / Modified
- `.github/workflows/ci.yml` — added `env-guard` job, `needs` chain on `bundle-validate`, `concurrency`, pip caching
- `.github/workflows/deploy.yml` — replaced `push: branches: [main]` trigger with `workflow_run` gate; added `concurrency` + `environment: production`
- `Makefile` — added `bundle-validate` target + updated `.PHONY` list
- `pyproject.toml` — moved `select`/`isort` to `[tool.ruff.lint]`/`[tool.ruff.lint.isort]` (ruff >= 0.2.0 deprecation); added `exclude` list (`.claude`, `.venv`, `dist`, `notebooks`)
- `.gitignore` — added `.ruff_cache/`, `.coverage`, `htmlcov/`, `.ipynb_checkpoints/`
- `tests/load_to_postgres.py` — auto-fixed: removed unused `datetime`/`timezone` imports; `Optional[str]` → `str | None` (UP045, F401)

### Problems encountered
- `ruff check .` flagged F821 (`dbutils`, `spark` undefined) in all notebooks — Databricks-specific globals not visible outside runtime
  → Solution: added `notebooks/` to `[tool.ruff] exclude`; linting notebooks locally is not meaningful

- ruff deprecated top-level `select` in `[tool.ruff]` → warning treated as lint failure in strict CI
  → Solution: moved to `[tool.ruff.lint]` and `[tool.ruff.lint.isort]`

- `tests/load_to_postgres.py` had 7 auto-fixable violations (F401 unused imports, UP045 Optional syntax)
  → Solution: `ruff check . --fix`

### Key design choices
- **`workflow_run` trigger on deploy.yml**: ensures `databricks bundle deploy --target prod` only fires when CI passes on `main`; safer than relying on branch protection alone
- **`cancel-in-progress: false` on deploy**: never interrupt an in-flight prod deploy (vs CI where stale runs can be cancelled freely)
- **`environment: production`**: enables GitHub Environments protection rules (approval gates, wait timers)
- **`needs: [env-guard, lint, test-contracts]` on `bundle-validate`**: fail fast — don't call Databricks API if Python tests are broken
- **`cache: pip` on setup-python**: speeds up CI by caching PyPI downloads per requirements hash

### Divergences from delegation doc
- `.gitignore` already existed (created by Agent 1); only extended, not replaced
- `pyproject.toml` already existed; only the ruff sections were restructured

### Validation
```
ruff check .: All checks passed (0 errors, 0 warnings)
pytest tests/test_contracts.py: 141 passed in 2.06s
```

### Open questions / next steps
- Agent 10: Observability (Prometheus/Grafana configs)
- `DATABRICKS_HOST` + `DATABRICKS_TOKEN` secrets must be set in GitHub repo settings before CI `bundle-validate` step can run
- GitHub Environment `production` must be created in repo settings before deploy gate activates

---

## 2026-06-16 — Agent 8 — data-platform-engineer (databricks.yml)

### Implemented
- `databricks.yml` — Databricks Asset Bundle, 37 tasks across dev + prod targets

### Task breakdown (37 total)
| Layer | Count | Notebook | Notes |
|---|---|---|---|
| Bronze | 20 | pipeline_bronze.ipynb | All 20 domains; order_items max_offsets=5000 |
| Silver | 10 | pipeline_silver.ipynb | 10 Silver domains with contracts |
| Users | 1 | pipeline_users.ipynb | Depends on bronze_users_mongo + bronze_users_mssql |
| Gold | 6 | cross_domain/*.ipynb | Depends on Silver sources they read |

### Key design choices
- **`${var.x}` syntax**: DABs-native variable substitution in all base_parameters (not `{{var.x}}` legacy form)
- **`workspace_root` variable**: resolves contract YAML paths at runtime; default `christiandr@gmail.com` path; override per-workspace
- **`checkpoint_base` per target**: `/Volumes/ubereats_dev/checkpoints` (dev) vs `/Volumes/ubereats_prod/checkpoints` (prod)
- **`data_security_mode: SINGLE_USER`**: required for Unity Catalog access from job clusters
- **`no_alert_for_skipped_runs: true`**: prevents false-alarm emails when tasks are skipped by the scheduler
- **20 Bronze tasks**: all run in parallel (no depends_on); use `availableNow=True` trigger → micro-batch, exits on completion
- **Gold fan-in dependencies**: `gold_driver_performance` depends on 3 Silver tasks; `gold_revenue_per_restaurant` depends on 3 Silver tasks
- **`silver_users` fan-in**: depends on `bronze_users_mongo` AND `bronze_users_mssql` (2-node fan-in)
- **DBR 15.4 LTS**: `15.4.x-scala2.12`; node_type_id comment covers Azure/AWS/GCP variants

### Divergences from delegation doc
- **37 tasks, not 39**: `payment_current_state` excluded.
  - CLAUDE.md Silver domains list includes `payment_current_state` (12th Silver domain)
  - No `contracts/payment_current_state.yml` exists → `load_contract()` would fail at runtime
  - No dedicated Bronze table for it (it derives from `silver.payment_events`, not a raw Bronze source)
  - None of the 6 Gold notebooks consume `payment_current_state`
  - **Recommendation**: implement as a custom notebook (read from silver.payment_events, pivot to latest state per payment_id) in a future iteration; add to Agent 9/10 scope

### ADRs honoured
- ADR-03: DABs variables parametrize all notebooks — 2 notebooks run N times via DABs (not N static notebooks)
- ADR-04: Liquid Clustering column = merge_key; all MERGE ON clauses target same key → file pruning works

### Open questions / next steps
- Agent 9: CI/CD (`.github/workflows/`)
- Agent 10: Observability (Prometheus/Grafana)
- `workspace_root` variable must be updated to actual workspace URL before `databricks bundle deploy`

---

## 2026-06-16 — Agent 1 — infra-base (docker-compose + JMX)

### Implemented
- `docker-compose.yml` — 10-service stack (postgres, kafka, schema-registry, kafka-connect,
  kafka-ui, kafka-exporter, jmx-kafka, jmx-kafka-connect, prometheus, grafana)
- `docker-compose.override.yml` — local dev host port bindings (5432, 9092, 8081, 8083, 8080, 9090, 3001)
- `Dockerfile.connect` — `confluentinc/cp-kafka-connect:7.7.1` + Debezium PostgreSQL connector 2.7.1
- `observability/jmx/broker-config.yml` — bitnami/jmx-exporter config for Kafka broker (hostPort: kafka:9999)
- `observability/jmx/connect-config.yml` — bitnami/jmx-exporter config for Kafka Connect (hostPort: kafka-connect:9998)
- `observability/grafana/provisioning/datasources/prometheus.yml` — auto-provision Prometheus datasource (uid: prometheus)
- `observability/grafana/provisioning/dashboards/dashboard.yml` — auto-provision dashboards from /etc/grafana/dashboards
- `sql/init.sql` — placeholder (pgcrypto extension + debezium_publication); Agent 2 expands with full schema
- Updated `observability/prometheus/prometheus.yml` — fixed JMX targets from `kafka:9101` and `kafka-connect:9404`
  to `jmx-kafka:9101` and `jmx-kafka-connect:9404` to match bitnami sidecar service names

### Problems encountered
- `prometheus.yml` was written (Agent 10) with `kafka:9101` and `kafka-connect:9404` assuming javaagent
  pattern (JMX exporter running inside the JVM). Agent 1 uses bitnami/jmx-exporter as separate sidecar
  services named `jmx-kafka` and `jmx-kafka-connect`.
  → Solution: updated targets in prometheus.yml to `jmx-kafka:9101` and `jmx-kafka-connect:9404`
  → Status: resolved

- `sql/init.sql` did not exist; bind mount in docker-compose.yml would silently create a directory.
  → Solution: created placeholder `sql/init.sql` with pgcrypto extension and Debezium publication
  → Status: resolved (Agent 2 will expand with full schema)

### Decisions made during build
- **Sidecar pattern over javaagent** — bitnami/jmx-exporter runs as separate containers (`jmx-kafka`,
  `jmx-kafka-connect`) rather than injecting a javaagent JAR into the Kafka/Connect images. This avoids
  a custom Dockerfile.kafka and keeps the broker image unmodified.
- **JMX ports**: kafka → 9999 (KAFKA_JMX_PORT), kafka-connect → 9998 (KAFKA_JMX_PORT). Both are
  internal-only; not exposed to host.
- **KRaft listeners**: PLAINTEXT on 9092 (inter-container), PLAINTEXT_HOST on 9094 (exposed as host:9092
  via override). Avoids listener naming collision with single port.
- **Grafana provisioning**: datasource uid = `prometheus` (matches `${DS_PROMETHEUS}` template variable
  in kafka.json and kafka_connect.json dashboards — Grafana resolves by name match, not uid, when the
  variable type is datasource).
- **`docker-compose.override.yml`** — host port bindings extracted from base compose to keep base config
  portable (no ports exposed in CI). Port 3001:3000 for Grafana matches CLAUDE.md.

### Open questions / next steps
- Agent 2 (infra-postgres): expand `sql/init.sql` with full schema for all 20 domains + replication config
- Agent 3 (infra-kafka): create Debezium connector configs in `connectors/` + Makefile `register-connectors` target
- TD-06 is now resolved — JMX ports 9101 and 9404 are wired in docker-compose.yml via sidecar services
- Unity Catalog Volumes must exist before first run: `CREATE VOLUME IF NOT EXISTS ubereats_dev.checkpoints`

---

## 2026-06-17 — TD-05 resolved — `payment_current_state` removed as YAGNI

### Context
`/brainstorm payment_current_state como Silver derivado` explored implementing the missing
12th Silver domain (TD-05). Investigation showed `gold_payment_lifecycle.ipynb` already
computes `first_event_name`/`last_event_name`/`lifecycle_duration_sec` per `payment_id` from
`silver.payment_events`, and a repo-wide grep confirmed zero notebooks, SQL, or Python code
ever referenced `payment_current_state` outside design docs. `databricks.yml` already excluded
it (comment at line ~383) and `.claude/03_design.md` `domain_map` never included it either —
only the narrative counts (12 Silver domains) and the `02_define.md`/`CLAUDE.md` lists were stale.

### Decision
Remove `payment_current_state` entirely rather than implement it. No code changes — the
pipeline was already running correctly at 11 Silver domains / 37 DABs tasks; only the
documentation had drifted.

### Implemented (docs only)
- `CLAUDE.md` — Silver domain count 12 → 11, removed from domain list, fixed Unity Catalog
  structure table (silver/quarantine: 12 → 11 tables) and ADR-03 narrative (12x → 11x)
- `.claude/02_define.md` — `silver_domains: 12 → 11`, removed from `silver_list`, fixed AC-10
  (12x → 11x silver runs, 32 → 31 tasks)
- `.claude/03_design.md` — `domain_map` was already correct (never listed it); fixed stale
  counters (`silver_tables`, `quarantine_tables`: 12 → 11; `dabs_silver_runs`: 12 → 11) and
  ADR-03 decision/interview_phrase text (12 → 11)
- `.claude/06_retrospective.md` — TD-05 marked resolved as YAGNI; metrics table corrected
  (DABs tasks target 39 → 37, now ✅; Silver tasks target 12 → 11, gap now attributed only to
  TD-02 which remains open); removed TD-05 item from the v1.0.1 improvements checklist

### Verification
- `grep -rn payment_current_state` across `.ipynb`/`.py`/`.sql` → 0 matches (confirmed nothing
  depends on it)
- `databricks.yml`: counted distinct `task_key:` definitions → 20 bronze + 11 silver + 6 gold = 37
  (matches the corrected target, no DABs change needed)

### Status: resolved

---

## 2026-06-17 — v1.0.1 infra readiness — TD-04/TD-06 resolved, ADR-02 corrected

### Context
`/brainstorm` → `/define` → `/design` → `/build` cycle for the 2 real infra gaps blocking
the first `make up` → `databricks bundle deploy`: PostgreSQL never had the 20-table schema
or a registered Debezium connector, and nothing provisioned the Unity Catalog. Surfaced along
the way: ADR-02 as written ("no SMT") contradicted the already-implemented code (SMT is used).

### Implemented
- `sql/init.sql` — expanded from 14 lines (extension + slot + publication only) to the full
  20 `CREATE TABLE` schema, adapted from `sdd-kafka-snowflake/scripts/init.sql`, verified
  field-by-field against `contracts/*.yml` (types, special cases: `order_status.status_id`
  INTEGER PK, `receipts`/`search_events`/`inventory` without `dt_current_timestamp`)
- `connectors/debezium.json` — new. Keeps `transforms.unwrap.type: ExtractNewRecordState`
  (matches the real Bronze/Silver code); `publication.name: debezium_publication` to match
  `sql/init.sql` (the reference project used `dbz_publication` — renamed to stay consistent)
- `scripts/register_connectors.sh` — new, adapted from the reference script, reduced from
  3 connectors (Debezium + 2 Snowflake sinks) to 1 (`debezium-postgres-cdc` only — ADR-01:
  Databricks reads Kafka directly). Inlines the Schema Registry BACKWARD compatibility PUT
  instead of a separate `set_compatibility.sh` (YAGNI)
- `scripts/preflight_unity_catalog.sh` — new. `--target dev|prod`; idempotent `get`/`read`
  before `create` for catalog, 4 data schemas, and a `checkpoints` schema with `bronze`/
  `silver` Volumes (Structured Streaming checkpoint locations — Gold is batch MERGE, no
  checkpoint needed). Fails fast with a clear message if the Databricks CLI isn't authenticated
- `CLAUDE.md`, `.claude/03_design.md`, `.claude/02_define.md` — ADR-02 text flipped from
  "no SMT, raw envelope, unwrap in Silver" to "uses SMT ExtractNewRecordState, Bronze is
  flat" — matches `pipeline_bronze.ipynb`'s own comment ("Post-SMT Avro schema... ADR-002")
  and the fact that `pipeline_silver.ipynb` has no envelope-unwrap logic at all
- `.claude/06_retrospective.md` — TD-04 (Volumes) resolved citing the new pre-flight script;
  TD-06 (JMX) resolved — turned out to be a documentation false positive, `docker-compose.yml`
  already had the `jmx-kafka`/`jmx-kafka-connect` sidecars correctly wired

### Problems encountered
- Smoke-testing `preflight_unity_catalog.sh --target dev` ran `databricks catalogs create
  ubereats_dev` against the real authenticated workspace (`dbc-f3701868-1581.cloud.databricks.com`)
  instead of an isolated test environment — the CLI had a live profile configured locally.
  → The API itself rejected the call ("Metastore storage root URL does not exist"), so no
    catalog was actually created; user confirmed `ubereats_dev` does not exist post-incident.
  → Status: resolved, no cleanup needed. No further live `databricks` calls were made for the
    rest of this build — only `bash -n` syntax checks.
  → Rule for future sessions: never invoke a CLI tool that can mutate a cloud resource (`databricks`,
    `aws`, `gcloud`, etc.) without first checking whether it's pointed at a real/authenticated
    environment (e.g. `databricks auth describe`) — local dev tooling assumptions don't hold when
    the user's machine already has live credentials configured.

### Verification
- `sql/init.sql` validated against a real (throwaway) PostgreSQL container: `docker compose up
  postgres` → 20/20 tables created, `debezium_publication` (`puballtables=true`) and
  `debezium_slot` (`pgoutput`) both present
- `make produce-initial` equivalent (`load_to_postgres.py --batch initial`) against that same
  container: 127,892 records inserted across the 20 tables, 0 errors
- `connectors/debezium.json` — valid JSON, `table.include.list` has exactly 20 entries
- `scripts/register_connectors.sh`, `scripts/preflight_unity_catalog.sh` — `bash -n` clean;
  `preflight_unity_catalog.sh` argument validation (`--target staging` → clean exit 1) verified
- Throwaway PostgreSQL container and volume removed after verification (`docker compose down -v`)

### Status: resolved

---

## 2026-06-17 — v1.0.1 full stack verification — 3 pre-existing bugs found, 2 fixed

### Context
Resumed the v1.0.1 build to write the BUILD_REPORT. Decided to verify `register_connectors.sh`
against a real local Kafka Connect (not just `bash -n`), which surfaced that `make up` had
never actually completed successfully on this repo, ever — three independent, pre-existing
bugs, each one blocking the next layer. All three were outside the DESIGN file manifest;
each was confirmed and explicitly approved before touching it.

### Problems encountered

- **`Dockerfile.connect` never built** — `confluent-hub install
  debezium/debezium-connector-postgresql:2.7.1.Final` fails: that version was never published
  to Confluent Hub (jumps `2.5.4` → `3.0.8`, confirmed via the Hub API).
  → Considered switching to `debezium/connect:2.7.1.Final` (official image, bundles the
    connector) — rejected after checking the image directly: it has no Confluent Avro
    Converter and no `confluent-hub` CLI to install one, and uses a different env var
    convention (`BOOTSTRAP_SERVERS`, `JMXPORT`/`JMXHOST` instead of `CONNECT_*`/`KAFKA_JMX_*`),
    which would have required rewriting `docker-compose.yml` and risked silently breaking the
    JMX wiring just closed as TD-06.
  → Fix: kept `confluentinc/cp-kafka-connect:7.7.1` (already bundles the Avro converter,
    confirmed by inspecting the image directly) and pinned the connector to `2.5.4`, the
    latest 2.x actually on Confluent Hub. One-line change, zero new risk.
  → Status: resolved (TD-08)

- **`kafka` service fails to start on every fresh volume** — `KAFKA_LOG_DIRS: /tmp/kraft-combined-logs`
  with `kafka-data:/tmp/kraft-combined-logs`. That path doesn't pre-exist in
  `confluentinc/cp-kafka:7.7.1` (only `/tmp` does, mode 1777), so Docker creates the named
  volume `root:root 0755`. The container runs as non-root `appuser` (uid 1000) → can't write
  → `Error while writing meta.properties file`. Confirmed via `docker run ... stat` that
  `/var/lib/kafka/data` is the image's actual pre-owned (`appuser:root 0775`) data directory —
  also the path the image's own preflight check already logs (`Check if /var/lib/kafka/data
  is writable`), meaning this project's compose file used a non-standard path from the start.
  → Fix: `KAFKA_LOG_DIRS` and the volume mount both changed to `/var/lib/kafka/data`.
  → Status: resolved (TD-09)

- **`tests/load_to_postgres.py` reports success for records it never wrote** — after fixing
  the two bugs above and running the full stack end-to-end, the Debezium snapshot completed
  but only produced 16 of 20 `pg.public.*` topics. The 4 missing (`restaurants`, `drivers`,
  `ratings`, `inventory`) turned out to be genuinely empty in Postgres (`SELECT count(*)` = 0),
  despite `load_to_postgres.py --batch initial` having printed non-zero counts and "0 errors"
  for all 4. Root cause of the line: `stats["inserted"] += len(transformed)` (~line 429)
  counts records as inserted unconditionally, never checking what `insert_batch()` actually
  wrote — but why exactly these 4 (and not the other 16) end up empty was not root-caused.
  → Status: not fixed — out of scope for v1.0.1 (this feature owns the CDC/Unity Catalog
    layer, not `load_to_postgres.py`'s insert correctness). Logged as TD-10 for v1.0.2.

- **`register_connectors.sh` idempotency was broken by our own `set -e` + `curl -f` combo** —
  `curl -sf` returns exit 22 on HTTP ≥400 (including the expected 409), and under
  `set -euo pipefail` that aborts the script before the `case` statement can treat 409 as
  success. Found by literally re-running the script against a live connector.
  → Fix: dropped `-f` from that one `curl` call in `register_connector()` — the script already
    inspects `$HTTP` explicitly, it doesn't need curl to also fail on it.
  → Status: resolved (own bug, fixed directly — `connectors/debezium.json` and
    `scripts/register_connectors.sh` are both in this feature's manifest)

- **No Kafka topics were created at all on first registration** — `KAFKA_AUTO_CREATE_TOPICS_ENABLE:
  "false"` on the broker, and the connector's producer just retried `UNKNOWN_TOPIC_OR_PARTITION`
  forever without ever erroring or creating the topic.
  → Fix: added `topic.creation.enable`, `topic.creation.default.replication.factor`,
    `topic.creation.default.partitions` to `connectors/debezium.json` (Kafka Connect's
    per-connector topic-creation feature) instead of flipping the broker's global
    auto-create setting — narrower blast radius, and the fix belongs in a file already
    in this feature's manifest.
  → Status: resolved (own bug, fixed directly)

### Verification (full local stack, real containers, then torn down)
- `docker compose build kafka-connect` — succeeds with the `2.5.4` pin
- `kafka` boots healthy with the `/var/lib/kafka/data` fix; `schema-registry` and
  `kafka-connect` come up healthy after
- `register_connectors.sh --env verify`: connector `RUNNING`, `transforms.unwrap.type` =
  `io.debezium.transforms.ExtractNewRecordState`, `publication.name` = `debezium_publication`
  (AT-003, AT-004) — matches DEFINE acceptance tests
- Schema Registry `GET /config` → `{"compatibilityLevel":"BACKWARD"}` (AT-006)
- Re-running `register_connectors.sh` twice in a row both exit 0 (AT-005, after the curl -f fix)
- Full Debezium snapshot: "Snapshot ended with SnapshotResult [status=COMPLETED...]" across
  all 20 tables; 16 `pg.public.*` topics confirmed populated (the other 4 are the TD-10 issue,
  upstream of Debezium, not a CDC config problem)
- All containers and volumes from this verification removed (`docker compose down -v`);
  `.env.verify` (throwaway credentials file, never committed) deleted

### Status: resolved (TD-08, TD-09 resolved; TD-10 logged for v1.0.2)

---

## 2026-06-17 — `make up` failed on real machine — `bitnami/jmx-exporter:1.0.1` retired

### Context
User ran `make up` after the v1.0.1 ship and hit `manifest for bitnami/jmx-exporter:1.0.1
not found`. Bitnami retired versioned tags from the free `bitnami/*` Docker Hub repos in
2025 — only `latest` remains there now. The previously-published versioned images were
moved to a frozen mirror, `bitnamilegacy/*`, which still has `1.0.1` as an exact tag
(confirmed via Docker Hub API and `docker pull`).

### Fix
`docker-compose.yml` — both `jmx-kafka` and `jmx-kafka-connect` services:
`bitnami/jmx-exporter:1.0.1` → `bitnamilegacy/jmx-exporter:1.0.1`. Same exact version, no
behavior change, just a different (frozen, no-longer-updated) registry namespace. A comment
was added explaining why, so a future contributor doesn't "fix" it back.

### Verification
- `docker pull bitnamilegacy/jmx-exporter:1.0.1` — succeeds
- `make up` — all 10 services come up, all healthchecked services report `healthy`,
  including `kafka`, `kafka-connect`, `schema-registry`, `postgres` (the JMX sidecars and
  `grafana`/`prometheus`/`kafka-ui`/`kafka-exporter` have no healthcheck defined, just `Up`)

### Status: resolved

---

## 2026-06-17 — Free Edition Bronze — source_mode=volume, databricks.yml anchors, 2 more bugs found

### Context
Built the FREE_EDITION_BRONZE feature end to end (brainstorm → define → design → build):
Databricks Free Edition's serverless compute can't reach the local Kafka broker (fixed,
non-customizable outbound allowlist), so Bronze needed a second ingestion path. Full
summary in `.claude/sdd/reports/BUILD_REPORT_FREE_EDITION_BRONZE.md`.

### What was built
- `scripts/export_kafka_to_volume.py` — consumes the 20 `pg.public.*` topics end to end,
  casts to contract types, writes Parquet (one dir per domain)
- `pipeline_bronze.ipynb` — `source_mode` widget (`kafka` default | `volume`); Kafka cells
  consolidated into one `if` block so the branch is valid per-cell Python; new `elif`
  branch does a one-shot batch read + `merge_to_bronze()`, no checkpoint needed
- `databricks.yml` — full restructure: 37 tasks defined once as YAML anchors
  (`task_definitions`), two task arrays (`classic_tasks`/`serverless_tasks`), `dev`/`prod`/
  `free_edition` each own their `resources.jobs.ubereats_pipeline` (DABs can't exclude a
  root-level resource from one target — github.com/databricks/cli#2872)
- `scripts/preflight_unity_catalog.sh` — new `landing` schema + `kafka_export` Volume,
  `ensure_volume()` generalized to take a schema argument
- `pyproject.toml` — added `pyarrow`

### Problems found while live-testing the export script (real Kafka, real data)
- **`KAFKA_ADVERTISED_LISTENERS` pointed at an unmapped port** — advertised
  `localhost:9094`, but `docker-compose.override.yml` only maps `9092:9094`. Any Kafka
  client outside Docker (the new export script — the first one ever in this repo) got
  `Connection refused` reconnecting to the advertised address. Fixed: advertise
  `localhost:9092` instead. Container recreated, re-verified healthy.
- **6 contracts had the wrong primitive type** versus the actual PostgreSQL column —
  `drivers.driver_id`/`routes.driver_id` declared `integer` but are `VARCHAR(20)`;
  `gps_events.speed_kph`/`ratings.rating`/`routes.estimated_duration_min` declared
  `integer` but are `NUMERIC` with decimals; `driver_shifts.shift_duration_min` declared
  `double` but is plain `INTEGER`. Never caught before because nothing had cast
  Avro-decoded values against the contract's declared type until this script. Fixed all 6;
  re-ran the 141 contract tests — no regressions; confirmed `ratings`'s
  `allowed_values: [1,2,3,4,5]` rule still matches (`4.0 in [1,2,3,4,5]` is `True`, and the
  fixture data only has integer ratings anyway).
- **Debezium emits `TIMESTAMPTZ` as ISO-8601 strings, not epoch-millis longs** — DESIGN's
  Pattern 1 only handled the `int` case (`time.precision.mode=connect`'s plain-`TIMESTAMP`
  behavior); every domain failed on every timestamp field until `_cast_record()` also
  handled the `str` → `datetime.fromisoformat()` case.
- **`__deleted` (added by the Debezium SMT) isn't in any contract** — `_cast_record()` was
  copying it through unfiltered, which would have broken `pa.Table.from_pylist` once the
  timestamp fix was in. Fixed: only keep keys present in the contract's schema.

### Verification
- `ruff check .`, `bash -n` on both scripts, YAML/JSON parsing on `databricks.yml` and the
  notebook, `ast.parse()` on every notebook code cell — all clean
- `PYTHONPATH=. pytest tests/test_contracts.py` — 141/141 passing (after the contract fixes)
- Live run of `export_kafka_to_volume.py` against the user's real stack (after
  `make register-connectors` + `produce-initial`): 20/20 domains processed, 127,892 records
  across the 16 populated domains (4 are the pre-existing TD-10 gap from v1.0.1 — empty
  Parquet written gracefully, no crash); Parquet schema spot-checked against
  `payment_events`/`driver_shifts` — exact match with the contract, `__op`/`__source_ts_ms`
  present and correctly typed
- `databricks.yml`: confirmed via `yaml.safe_load` that `dev`/`prod` each get 37 tasks with
  `job_cluster_key`, `free_edition` gets the same 37 with no `job_cluster_key` and no
  `job_clusters` block at all
- **Not done**: anything requiring a live, authenticated call against the real Databricks
  workspace (`databricks bundle validate -t free_edition`, an actual Bronze
  `source_mode=volume` run) — same standing restriction as v1.0.1; a real
  `.databrickscfg` profile exists locally but was not used

### Status: resolved (own bugs fixed in-manifest; 2 pre-existing bugs found and fixed
with approval — Kafka listener, 6 contract types; TD-10 from v1.0.1 remains open,
unrelated to this feature)

---

## 2026-06-18 — databricks.yml: fix `unknown field: task_definitions` validator warning

### Implemented
- `databricks.yml` — moved the 37-task/job-cluster/email-notification YAML anchor
  scaffold from a root-level `task_definitions:` key into
  `variables._pipeline_anchors.default`.

### Problems encountered
- `databricks bundle validate` warned `unknown field: task_definitions` because
  the DABs JSON schema's root struct has a fixed field list (`bundle`, `workspace`,
  `variables`, `targets`, `resources`, ...) and doesn't recognize arbitrary
  root-level keys used purely to host YAML anchors.
  → Tried the user's suggested `x-` prefix convention (à la Docker Compose/OpenAPI
    extensions) first — confirmed empirically that the Databricks CLI does **not**
    special-case `x-`-prefixed fields; `x-task_definitions` still warned.
  → Diagnostic finding: the CLI only ever reports the *first* unrecognized
    root-level key in document order (`databricks.yml:21:1` every time, regardless
    of which key sat there) — `classic_job_clusters`, `classic_tasks`,
    `serverless_tasks`, `email_notifications` are equally unrecognized root keys
    but were never flagged, only because `task_definitions` came first. Renaming
    that one key would have just moved the warning to whichever key became first.
  → Solution: `databricks bundle schema` shows `variables.<name>.default` resolves
    to `$defs/interface` (an empty/unconstrained schema), making it the only
    schema-sanctioned home for a free-form anchor scaffold. Moved the entire
    `task_definitions` map plus `classic_job_clusters`, `classic_tasks`,
    `serverless_tasks`, and `email_notifications` anchors under one new variable,
    `_pipeline_anchors` (never referenced as `${var._pipeline_anchors}` — only its
    nested YAML anchors are consumed, via existing `<<: *anchor`/`*anchor` merge
    keys and aliases elsewhere in the file).
  → Status: resolved

### Verification
- `databricks bundle validate --target dev|prod|free_edition` — all three:
  `Validation OK!`, zero warnings (previously 1 warning each).
- `databricks bundle validate -t <target> -o json`, parsed: `dev`/`prod` resolve to
  37 tasks each with `job_cluster_key` set and `job_clusters: [ubereats_cluster]`
  present; `free_edition` resolves to the same 37 tasks with no `job_cluster_key`
  on any task and `job_clusters: None` — identical to pre-change behavior, confirming
  the anchor relocation didn't change the merged job definitions, only where they're
  declared in the source YAML.

### Status: resolved

---

## 2026-06-18 — databricks.yml — workspace_root pointed at a Repos path no target uses

### Problems encountered
- `var.workspace_root` (used to build every `contract_path` base parameter) defaulted
  to `/Workspace/Repos/christiandr@gmail.com/sdd-kafka-databricks`. The global
  `workspace.root_path` had already been migrated to
  `/Workspace/Users/christiandr@gmail.com/.bundle/${bundle.name}/${bundle.target}`
  for all three targets, so after `bundle deploy` the contracts actually land under
  `.../.bundle/sdd-kafka-databricks/<target>/files/contracts/`. The stale default
  surfaced first on `free_edition` (the target actually being run), but `dev`/`prod`
  carried the identical latent bug.
  → Solution: replaced the `workspace_root` variable's `default` with the predefined
    bundle variable `${workspace.file_path}`, which DABs resolves per-target to
    `<root_path>/files` — the exact directory `contracts/` is synced into. Fixes all
    three targets with a single line, no per-target override needed.
  → Status: resolved

### Verification
- `databricks bundle validate -t dev|prod|free_edition` — all three: `Validation OK!`
- `databricks bundle validate -t <target> -o json`, parsed `contract_path` on
  resolved tasks: each target now resolves to its own correct
  `.../.bundle/sdd-kafka-databricks/<target>/files/contracts/{table}.yml`.

### Status: resolved

---

## 2026-06-18 — databricks.yml — free_edition fanned out too many concurrent Spark sessions

### Problems encountered
- `serverless_tasks` (used only by `free_edition`) let all 20 Bronze tasks start
  with zero `depends_on`, all 11 Silver tasks start as soon as their one Bronze
  dependency finished, and all 6 Gold tasks start as soon as their Silver
  dependencies finished — up to 20-way fan-out at job start. Free Edition's
  serverless compute can't sustain that many concurrent Spark sessions.
  → Solution: added `max_concurrent_runs: 1` to the `free_edition`
    `ubereats_pipeline` job (no overlapping job runs), and rewrote `serverless_tasks`
    to override `depends_on` per task, collapsing the natural 3-tier fan-out into
    sequential batches of ≤4: Bronze 5×4, Silver 4+4+3, Gold 3+3. Each layer's first
    batch is gated on the previous layer's last batch finishing entirely, so
    correctness (every task's real upstream dependency) is preserved — at most 4
    tasks ever run concurrently. `classic_tasks` (`dev`/`prod`) is unchanged.
  → Status: resolved

### Verification
- `databricks bundle validate -t free_edition` — `Validation OK!`
- `databricks bundle validate -t free_edition -o json`, parsed: 37 tasks present,
  `max_concurrent_runs: 1`, no task has more than 4 entries in `depends_on`, and
  every task's resolved `depends_on` set is a superset of its original semantic
  dependency (e.g. `silver_users` still only becomes runnable after both
  `bronze_users_mongo` and `bronze_users_mssql`, now via the Bronze batch gate).
- `databricks bundle validate -t dev -o json` — still 37 tasks, `job_clusters`
  present, confirming `classic_tasks`/dev/prod were not touched.

### Status: resolved

---

## 2026-06-18 — pipeline_users.ipynb — missing-cpf users silently dropped, not quarantined

### Problems encountered
- `dedup_by_cpf` (see prior entry on EXPLODING_JOIN, which was already fixed)
  partitions by `cpf_key`, and Spark's window partitioning groups all `NULL`s into
  one partition — so rows with no CPF on either `users_mongo` or `users_mssql`
  collapsed to a single arbitrary survivor per side instead of being preserved.
  Those rows then never matched anything in the `full_outer` join's `cpf_key`
  predicate (`NULL == NULL` is not true in Spark SQL semantics) and were silently
  lost — no error, no record in any table.
  → Solution: added a `quarantine_table` widget (default
    `ubereats_dev.quarantine.users`, no contract exists for `users` so the DDL is
    hand-rolled in cell `cell-create-table`, mirroring `silver.users`'s shape plus
    `_quarantine_reason`/`_quarantine_ts`). In `cell-read-bronze`, split each raw
    bronze frame on `cpf_key IS NULL` *before* `dedup_by_cpf` runs: the null-cpf
    side is projected into the quarantine shape via `to_quarantine_shape()` (per
    source, since `users_mongo` and `users_mssql` have disjoint column sets),
    unioned, tagged `_quarantine_reason="missing_cpf"`, and appended to
    `quarantine_table`. Only the cpf-present side continues into `dedup_by_cpf` →
    the existing `full_outer` join → Silver write. Also added
    `quarantine_table: ${var.catalog}.quarantine.users` to the `silver_users` task's
    `base_parameters` in `databricks.yml` — without it, the job would always
    default to `ubereats_dev.quarantine.users` regardless of target/catalog.
  → Status: resolved

### Verification
- `python3 -c "import json; json.load(open('notebooks/pipeline_users.ipynb'))"` —
  notebook is valid JSON, 7 cells (unchanged count, cells edited in place).
- `databricks bundle validate -t dev -o json`, parsed `silver_users` task
  `base_parameters`: now includes `quarantine_table: ${var.catalog}.quarantine.users`
  alongside the existing three params.
- Full `databricks bundle validate` (text mode, all 3 targets) currently blocked
  in this environment by an expired OAuth refresh token (`error getting token:
  token refresh ... Refresh token is invalid`), unrelated to this change — flagged
  to the user; `-o json` calls still rendered correctly despite the same warning
  on stderr. Re-run `databricks bundle validate -t <target>` after `databricks
  auth login` to get a clean confirmation.

### Status: resolved (pending fresh CLI auth for full remote validation)

---

## 2026-06-18 — gold_user_behavior.ipynb — DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE

### Problems encountered
- `cell-merge`'s `MERGE INTO {gold_table}` failed with
  `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`: the `gold_user_behavior_batch`
  source view had more than one row per `user_id`. Root cause is upstream of this
  notebook — `pipeline_users.ipynb` dedupes/merges `silver.users` by `cpf_key`, not by
  `user_id`, so `silver.users` can still carry duplicate `user_id` values. `cell-transform`'s
  `LEFT JOIN` of the (unique-on-`user_id`) `search_agg`/`rec_agg` full-outer result against
  `users.select("user_id", "cpf", "city", "country")` fans out one row per duplicate match,
  breaking the gold table's one-row-per-`user_id` grain right before the `MERGE`.
  → Solution: in `cell-merge`, added a `Window.partitionBy("user_id").orderBy(desc("_computed_at"))`
    + `row_number()` dedup (same pattern as `dedup_by_cpf` in `pipeline_users.ipynb`) producing
    `behavior_df_deduped`, and pointed `createOrReplaceTempView` at the deduped frame instead of
    `behavior_df`. Used `_computed_at` (the column actually present on this gold frame) rather than
    `_ingested_at` as the freshness tiebreaker.
  → Status: resolved — workaround, not a root-cause fix; the real duplicate-`user_id` rows still
    exist in `silver.users` and will silently pick an arbitrary "freshest" survivor here. Flagging
    for `/design` review: consider deduping `users.select(...)` by `user_id` in `cell-transform`
    instead/also, or enforcing `user_id` uniqueness in `pipeline_users.ipynb`.

### Verification
- `python3 -c "import json; json.load(open('notebooks/cross_domain/gold_user_behavior.ipynb'))"` —
  valid JSON, 6 cells (unchanged count, only `cell-merge` edited).

### Status: resolved (flagged for /design — duplicate user_id in silver.users not addressed at source)
