# KB: CI/CD — GitHub Actions for DABs + ruff/bandit
# sdd-kafka-databricks specific — this file used to be a near-verbatim copy of
# sdd-kafka-snowflake's CI/CD KB (dbt compile, Dagster restart, Snowflake
# secrets). None of that exists in this project: there's no dbt, no Dagster,
# and the deploy target is a Databricks Asset Bundle, not Snowflake/Dagster.
# Corrected 2026-06-20.

## Why CI/CD for this pipeline

Without it, every change to `contracts/*.yml` or `pipelines/ubereats_pipeline.py`
needs a manual, error-prone deploy sequence. Failures this setup actually
catches:
- Committing `.env` with real credentials (`env-guard` job)
- A contract YAML with a typo that breaks `load_contract()` or violates ADR-04
  (`merge_key` not in `cluster_by`) — caught by `tests/test_contracts.py`
- A `dlt_adapter.py` change that silently breaks expectation translation —
  caught by `tests/test_dlt_adapter.py` (see `kb/data-quality.md` — this test
  file existed but never ran in CI until 2026-06-20; fixed)
- A `databricks.yml` change that's invalid for one target but not another —
  caught by `databricks bundle validate` run separately per target

CI validates before merge. CD deploys to `prod` automatically after CI passes
on `main`. There is no dbt compile step and no Dagster restart anywhere in
this project's real CI/CD.

## Pipeline structure (matches .github/workflows/*.yml as of v1.2.0)

```
Push to any branch / PR to main
    └─▶ ci.yml (GitHub Actions)
            ├── env-guard      — fails if .env is tracked by git
            ├── lint           — ruff check . && yamllint contracts/
            ├── test           — pytest tests/ -v (all tests/test_*.py)
            └── bundle-validate (needs: env-guard, lint, test)
                    ├── databricks bundle validate --target dev
                    └── databricks bundle validate --target free_edition
                        (validated separately — its resource shape changed the
                        most of the 3 targets in the pipeline-unification work,
                        docs/adr/007_pipeline_unification.md)

Push to main (after CI succeeds)
    └─▶ deploy.yml (GitHub Actions, workflow_run trigger)
            └── databricks bundle deploy --target prod
```

`deploy.yml` never runs `bundle validate --target prod` itself — it relies on
`ci.yml`'s `bundle-validate` job (dev + free_edition only) plus the
`environment: production` GitHub approval gate. If you ever need to validate
`prod` specifically in CI, add a third `bundle validate --target prod` step to
`ci.yml`'s `bundle-validate` job — it isn't there today.

## ci.yml (real, current)

```yaml
jobs:
  env-guard:
    steps:
      - run: |
          if git ls-files | grep -qE '^\.env$'; then
            echo "ERROR: .env is tracked by git. Remove it: git rm --cached .env"
            exit 1
          fi

  lint:
    steps:
      - run: pip install ruff yamllint
      - run: ruff check .
      - run: yamllint contracts/

  test:
    steps:
      - run: pip install pyyaml pytest pydantic
      - run: pytest tests/ -v

  bundle-validate:
    needs: [env-guard, lint, test]
    steps:
      - uses: databricks/setup-cli@main
      - run: databricks bundle validate --target dev
      - run: databricks bundle validate --target free_edition
```

## deploy.yml (real, current)

```yaml
on:
  workflow_run:
    workflows: ["CI"]
    branches: [main]
    types: [completed]

jobs:
  deploy:
    if: github.event.workflow_run.conclusion == 'success'
    environment: production   # GitHub Environment protection rule = manual approval gate
    steps:
      - uses: actions/checkout@v4
        with:
          ref: ${{ github.event.workflow_run.head_sha }}
      - uses: databricks/setup-cli@main
      - run: databricks bundle deploy --target prod
```

## Local pre-commit gate (.pre-commit-config.yaml, added 2026-06-20)

Runs the same `ruff` lint CI runs, plus `ruff-format`, basic file hygiene
(trailing whitespace, large files, merge conflicts, private keys),
`yamllint` on `contracts/`, and a `bandit` security scan — **none of this ran
locally before this was added; only `ruff`/`yamllint` ran, and only in CI.**

```bash
pip install pre-commit   # or: make precommit-install after `pip install -e ".[dev]"`
pre-commit install
pre-commit run --all-files
```

## Makefile targets (real, current)

```bash
make lint               # ruff check . && yamllint contracts/
make test                # pytest tests/ -v
make security             # bandit -r contracts/ pipelines/ tests/ scripts/ -ll --skip B101,B608
make precommit-install   # pre-commit install
make bundle-validate     # databricks bundle validate --target dev
make deploy-dev          # databricks bundle deploy --target dev
make deploy-prod         # databricks bundle deploy --target prod
```

`deploy-dev`/`deploy-prod` are for manual/local use — the real `prod` deploy
path in CI is `deploy.yml`, not someone running `make deploy-prod` by hand.

## .gitignore (real, current — no dbt/Dagster entries)

```
.env / .env.* (except .env.example)
__pycache__/, *.pyc, .pytest_cache/, .ruff_cache/, .coverage
.databricks/, bundle.lock
*.pem, *.key, credentials.json
```

The `dbt (if added)` block in the real `.gitignore` (`target/`,
`dbt_packages/`, `logs/`) is defensive boilerplate for a dbt project that
doesn't exist here — this project uses Lakeflow Declarative Pipelines, not
dbt, per `CLAUDE.md`'s "What changed from sdd-kafka-snowflake" table.

## GitHub Secrets required

Set in GitHub → Settings → Secrets and variables → Actions:

| Secret name | Used by |
|---|---|
| `DATABRICKS_HOST` | `ci.yml`'s `bundle-validate`, `deploy.yml`'s `deploy` |
| `DATABRICKS_TOKEN` | same as above |

No Snowflake, no Postgres credentials needed in GitHub Secrets — `ci.yml`
never connects to a live database; `tests/test_contracts.py`/
`test_dlt_adapter.py` only parse local YAML, and `bundle validate` is a static
check against `databricks.yml`, not a live deploy.
