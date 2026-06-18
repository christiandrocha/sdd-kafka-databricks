# KB: CI/CD — GitHub Actions for dbt + Kafka Connect
# Knowledge base for sdd-kafka-snowflake agents

## Why CI/CD for a data pipeline project

Without CI/CD, every change to connectors, dbt models or Dagster code
requires a manual, error-prone deploy sequence. Common failures:
- Committing .env with real credentials
- Deploying malformed connector JSON that silently fails
- dbt model with broken ref() that only fails at runtime
- Deploying to prod without running dbt compile first

CI validates before merge. CD automates after merge. Together they
make the pipeline reproducible and auditable.

## Pipeline structure

```
Feature branch → PR opened
    └─▶ ci.yml (GitHub Actions)
            ├── dbt compile (syntax + ref() resolution, no Snowflake)
            ├── connector JSON lint (valid JSON, required fields)
            ├── .env guard (fails if .env is in the PR)
            └── pre-commit checks (trailing whitespace, etc.)

PR merged to main
    └─▶ deploy.yml (GitHub Actions)
            ├── register_connectors.sh --env prod (GitHub Secrets → Snowflake)
            └── dagster deploy (restart dagster + dagster-daemon)
```

## ci.yml structure

```yaml
name: CI
on:
  pull_request:
    branches: [main]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dbt
        run: pip install dbt-core==1.7.* dbt-snowflake==1.7.*

      - name: dbt deps
        working-directory: dbt
        run: dbt deps

      - name: dbt compile (no Snowflake connection)
        working-directory: dbt
        run: dbt compile --profiles-dir .ci/profiles
        # .ci/profiles/profiles.yml uses dummy Snowflake credentials
        # dbt compile validates syntax and ref() without executing queries

      - name: Lint connector JSONs
        run: |
          for f in connectors/*.json; do
            python -m json.tool "$f" > /dev/null || exit 1
          done

      - name: Check .env not committed
        run: |
          if git diff --name-only origin/main...HEAD | grep -E '^\.env$'; then
            echo "ERROR: .env file must not be committed"
            exit 1
          fi
```

## deploy.yml structure

```yaml
name: Deploy
on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    environment: production
    steps:
      - uses: actions/checkout@v4

      - name: Register Kafka connectors
        env:
          SNOWFLAKE_URL:         ${{ secrets.SNOWFLAKE_URL }}
          SNOWFLAKE_USER:        ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PRIVATE_KEY: ${{ secrets.SNOWFLAKE_PRIVATE_KEY }}
          SNOWFLAKE_DATABASE:    ${{ secrets.SNOWFLAKE_DATABASE }}
          SNOWFLAKE_ROLE:        ${{ secrets.SNOWFLAKE_ROLE }}
          POSTGRES_USER:         ${{ secrets.POSTGRES_USER }}
          POSTGRES_PASSWORD:     ${{ secrets.POSTGRES_PASSWORD }}
        run: |
          chmod +x scripts/register_connectors.sh
          ./scripts/register_connectors.sh --env prod

      - name: Restart Dagster
        run: |
          # In production, this would SSH to the server or call a deploy API
          echo "Dagster deploy step — implement per production environment"
```

## CI-safe dbt profiles

dbt compile in CI does not need a real Snowflake connection.
Use a dummy profiles file that satisfies dbt's schema validation:

```yaml
# infra/dbt/.ci/profiles/profiles.yml
sdd_kafka_snowflake:
  target: ci
  outputs:
    ci:
      type: snowflake
      account: ci-dummy
      user: ci-dummy
      password: ci-dummy
      database: CDC_POC
      warehouse: CDC_WH
      schema: SILVER
      threads: 1
```

dbt compile resolves ref() and source() without executing any SQL.
Malformed Jinja, broken ref() and missing columns are caught here.

## .gitignore for this project

```
# Credentials — never commit
.env
.env.*
!.env.example

# dbt build artifacts
dbt/target/
dbt/dbt_packages/
dbt/logs/

# Python
__pycache__/
*.pyc
*.pyo
.pytest_cache/

# Dagster
dagster/dagster_home/

# RSA keys
rsa_key*.p8
rsa_key*.pem

# OS
.DS_Store
```

## pre-commit configuration

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.5.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-json          # catches malformed connector JSONs
      - id: check-yaml          # catches malformed YAML configs

  - repo: https://github.com/Yelp/detect-secrets
    rev: v1.4.0
    hooks:
      - id: detect-secrets      # blocks .env and credential patterns
        args: ['--baseline', '.secrets.baseline']
```

Install and activate:
```bash
pip install pre-commit detect-secrets
pre-commit install
detect-secrets scan > .secrets.baseline
```

## docker-compose.override.yml pattern

Docker Compose loads `docker-compose.override.yml` automatically when
both files are in the same directory. No flag needed for local dev.

```yaml
# docker-compose.override.yml — local only, not for production
version: "3.8"

services:
  postgres:
    ports:
      - "5432:5432"   # exposed locally for psql access
    volumes:
      - ./scripts/init.sql:/docker-entrypoint-initdb.d/init.sql

  kafka:
    ports:
      - "9092:9092"   # exposed for local Kafka clients

  kafka-connect:
    ports:
      - "8083:8083"

  dagster:
    volumes:
      - ./dbt:/opt/dagster/dbt        # live reload dbt models in local
      - ./dagster:/opt/dagster/app
      - ./scripts:/opt/dagster/scripts  # sync_metadata.py accessible to Dagster
```

## docker-compose.prod.yml — reference topology

```yaml
# docker-compose.prod.yml — REFERENCE ONLY
# In production:
# - Kafka → Confluent Cloud or AWS MSK (remove zookeeper, kafka services)
# - Schema Registry → Confluent Cloud (remove schema-registry service)
# - Prometheus + Grafana → managed (Grafana Cloud, etc.)
# - All services: restart: always, no exposed ports, secrets via vault

version: "3.8"

services:
  postgres:
    restart: always
    # No ports exposed — accessed via internal network only

  kafka-connect:
    restart: always
    environment:
      BOOTSTRAP_SERVERS: <confluent-cloud-bootstrap>:9092
      # ...confluent cloud credentials via secrets manager

  dagster:
    restart: always

  dagster-daemon:
    restart: always
```

## GitHub Secrets required for deploy.yml

Set in GitHub → Settings → Secrets and variables → Actions:

| Secret name | Description |
|---|---|
| SNOWFLAKE_URL | `<account>.snowflakecomputing.com` |
| SNOWFLAKE_USER | Snowflake service account user |
| SNOWFLAKE_PRIVATE_KEY | Base64-encoded private key |
| SNOWFLAKE_DATABASE | `CDC_POC` (or prod equivalent) |
| SNOWFLAKE_ROLE | `CDC_ROLE` (or prod equivalent) |
| POSTGRES_USER | PostgreSQL user |
| POSTGRES_PASSWORD | PostgreSQL password |
