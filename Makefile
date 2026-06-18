.PHONY: up down register-connectors produce-initial produce-incremental dry-run lint test bundle-validate deploy-dev deploy-prod

# Infrastructure
up:
	docker compose -f docker-compose.yml -f docker-compose.override.yml up -d --wait

down:
	docker compose -f docker-compose.yml -f docker-compose.override.yml down

register-connectors:
	./scripts/register_connectors.sh

# Data loading
produce-initial:
	python3 tests/load_to_postgres.py --data-dir tests/data/ --batch initial --db-url $(DATABASE_URL)

produce-incremental:
	python3 tests/load_to_postgres.py --data-dir tests/data/ --batch incremental --db-url $(DATABASE_URL)

dry-run:
	python3 tests/load_to_postgres.py --data-dir tests/data/ --dry-run

# Quality
lint:
	ruff check .
	yamllint contracts/

test:
	pytest tests/test_contracts.py -v

# Databricks
bundle-validate:
	databricks bundle validate --target dev

deploy-dev:
	databricks bundle deploy --target dev

deploy-prod:
	databricks bundle deploy --target prod
