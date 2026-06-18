{
  "version": "1.0.0",
  "project": "sdd-kafka-databricks",
  "last_updated": "2026-06-16",

  "architecture": {
    "data_flow": [
      "100 JSON files (20 domains, 129,353 records)",
      "load_to_postgres.py (80% initial + 20% incremental)",
      "PostgreSQL 16 (20 tables, WAL wal_level=logical)",
      "Debezium PostgreSQL Source Connector (NO SMT — raw envelope)",
      "Confluent Schema Registry (Avro, BACKWARD compatibility)",
      "Kafka KRaft (20 topics: pg.public.{table})",
      "Databricks Structured Streaming (trigger availableNow=True)",
      "pipeline_bronze.ipynb (parametrized, 20x via DABs)",
      "ubereats_dev.bronze.{table} (append-only, raw Debezium envelope)",
      "pipeline_silver.ipynb (parametrized, 11x via DABs)",
      "ubereats_dev.silver.{table} (MERGE INTO + quality rules)",
      "cross_domain/gold_*.ipynb (6 notebooks, fact × dimensions JOIN)",
      "ubereats_dev.gold.{model} (Liquid Clustering, Delta Lake)"
    ],

    "unity_catalog": {
      "catalogs": ["ubereats_dev", "ubereats_prod"],
      "schemas": ["bronze", "silver", "gold", "quarantine"],
      "bronze_tables": 20,
      "silver_tables": 11,
      "gold_tables": 6,
      "quarantine_tables": 11
    },

    "notebooks": {
      "parametrized": ["notebooks/pipeline_bronze.ipynb", "notebooks/pipeline_silver.ipynb"],
      "cross_domain": [
        "notebooks/cross_domain/gold_payment_lifecycle.ipynb",
        "notebooks/cross_domain/gold_payment_funnel.ipynb",
        "notebooks/cross_domain/gold_payments_by_status.ipynb",
        "notebooks/cross_domain/gold_driver_performance.ipynb",
        "notebooks/cross_domain/gold_revenue_per_restaurant.ipynb",
        "notebooks/cross_domain/gold_user_behavior.ipynb"
      ],
      "dabs_bronze_runs": 20,
      "dabs_silver_runs": 11
    }
  },

  "adrs": [
    {
      "id": "ADR-01",
      "title": "Databricks instead of Snowflake",
      "status": "accepted",
      "decision": "Replace Snowflake Sink + dbt + Dagster with Databricks Structured Streaming + PySpark + DABs",
      "rationale": "Unity Catalog for governance + lineage, Spark-native streaming (no Snowpipe latency), Delta Lake Liquid Clustering, DABs for GitOps-native orchestration, trigger(availableNow=True) scales to zero",
      "alternatives_considered": ["Snowflake (original)", "BigQuery", "Databricks DLT/Lakeflow"],
      "consequences": {
        "positive": ["No Snowpipe latency", "Native streaming", "Unity Catalog governance", "Liquid Clustering"],
        "negative": ["Requires Databricks workspace", "More complex local dev setup"]
      }
    },
    {
      "id": "ADR-02",
      "title": "Bronze = flat records via SMT (ExtractNewRecordState)",
      "status": "accepted",
      "decision": "Use ExtractNewRecordState SMT in connectors/debezium.json. Bronze receives flat business fields plus __op and __source_ts_ms, not the raw Debezium envelope. There is no unwrap step in Silver — pipeline_bronze.ipynb already parses post-SMT Avro and pipeline_silver.ipynb has no before/after navigation logic.",
      "rationale": "The audit-trail argument for skipping the SMT only applies to bidirectional CDC topologies. This project is unidirectional (JSON exports -> PostgreSQL -> Debezium -> Kafka -> Databricks), matching the proven sdd-kafka-snowflake pattern, where the SMT is the correct, simpler choice. Corrected during the design retrospective (06_retrospective.md, 'v1.0.0 - Design complete') after the bidirectional-topology concern was abandoned; this formal ADR block was not updated to match until v1.0.1.",
      "alternatives_considered": ["Raw envelope, no SMT, unwrap in Silver (rejected — was the pre-correction decision; adds Spark compute and complexity with no benefit for a unidirectional topology)"],
      "consequences": {
        "positive": ["Simpler Silver notebook (no envelope navigation)", "Matches the validated sdd-kafka-snowflake pattern", "Less Spark compute in Silver"],
        "negative": ["Bronze does not preserve before/after/source for audit replay — acceptable for this unidirectional topology"]
      },
      "interview_phrase": "We use the ExtractNewRecordState SMT in Bronze. For a unidirectional CDC topology there's no audit-trail benefit to keeping the raw envelope, so the SMT keeps Bronze and Silver simpler without losing anything we'd actually use."
    },
    {
      "id": "ADR-03",
      "title": "2 parametrized notebooks instead of 60 static notebooks",
      "status": "accepted",
      "decision": "pipeline_bronze.ipynb and pipeline_silver.ipynb receive table_name, kafka_topic, contract_path, bronze_table, silver_table, checkpoint as widgets. DABs orchestrates 20 bronze runs and 11 silver runs with domain-specific parameters.",
      "rationale": "DRY principle applied to data engineering. 60 static notebooks would require editing 20 files for any logic change. Parametrized notebooks demonstrate software engineering maturity.",
      "alternatives_considered": ["60 static notebooks (rejected — violates DRY)", "Lakeflow DLT pipelines (rejected — different abstraction, less explicit control)"],
      "consequences": {
        "positive": ["DRY — single change point", "DABs handles parallelism", "Easier to test"],
        "negative": ["All domains share same notebook logic — special cases require widget-driven conditionals"]
      },
      "interview_phrase": "Instead of 60 notebooks, we built 2 parametrized ones via dbutils.widgets. DABs orchestrates them 20x and 11x with domain-specific configs — applying DRY to data engineering."
    },
    {
      "id": "ADR-04",
      "title": "Liquid Clustering cluster_by MUST equal merge_key",
      "status": "accepted",
      "decision": "The columns in cluster_by for Silver and Gold tables must exactly match the columns in the MERGE INTO ON clause. Misalignment causes Databricks to perform full table scans during MERGE.",
      "rationale": "Liquid Clustering only enables file pruning during MERGE when the ON condition columns match the cluster columns. test_contracts.py validates this alignment automatically.",
      "verification": "test_contracts.py: test_cluster_by_aligns_with_merge_key()",
      "alignment_table": {
        "silver_payment_events": {"cluster_by": ["event_id", "event_ts"], "merge_on": "event_id"},
        "silver_orders": {"cluster_by": ["order_id"], "merge_on": "order_id"},
        "silver_users": {"cluster_by": ["cpf"], "merge_on": "cpf"},
        "gold_payment_lifecycle": {"cluster_by": ["payment_id"], "merge_on": "payment_id"},
        "gold_driver_performance": {"cluster_by": ["driver_id"], "merge_on": "driver_id"},
        "gold_revenue_per_restaurant": {"cluster_by": ["restaurant_cnpj"], "merge_on": "restaurant_cnpj"}
      },
      "interview_phrase": "Liquid Clustering only accelerates MERGE when cluster_by matches the MERGE ON columns. We enforce this in test_contracts.py so it's validated before any code runs."
    },
    {
      "id": "ADR-05",
      "title": "Confluent Schema Registry instead of Apicurio",
      "status": "accepted",
      "decision": "Use Confluent Schema Registry (identical to sdd-kafka-snowflake). Do not introduce Apicurio Registry.",
      "rationale": "Confluent Schema Registry is battle-tested with Debezium, has native Avro support, and is identical to the reference project. Apicurio adds complexity (in-memory data loss risk, additional container, different API endpoint) without meaningful benefit for this use case.",
      "alternatives_considered": ["Apicurio with PostgreSQL backend (rejected — complexity without benefit)"],
      "consequences": {
        "positive": ["Zero new infrastructure vs reference project", "Native Debezium support", "Proven stability"],
        "negative": ["Confluent license (free for OSS use)"]
      }
    },
    {
      "id": "ADR-06",
      "title": "Unified PostgreSQL via load_to_postgres.py (unidirectional CDC)",
      "status": "accepted",
      "decision": "Use load_to_postgres.py to populate PostgreSQL from JSON files. Debezium reads WAL → Kafka → Databricks. No bidirectional topology (no JDBC Sink).",
      "rationale": "The 4 source systems (Kafka, MongoDB, MySQL, MSSQL) are snapshot exports, not live production systems. Bidirectional Debezium would introduce loop risk, require Groovy filter transforms, and _source column anti-loop mechanism — all unnecessary complexity.",
      "alternatives_considered": ["Bidirectional Debezium with JDBC Sink (rejected — loop risk, over-engineering)"],
      "consequences": {
        "positive": ["Simple topology", "No loop risk", "Identical to sdd-kafka-snowflake"],
        "negative": ["Not a true real-time multi-source topology — load_to_postgres.py is a simulation"]
      }
    },
    {
      "id": "ADR-07",
      "title": "silver.users = FULL OUTER JOIN users_mongo + users_mssql by CPF",
      "status": "accepted",
      "decision": "Merge two user sources (users_mongo and users_mssql) in Silver using CPF as the business key. Use materialized='table' (full refresh) because FULL OUTER JOIN is incompatible with incremental MERGE.",
      "rationale": "Both user sources share CPF as business key. 700 combined records — full refresh cost is negligible. Same pattern as sdd-kafka-snowflake silver_users.",
      "consequences": {
        "positive": ["Single user entity in Silver", "CPF-based joins work downstream"],
        "negative": ["Full refresh on every run (acceptable at ~700 rows)"]
      }
    },
    {
      "id": "ADR-08",
      "title": "order_items separate handling (85% of total volume)",
      "status": "accepted",
      "decision": "pipeline_bronze.ipynb and pipeline_silver.ipynb receive maxOffsetsPerTrigger as a widget. order_items DABs task uses a higher value (5000) vs standard domains (1000).",
      "rationale": "order_items has 110,001 records (85% of total). Using the same offset limit as other domains would require 110 micro-batches vs 1-5 for others. Separate DABs parameter avoids blocking.",
      "consequences": {
        "positive": ["order_items processed efficiently", "Other domains not blocked"],
        "negative": ["Slightly different DABs configuration for one domain"]
      }
    }
  ],

  "domain_map": {
    "payment_events": {"source": "kafka_events", "pk": "event_id", "records": 2208, "silver": true, "special": "nested JSONB event field"},
    "orders":         {"source": "kafka_orders", "pk": "order_id", "records": 405,  "silver": true, "special": "hub table with CPF/CNPJ/driver_id business keys"},
    "payments":       {"source": "kafka_payments","pk": "payment_id","records": 260, "silver": true},
    "order_items":    {"source": "mongodb_items", "pk": "order_item_id","records": 110001,"silver": true, "special": "85% of total volume — larger maxOffsetsPerTrigger"},
    "gps_events":     {"source": "kafka_gps",    "pk": "gps_id",    "records": 7350, "silver": false},
    "order_status":   {"source": "kafka_status", "pk": "status_id", "records": 4176, "silver": true, "special": "nested JSONB status field"},
    "routes":         {"source": "kafka_route",  "pk": "route_id",  "records": 410,  "silver": false},
    "receipts":       {"source": "kafka_receipts","pk": "receipt_id","records": 377, "silver": false, "special": "no dt_current_timestamp — uses receipt_generated_at"},
    "driver_shifts":  {"source": "kafka_shift",  "pk": "shift_id",  "records": 468,  "silver": true},
    "search_events":  {"source": "kafka_search", "pk": "search_id", "records": 202,  "silver": true},
    "recommendations":{"source": "mongodb_recommendations","pk": "event_id","records": 254,"silver": true},
    "support_tickets":{"source": "mongodb_support","pk": "ticket_id","records": 410, "silver": false},
    "users_mongo":    {"source": "mongodb_users","pk": "uuid",      "records": 411,  "silver": "merged"},
    "users_mssql":    {"source": "mssql_users",  "pk": "uuid",      "records": 288,  "silver": "merged"},
    "restaurants":    {"source": "mysql_restaurants","pk": "uuid",  "records": 461,  "silver": true},
    "drivers":        {"source": "postgres_drivers","pk": "uuid",   "records": 354,  "silver": true},
    "products":       {"source": "mysql_products","pk": "product_id","records": 368, "silver": false},
    "menu_sections":  {"source": "mysql_menu",   "pk": "menu_section_id","records": 362,"silver": false},
    "ratings":        {"source": "mysql_ratings","pk": "rating_id", "records": 327,  "silver": false},
    "inventory":      {"source": "postgres_inventory","pk": "stock_id","records": 261,"silver": false, "special": "no dt_current_timestamp — uses last_updated"}
  },

  "file_manifest": {
    "contracts": [
      "contracts/payment_events.yml",
      "contracts/orders.yml",
      "contracts/payments.yml",
      "contracts/order_items.yml",
      "contracts/gps_events.yml",
      "contracts/order_status.yml",
      "contracts/routes.yml",
      "contracts/receipts.yml",
      "contracts/driver_shifts.yml",
      "contracts/search_events.yml",
      "contracts/recommendations.yml",
      "contracts/support_tickets.yml",
      "contracts/users.yml",
      "contracts/restaurants.yml",
      "contracts/drivers.yml",
      "contracts/products.yml",
      "contracts/menu_sections.yml",
      "contracts/ratings.yml",
      "contracts/inventory.yml",
      "contracts/loader.py",
      "contracts/spark_schema.py",
      "contracts/pydantic_models.py"
    ],
    "notebooks": [
      "notebooks/pipeline_bronze.ipynb",
      "notebooks/pipeline_silver.ipynb",
      "notebooks/cross_domain/gold_payment_lifecycle.ipynb",
      "notebooks/cross_domain/gold_payment_funnel.ipynb",
      "notebooks/cross_domain/gold_payments_by_status.ipynb",
      "notebooks/cross_domain/gold_driver_performance.ipynb",
      "notebooks/cross_domain/gold_revenue_per_restaurant.ipynb",
      "notebooks/cross_domain/gold_user_behavior.ipynb"
    ],
    "infrastructure": [
      "docker-compose.yml",
      "docker-compose.override.yml",
      "Dockerfile.connect",
      "connectors/debezium.json",
      "sql/init.sql",
      "scripts/register_connectors.sh",
      "scripts/preflight_unity_catalog.sh",
      "tests/load_to_postgres.py"
    ],
    "orchestration": [
      "databricks.yml"
    ],
    "observability": [
      "observability/prometheus/prometheus.yml",
      "observability/prometheus/alert_rules.yml",
      "observability/grafana/dashboards/kafka.json",
      "observability/grafana/dashboards/kafka_connect.json",
      "observability/jmx/kafka-jmx-exporter.yml"
    ],
    "cicd": [
      ".github/workflows/ci.yml",
      ".github/workflows/deploy.yml",
      ".gitignore",
      "Makefile",
      "pyproject.toml"
    ],
    "tests": [
      "tests/test_contracts.py"
    ]
  }
}
