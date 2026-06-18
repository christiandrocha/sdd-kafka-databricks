# KB: Schema Registry — Avro and Schema Evolution
# Knowledge base for ai-kafka-microbatch agents

## Why Schema Registry

Without Schema Registry, each Kafka JSON message has two bad options:
1. `schemas.enable: false` → no typing, everything becomes STRING in Parquet
2. `schemas.enable: true` → schema repeated in every message (high volume)

With Schema Registry + Avro:
- Centralized and versioned schema outside the messages
- Each message carries only 4 bytes of schema ID
- Breaking changes blocked before reaching Kafka
- Types preserved: NUMERIC → DOUBLE, TIMESTAMPTZ → INT64 micros

## Structure of an Avro message in Kafka

```
[0x00][schema_id (4 bytes)][Avro binary payload]
 └── magic byte = 0
```

The consumer reads the schema ID, queries the Registry, deserializes the payload.

## Compatibility modes

| Mode | Add field | Remove field | Change type |
|------|-----------|--------------|-------------|
| BACKWARD | ✅ (nullable+default) | ✅ | ✖ |
| FORWARD | ✖ | ✅ | ✖ |
| FULL | ✅ (nullable+default) | ✖ | ✖ |
| NONE | ✅ | ✅ | ✅ |

**This project uses BACKWARD** — new consumers read old data.
Appropriate when the Snowflake Sink and dbt models are updated before
or alongside Debezium.

## PostgreSQL → Avro → Snowflake VARIANT type mapping

| PostgreSQL | Avro | Snowflake VARIANT (cast in Bronze) |
|---|---|---|
| SERIAL / INTEGER | int | `::INT` |
| BIGINT | long | `::BIGINT` |
| NUMERIC(p,s) | double | `::FLOAT` (ADR-13: 83% of timestamps are float) |
| VARCHAR / TEXT | string | `::VARCHAR` |
| BOOLEAN | boolean | `::BOOLEAN` |
| TIMESTAMPTZ | long (micros) | `::BIGINT` then converted |
| NULL (nullable) | ["null", type] | evaluates to `NULL` in VARIANT path |

## Schema evolution: what is allowed (BACKWARD)

```sql
-- ✅ Add nullable column — compatible
ALTER TABLE usuarios ADD COLUMN telefone VARCHAR(20) DEFAULT NULL;

-- ✅ Add column with explicit default — compatible
ALTER TABLE produtos ADD COLUMN desconto NUMERIC(5,2) DEFAULT 0.00;

-- ✅ Drop column — compatible (old consumers simply ignore the absence)
ALTER TABLE usuarios DROP COLUMN IF EXISTS obsolete_field;
```

## Schema evolution: what is blocked (BACKWARD)

```sql
-- ✖ Rename column — old consumers expect the old name
-- ALTER TABLE usuarios RENAME COLUMN email TO email_address;

-- ✖ Change type — breaks Avro serialization
-- ALTER TABLE usuarios ALTER COLUMN id TYPE TEXT;

-- ✖ Add NOT NULL without DEFAULT — incompatible with v1 data
-- ALTER TABLE usuarios ADD COLUMN cpf VARCHAR(14) NOT NULL;
```

## Schema Registry REST API

```bash
# List registered subjects
curl http://localhost:8081/subjects

# View current schema
curl http://localhost:8081/subjects/pg.public.usuarios-value/versions/latest

# View global compatibility level
curl http://localhost:8081/config

# Set BACKWARD globally
curl -X PUT http://localhost:8081/config \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "BACKWARD"}'

# Test compatibility before applying
curl -X POST http://localhost:8081/compatibility/subjects/pg.public.usuarios-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "<avro schema json>"}'
```

## set_compatibility.sh utility

```bash
./infra/scripts/set_compatibility.sh list               # list subjects
./infra/scripts/set_compatibility.sh show <subject>     # view fields and types
./infra/scripts/set_compatibility.sh versions <subject> # version history
./infra/scripts/set_compatibility.sh check <subject> <file.avsc>  # test compatibility
./infra/scripts/set_compatibility.sh compat [subject]   # view level
```

## Schema version log (keep updated)

| Version | Date | Table | Change | Compatible |
|---|---|---|---|---|
| v1 | 2026-05-14 | usuarios | Initial schema | — |
| v1 | 2026-05-14 | produtos | Initial schema | — |
| v2 | — | usuarios | ADD COLUMN telefone VARCHAR(20) DEFAULT NULL | ✅ BACKWARD |

## Schema evolution in Snowflake VARIANT

Partitions with v1 schema (no `telefone`) and v2 (with `telefone`) coexist
in the same VARIANT table. The Bronze dbt model handles this automatically:

- `on_schema_change = 'sync_all_columns'` adds the new column to the Bronze
  typed table on the next incremental run
- Rows without the new field return `NULL` when the VARIANT path is accessed
- No manual migration needed in Snowflake — the VARIANT landing is always additive
