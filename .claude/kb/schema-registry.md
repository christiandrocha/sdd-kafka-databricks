# KB: Schema Registry — Avro and Schema Evolution
# sdd-kafka-databricks specific — this file used to be a near-verbatim copy of
# a sibling project's schema-registry KB (Snowflake VARIANT casting, dbt
# models, a `usuarios`/`produtos` example schema, a `set_compatibility.sh`
# script that doesn't exist in this repo). Corrected 2026-06-20.

## Why Schema Registry

Without it, each Kafka message has two bad options:
1. `schemas.enable: false` → no typing, everything becomes STRING
2. Schema repeated in every message → high volume overhead

With Schema Registry + Avro (Confluent, per `CLAUDE.md` — not Apicurio):
- Centralized and versioned schema outside the messages
- Each message carries only 4 bytes of schema ID
- Breaking changes blocked before reaching Kafka
- Types preserved per Debezium's wire encoding (see mapping below)

## Structure of an Avro message in Kafka

```
[0x00][schema_id (4 bytes)][Avro binary payload]
 └── magic byte = 0
```

`register_bronze()` in `pipelines/ubereats_pipeline.py` strips this prefix
itself before decoding: `expr("substring(value, 6)")` (1 magic byte + 4 schema
ID bytes = 5, so the Avro payload starts at byte 6 in Spark's 1-indexed
`substring`), then calls `from_avro()` with the schema fetched from the
registry via `_avro_schema_str()`.

## Compatibility modes

| Mode | Add field | Remove field | Change type |
|------|-----------|--------------|-------------|
| BACKWARD | ✅ (nullable+default) | ✅ | ✖ |
| FORWARD | ✖ | ✅ | ✖ |
| FULL | ✅ (nullable+default) | ✖ | ✖ |
| NONE | ✅ | ✅ | ✅ |

**This project uses BACKWARD** (set globally by `scripts/register_connectors.sh`
on startup) — new consumers (Databricks, reading whatever the latest registered
schema is via `_avro_schema_str()`) must be able to read old messages still
sitting in a topic. Each contract's `schema_evolution.new_fields: allowed` /
`removed_fields: forbidden` / `type_changes: forbidden` (`contracts/*.yml`)
mirrors exactly what BACKWARD permits — that's not a coincidence, the contract
convention was designed to match the registry's actual enforcement.

## PostgreSQL → Avro → Delta type mapping (real Debezium config)

Per `connectors/debezium.json`'s actual settings (`decimal.handling.mode=double`,
`time.precision.mode=connect`, `interval.handling.mode=string` — see
`kb/kafka-cdc.md`):

| PostgreSQL | Avro | Contract `type` (`contracts/*.yml`) |
|---|---|---|
| INTEGER / SERIAL | int | `integer` |
| BIGINT | long | `long` |
| NUMERIC(p,s) | double (not BYTES — `decimal.handling.mode=double`) | `double` |
| VARCHAR / TEXT / UUID | string | `string` |
| BOOLEAN | boolean | `boolean` |
| TIMESTAMP (no tz) | long (epoch millis, `time.precision.mode=connect`) | `timestamp` |
| TIMESTAMPTZ | string (ISO-8601, `io.debezium.time.ZonedTimestamp`) — **not** epoch-millis, see below | `timestamp` |
| DATE | int (days since epoch) | `date` |
| NULL (nullable) | `["null", type]` union | `nullable: true` |

**TIMESTAMPTZ vs TIMESTAMP wire formats differ** — this was a real bug caught
during `scripts/export_kafka_to_volume.py` development (`.claude/05_implementation_log.md`):
Debezium emits `TIMESTAMPTZ` columns as ISO-8601 strings, but plain `TIMESTAMP`
columns as epoch-millis longs, under the same `time.precision.mode=connect`.
Code that only handles the `int`/`long` case will fail on every `TIMESTAMPTZ`
field. `register_bronze()`'s `timestamp_fields` cast loop in
`pipelines/ubereats_pipeline.py` and `export_kafka_to_volume.py`'s
`_cast_record()` both need to handle both cases — see `kb/medallion.md`.

There is no Snowflake VARIANT anywhere in this project — Bronze tables are
Delta with typed columns from `from_avro()` directly, not a JSON/VARIANT
landing zone cast later by a dbt model.

## Schema evolution: what is allowed (BACKWARD)

```sql
-- ✅ Add nullable column — compatible
ALTER TABLE drivers ADD COLUMN insurance_number VARCHAR(30) DEFAULT NULL;

-- ✅ Add column with explicit default — compatible
ALTER TABLE products ADD COLUMN discount NUMERIC(5,2) DEFAULT 0.00;
```

Then: add the field to the matching `contracts/<domain>.yml`'s `schema:` list
(`new_fields: allowed` already permits it) and re-run `tests/test_contracts.py`
to confirm the contract still validates.

## Schema evolution: what is blocked (BACKWARD) — and what this project declares forbidden anyway

```sql
-- ✖ Rename column — old consumers expect the old name, also forbidden by
-- every contract's schema_evolution.removed_fields: forbidden (a rename is a
-- drop + an add from the contract's point of view)
-- ALTER TABLE drivers RENAME COLUMN phone_number TO contact_phone;

-- ✖ Change type — breaks Avro deserialization, also forbidden by
-- schema_evolution.type_changes: forbidden in every contract
-- ALTER TABLE drivers ALTER COLUMN driver_id TYPE INTEGER;

-- ✖ Add NOT NULL without a default — incompatible with rows already in Bronze
-- ALTER TABLE drivers ADD COLUMN tax_id VARCHAR(14) NOT NULL;
```

## An undeclared field reaching the registry is a real, observed failure mode

`delete.handling.mode=rewrite` (`connectors/debezium.json`) adds a `__deleted`
field to **every** record (not just deletes) that no `contracts/*.yml` declares.
Confirmed live (2026-06-20): `curl http://localhost:8081/subjects/pg.public.payments-value/versions/latest`
shows `__deleted` in the schema from version 1 onward — BACKWARD compatibility
happily registers it since it's an added nullable field, exactly the kind of
change this section says is allowed. The contract's `new_fields: allowed`
setting means downstream code has to actively decide what to do with a field
like this, not just assume the registry blocking incompatible changes is
enough: `export_kafka_to_volume.py`'s `_cast_record()` explicitly drops it;
`register_bronze()` in `pipelines/ubereats_pipeline.py` does not, so it flows
into Bronze as schema drift in `kafka` mode. This is still true after the C08
delete-handling fix (2026-06-20, `kb/anti-patterns.md`) — the fix uses `__op`
(declared, already relied upon elsewhere), not `__deleted`, as the
`apply_as_deletes` condition, precisely to avoid depending on this undeclared
field. `__deleted` itself remains unread, undeclared schema drift either way.
See `kb/anti-patterns.md` (C08) and `CLAUDE.md` for the full mechanism.

## Schema Registry REST API

```bash
# List registered subjects
curl http://localhost:8081/subjects

# View current schema for a real topic in this project
curl http://localhost:8081/subjects/pg.public.payments-value/versions/latest

# View global compatibility level
curl http://localhost:8081/config

# Set BACKWARD globally — done automatically by scripts/register_connectors.sh
curl -X PUT http://localhost:8081/config \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "BACKWARD"}'

# Test compatibility before applying a schema change
curl -X POST http://localhost:8081/compatibility/subjects/pg.public.payments-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "<avro schema json>"}'
```

There is no `set_compatibility.sh` or equivalent wrapper script in this
project (`scripts/` only has `export_kafka_to_volume.py`,
`preflight_unity_catalog.sh`, `register_connectors.sh`) — use the REST API
above directly, or write one if this becomes a recurring need.
