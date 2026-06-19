#!/usr/bin/env python3
"""Export the pg.public.* Kafka topics (post-SMT) to local Parquet, one
directory per domain, casting fields to the types declared in contracts/*.yml.

Output feeds pipelines/ubereats_pipeline.py's register_bronze() source_mode=volume
path (Free Edition, where the serverless compute may not reach a self-hosted
Kafka broker).
Upload the result to the landing.kafka_export Volume with `databricks fs cp`.
"""
from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import DeserializingConsumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from contracts.loader import load_contract  # noqa: E402

_PYARROW_TYPE_MAP: dict[str, pa.DataType] = {
    "string": pa.string(),
    "integer": pa.int32(),
    "long": pa.int64(),
    "double": pa.float64(),
    "boolean": pa.bool_(),
    "timestamp": pa.timestamp("ms"),
    "date": pa.date32(),
}

# Fields added downstream of the export, never present in the Kafka payload.
_BRONZE_ONLY_FIELDS = {"_ingested_at"}


def _arrow_schema_for(contract: dict) -> pa.Schema:
    return pa.schema(
        [
            (f["name"], _PYARROW_TYPE_MAP[f["type"]])
            for f in contract["schema"]
            if f["name"] not in _BRONZE_ONLY_FIELDS
        ]
    )


def _cast_record(record: dict, contract_schema: list[dict]) -> dict:
    """Coerce decoded Avro values to the type the contract declares, and
    drop fields the contract doesn't know about (e.g. Debezium's __deleted).

    Debezium emits timestamps two ways depending on the source column type:
    TIMESTAMPTZ -> io.debezium.time.ZonedTimestamp, an ISO-8601 string;
    TIMESTAMP (no tz) under time.precision.mode=connect -> epoch-millis long.
    The contract is the source of truth for the target type either way.
    """
    known_fields = {f["name"] for f in contract_schema} - _BRONZE_ONLY_FIELDS
    casted = {k: v for k, v in record.items() if k in known_fields}
    for field in contract_schema:
        name = field["name"]
        if name in _BRONZE_ONLY_FIELDS or name not in casted:
            continue
        value = casted[name]
        if value is None:
            continue
        if field["type"] == "timestamp":
            if isinstance(value, int):
                casted[name] = datetime.fromtimestamp(value / 1000, tz=UTC)
            elif isinstance(value, str):
                casted[name] = datetime.fromisoformat(value)
    return casted


def export_topic(domain: str, contract_path: Path, args: argparse.Namespace) -> int:
    contract = load_contract(contract_path)
    topic = contract["table"]["kafka_topic"]
    arrow_schema = _arrow_schema_for(contract)

    sr_client = SchemaRegistryClient({"url": args.schema_registry_url})
    avro_deserializer = AvroDeserializer(sr_client)

    consumer = DeserializingConsumer(
        {
            "bootstrap.servers": args.kafka_bootstrap,
            "group.id": f"export-{domain}-{datetime.now(tz=UTC).timestamp()}",
            "key.deserializer": None,
            "value.deserializer": avro_deserializer,
            "auto.offset.reset": "earliest",
        }
    )

    out_dir = Path(args.output_dir) / domain
    out_dir.mkdir(parents=True, exist_ok=True)

    existing_topics = consumer.list_topics(timeout=10).topics
    if topic not in existing_topics:
        print(f"[export] {domain:<20} topic={topic:<30} not found — writing empty Parquet")
        pq.write_table(pa.Table.from_pylist([], schema=arrow_schema), out_dir / "data.parquet")
        consumer.close()
        return 0

    tp = TopicPartition(topic, 0)
    consumer.assign([tp])
    low, high = consumer.get_watermark_offsets(tp, timeout=10)

    records: list[dict] = []
    empty_polls = 0
    while True:
        current = consumer.position([tp])[0].offset
        if current >= high:
            break
        msg = consumer.poll(timeout=5.0)
        if msg is None:
            empty_polls += 1
            if empty_polls >= 3:
                break
            continue
        empty_polls = 0
        if msg.error():
            continue
        records.append(_cast_record(msg.value(), contract["schema"]))

    consumer.close()

    table = pa.Table.from_pylist(records, schema=arrow_schema)
    pq.write_table(table, out_dir / "data.parquet")

    print(f"[export] {domain:<20} topic={topic:<30} records={len(records):>6}")
    return len(records)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kafka-bootstrap", default="localhost:9092")
    parser.add_argument("--schema-registry-url", default="http://localhost:8081")
    parser.add_argument("--output-dir", default="./kafka_export")
    parser.add_argument("--domain", default=None, help="Export only this domain (debug)")
    args = parser.parse_args()

    contracts_dir = Path(__file__).resolve().parent.parent / "contracts"
    contract_files = (
        [contracts_dir / f"{args.domain}.yml"]
        if args.domain
        else sorted(contracts_dir.glob("*.yml"))
    )

    print("=" * 65)
    print(f"  sdd-kafka-databricks — Export Kafka → Volume ({len(contract_files)} domains)")
    print("=" * 65)

    total = 0
    for contract_path in contract_files:
        domain = contract_path.stem
        total += export_topic(domain, contract_path, args)

    print()
    print(f"[export] done — {len(contract_files)} domains, {total} records total")
    print(
        f"[export] next: databricks fs cp -r {args.output_dir} "
        "dbfs:/Volumes/<catalog>/landing/kafka_export"
    )


if __name__ == "__main__":
    main()
