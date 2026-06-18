#!/usr/bin/env python3
"""
load_to_postgres.py
Reads Uber Eats JSON export files and inserts into the correct
PostgreSQL table based on the filename prefix.

Usage:
    # Dry-run (validate without inserting)
    python3 load_to_postgres.py --data-dir tests/data/ --dry-run

    # Load 80% initial batch (sorted by filename, first 80 files)
    python3 load_to_postgres.py --data-dir tests/data/ --batch initial

    # Load 20% incremental batch (last 20 files)
    python3 load_to_postgres.py --data-dir tests/data/ --batch incremental

    # Load all files
    python3 load_to_postgres.py --data-dir tests/data/ --batch all

    # Load specific domain only
    python3 load_to_postgres.py --data-dir tests/data/ --domain kafka_orders

Requirements:
    pip install psycopg2-binary
"""

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Helpers ───────────────────────────────────────────────────────────────────

def _driver_key_to_int(key: str | None, n_drivers: int = 354) -> str | None:
    """Maps alphanumeric driver_key (e.g. 'wr2179397') to an integer string
    in range 1..n_drivers so it joins against drivers.driver_id."""
    if not key:
        return None
    digits = "".join(c for c in key if c.isdigit())
    return str(int(digits or "0") % n_drivers + 1)


# ── Domain → table mapping ────────────────────────────────────────────────────

DOMAIN_CONFIG = {
    "kafka_events": {
        "table": "payment_events",
        "pk": "event_id",
        "columns": ["event_id", "payment_id", "event", "dt_current_timestamp"],
        "transform": lambda r: {
            "event_id": r["event_id"],
            "payment_id": r["payment_id"],
            "event": json.dumps(r["event"]),
            "dt_current_timestamp": r["dt_current_timestamp"],
        },
    },
    "kafka_orders": {
        "table": "orders",
        "pk": "order_id",
        "columns": ["order_id", "order_date", "total_amount", "user_key",
                    "restaurant_key", "driver_key", "payment_key",
                    "rating_key", "dt_current_timestamp"],
        "transform": lambda r: {
            "order_id": r["order_id"],
            "order_date": r.get("order_date"),
            "total_amount": r.get("total_amount"),
            "user_key": r.get("user_key"),
            "restaurant_key": r.get("restaurant_key"),
            "driver_key": _driver_key_to_int(r.get("driver_key")),
            "payment_key": r.get("payment_key"),
            "rating_key": r.get("rating_key"),
            "dt_current_timestamp": r.get("dt_current_timestamp"),
        },
    },
    "kafka_payments": {
        "table": "payments",
        "pk": "payment_id",
        "columns": ["payment_id", "invoice_id", "order_key", "method", "provider",
                    "status", "amount", "net_amount", "tax_amount", "platform_fee",
                    "provider_fee", "refund_amount", "currency", "country",
                    "captured", "refunded", "card_brand", "card_last4",
                    "card_exp_month", "card_exp_year", "wallet_provider",
                    "failure_reason", "receipt_url", "ip_address", "user_agent",
                    "timestamp", "capture_timestamp", "dt_current_timestamp"],
        "transform": lambda r: {k: r.get(k) for k in [
            "payment_id", "invoice_id", "method", "provider", "status",
            "amount", "net_amount", "tax_amount", "platform_fee", "provider_fee",
            "refund_amount", "currency", "country", "captured", "refunded",
            "card_brand", "card_last4", "card_exp_month", "card_exp_year",
            "wallet_provider", "failure_reason", "receipt_url", "ip_address",
            "user_agent", "timestamp", "capture_timestamp", "dt_current_timestamp",
        ]} | {"order_key": r.get("order_key")},
    },
    "mongodb_items": {
        "table": "order_items",
        "pk": "order_item_id",
        "columns": ["order_item_id", "order_id", "product_id", "restaurant_id",
                    "product_name", "product_type", "cuisine_type", "unit_price",
                    "quantity", "subtotal", "discount_applied", "modifiers",
                    "is_combo", "is_vegetarian", "dt_current_timestamp"],
        "transform": lambda r: {k: r.get(k) for k in [
            "order_item_id", "order_id", "product_id", "restaurant_id",
            "product_name", "product_type", "cuisine_type", "unit_price",
            "quantity", "subtotal", "discount_applied", "modifiers",
            "is_combo", "is_vegetarian", "dt_current_timestamp",
        ]},
    },
    "kafka_gps": {
        "table": "gps_events",
        "pk": "gps_id",
        "columns": ["gps_id", "order_id", "lat", "lon", "altitude", "speed_kph",
                    "direction_deg", "accuracy_m", "duration_ms",
                    "timestamp", "dt_current_timestamp"],
        "transform": lambda r: {k: r.get(k) for k in [
            "gps_id", "order_id", "lat", "lon", "altitude", "speed_kph",
            "direction_deg", "accuracy_m", "duration_ms",
            "timestamp", "dt_current_timestamp",
        ]},
    },
    "kafka_status": {
        "table": "order_status",
        "pk": "status_id",
        "columns": ["status_id", "order_identifier", "status", "dt_current_timestamp"],
        "transform": lambda r: {
            "status_id": r["status_id"],
            "order_identifier": r.get("order_identifier"),
            "status": json.dumps(r["status"]),
            "dt_current_timestamp": r.get("dt_current_timestamp"),
        },
    },
    "kafka_route": {
        "table": "routes",
        "pk": "route_id",
        "columns": ["route_id", "order_id", "driver_id", "start_lat", "start_lon",
                    "end_lat", "end_lon", "distance_km", "estimated_duration_min",
                    "start_time", "end_time", "dt_current_timestamp"],
        "transform": lambda r: {k: r.get(k) for k in [
            "route_id", "order_id", "driver_id", "start_lat", "start_lon",
            "end_lat", "end_lon", "distance_km", "estimated_duration_min",
            "start_time", "end_time", "dt_current_timestamp",
        ]},
    },
    "kafka_receipts": {
        "table": "receipts",
        "pk": "receipt_id",
        "columns": ["receipt_id", "order_id", "payment_id", "total_amount",
                    "item_count", "receipt_generated_at"],
        "transform": lambda r: {k: r.get(k) for k in [
            "receipt_id", "order_id", "payment_id", "total_amount",
            "item_count", "receipt_generated_at",
        ]},
    },
    "kafka_shift": {
        "table": "driver_shifts",
        "pk": "shift_id",
        "columns": ["shift_id", "driver_id", "city", "region", "shift_type",
                    "login_method", "device_os", "start_time", "end_time",
                    "shift_duration_min", "num_orders", "distance_covered_km",
                    "earnings_brl", "shift_rating", "issues_reported",
                    "available", "dt_current_timestamp"],
        "transform": lambda r: {k: r.get(k) for k in [
            "shift_id", "driver_id", "city", "region", "shift_type",
            "login_method", "device_os", "start_time", "end_time",
            "shift_duration_min", "num_orders", "distance_covered_km",
            "earnings_brl", "shift_rating", "issues_reported",
            "available", "dt_current_timestamp",
        ]},
    },
    "kafka_search": {
        "table": "search_events",
        "pk": "search_id",
        "columns": ["search_id", "user_id", "query_text", "filters",
                    "result_count", "clicked_product_id", "timestamp"],
        "transform": lambda r: {k: r.get(k) for k in [
            "search_id", "user_id", "query_text", "filters",
            "result_count", "clicked_product_id", "timestamp",
        ]},
    },
    "mongodb_recommendations": {
        "table": "recommendations",
        "pk": "event_id",
        "columns": ["event_id", "user_id", "product_id", "event_type",
                    "timestamp", "dt_current_timestamp"],
        "transform": lambda r: {k: r.get(k) for k in [
            "event_id", "user_id", "product_id", "event_type",
            "timestamp", "dt_current_timestamp",
        ]},
    },
    "mongodb_support": {
        "table": "support_tickets",
        "pk": "ticket_id",
        "columns": ["ticket_id", "user_id", "order_id", "category",
                    "description", "status", "opened_at", "dt_current_timestamp"],
        "transform": lambda r: {k: r.get(k) for k in [
            "ticket_id", "user_id", "order_id", "category",
            "description", "status", "opened_at", "dt_current_timestamp",
        ]},
    },
    "mongodb_users": {
        "table": "users_mongo",
        "pk": "uuid",
        "columns": ["uuid", "user_id", "cpf", "email", "phone_number",
                    "city", "country", "delivery_address", "dt_current_timestamp"],
        "transform": lambda r: {k: r.get(k) for k in [
            "uuid", "user_id", "cpf", "email", "phone_number",
            "city", "country", "delivery_address", "dt_current_timestamp",
        ]},
    },
    "mssql_users": {
        "table": "users_mssql",
        "pk": "uuid",
        "columns": ["uuid", "user_id", "cpf", "first_name", "last_name",
                    "phone_number", "birthday", "job", "company_name",
                    "country", "dt_current_timestamp"],
        "transform": lambda r: {k: r.get(k) for k in [
            "uuid", "user_id", "cpf", "first_name", "last_name",
            "phone_number", "birthday", "job", "company_name",
            "country", "dt_current_timestamp",
        ]},
    },
    "mysql_restaurants": {
        "table": "restaurants",
        "pk": "uuid",
        "columns": ["uuid", "restaurant_id", "cnpj", "name", "address",
                    "city", "country", "phone_number", "cuisine_type",
                    "opening_time", "closing_time", "average_rating",
                    "num_reviews", "dt_current_timestamp"],
        "transform": lambda r: {k: r.get(k) for k in [
            "uuid", "restaurant_id", "cnpj", "name", "address",
            "city", "country", "phone_number", "cuisine_type",
            "opening_time", "closing_time", "average_rating",
            "num_reviews", "dt_current_timestamp",
        ]},
    },
    "postgres_drivers": {
        "table": "drivers",
        "pk": "uuid",
        "columns": ["uuid", "driver_id", "first_name", "last_name",
                    "phone_number", "city", "country", "date_birth",
                    "license_number", "vehicle_type", "vehicle_make",
                    "vehicle_model", "vehicle_year", "dt_current_timestamp"],
        "transform": lambda r: {k: r.get(k) for k in [
            "uuid", "driver_id", "first_name", "last_name",
            "phone_number", "city", "country", "date_birth",
            "license_number", "vehicle_type", "vehicle_make",
            "vehicle_model", "vehicle_year", "dt_current_timestamp",
        ]},
    },
    "mysql_products": {
        "table": "products",
        "pk": "product_id",
        "columns": ["product_id", "restaurant_id", "name", "product_type",
                    "cuisine_type", "flavor_profile", "tags", "price",
                    "unit_cost", "calories", "prep_time_min", "is_vegetarian",
                    "is_gluten_free", "created_at", "updated_at", "dt_current_timestamp"],
        "transform": lambda r: {k: r.get(k) for k in [
            "product_id", "restaurant_id", "name", "product_type",
            "cuisine_type", "flavor_profile", "tags", "price",
            "unit_cost", "calories", "prep_time_min", "is_vegetarian",
            "is_gluten_free", "created_at", "updated_at", "dt_current_timestamp",
        ]},
    },
    "mysql_menu": {
        "table": "menu_sections",
        "pk": "menu_section_id",
        "columns": ["menu_section_id", "restaurant_id", "name",
                    "description", "active", "dt_current_timestamp"],
        "transform": lambda r: {k: r.get(k) for k in [
            "menu_section_id", "restaurant_id", "name",
            "description", "active", "dt_current_timestamp",
        ]},
    },
    "mysql_ratings": {
        "table": "ratings",
        "pk": "rating_id",
        "columns": ["rating_id", "uuid", "restaurant_identifier",
                    "rating", "timestamp", "dt_current_timestamp"],
        "transform": lambda r: {k: r.get(k) for k in [
            "rating_id", "uuid", "restaurant_identifier",
            "rating", "timestamp", "dt_current_timestamp",
        ]},
    },
    "postgres_inventory": {
        "table": "inventory",
        "pk": "stock_id",
        "columns": ["stock_id", "restaurant_id", "product_id",
                    "quantity_available", "last_updated"],
        "transform": lambda r: {k: r.get(k) for k in [
            "stock_id", "restaurant_id", "product_id",
            "quantity_available", "last_updated",
        ]},
    },
}


# ── File handling ─────────────────────────────────────────────────────────────

def get_domain(filename: str) -> str | None:
    """Extracts domain prefix from filename."""
    parts = filename.split("_")
    # Try 2-word prefix first (kafka_orders, mysql_menu, etc.)
    if len(parts) >= 2:
        prefix2 = f"{parts[0]}_{parts[1]}"
        if prefix2 in DOMAIN_CONFIG:
            return prefix2
    return None


def load_file(path: Path) -> list[dict]:
    """Loads JSONL file. Returns list of records."""
    records = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def select_files(data_dir: Path, batch: str, domain_filter: str | None) -> list[Path]:
    """Returns files to process based on batch mode and domain filter."""
    all_files = sorted([
        f for f in data_dir.iterdir()
        if f.suffix.lower() == ".json" and f.is_file()
        and get_domain(f.name) is not None
    ])

    if domain_filter:
        all_files = [f for f in all_files if get_domain(f.name) == domain_filter]

    if batch == "initial":
        cutoff = int(len(all_files) * 0.8)
        return all_files[:cutoff]
    elif batch == "incremental":
        cutoff = int(len(all_files) * 0.8)
        return all_files[cutoff:]
    else:  # all
        return all_files


# ── PostgreSQL ────────────────────────────────────────────────────────────────

def build_upsert_sql(table: str, columns: list[str], pk: str) -> str:
    cols = ", ".join(columns)
    placeholders = ", ".join([f"%({c})s" for c in columns])
    updates = ", ".join([f"{c} = EXCLUDED.{c}" for c in columns if c != pk])
    return f"""
        INSERT INTO {table} ({cols})
        VALUES ({placeholders})
        ON CONFLICT ({pk}) DO UPDATE SET {updates}
    """


def insert_batch(conn, sql: str, records: list[dict]) -> int:
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, records, page_size=500)
        count = cur.rowcount
    conn.commit()
    return count


# ── Main ──────────────────────────────────────────────────────────────────────

def run(data_dir: Path, batch: str, domain_filter: str | None,
        dry_run: bool, db_url: str) -> dict:

    files = select_files(data_dir, batch, domain_filter)

    if not files:
        log.error("No files found matching criteria.")
        sys.exit(1)

    # Group unknown files
    unknown = [
        f for f in data_dir.iterdir()
        if f.suffix == ".json" and get_domain(f.name) is None
    ]

    log.info("")
    log.info("═" * 65)
    log.info("  sdd-kafka-snowflake — Load to PostgreSQL")
    log.info("═" * 65)
    log.info(f"  Data dir   : {data_dir}")
    log.info(f"  Batch      : {batch}")
    log.info(f"  Files      : {len(files)} to process")
    if unknown:
        log.info(f"  Skipped    : {len(unknown)} unrecognized files")
    log.info(f"  Dry run    : {dry_run}")
    log.info("═" * 65)

    conn = None if dry_run else psycopg2.connect(db_url)

    # Pre-build SQL per domain
    sql_cache = {}
    for domain, cfg in DOMAIN_CONFIG.items():
        sql_cache[domain] = build_upsert_sql(cfg["table"], cfg["columns"], cfg["pk"])

    stats = {"files": 0, "records": 0, "inserted": 0, "errors": 0}
    domain_stats: dict[str, int] = {}
    start = time.time()

    for path in files:
        domain = get_domain(path.name)
        cfg = DOMAIN_CONFIG[domain]
        stats["files"] += 1

        try:
            raw_records = load_file(path)
            transformed = []
            for r in raw_records:
                try:
                    transformed.append(cfg["transform"](r))
                except Exception as e:
                    log.warning(f"  Transform error in {path.name}: {e}")
                    stats["errors"] += 1

            stats["records"] += len(transformed)
            domain_stats[domain] = domain_stats.get(domain, 0) + len(transformed)

            if not dry_run and transformed:
                insert_batch(conn, sql_cache[domain], transformed)
                stats["inserted"] += len(transformed)

            log.info(f"  ✅ {path.name[:60]:<60} "
                     f"→ {cfg['table']:<20} {len(transformed):>6} records")

        except Exception as e:
            log.error(f"  ✖  {path.name}: {e}")
            stats["errors"] += 1

    if conn:
        conn.close()

    elapsed = time.time() - start

    log.info("")
    log.info("═" * 65)
    log.info("  Summary")
    log.info("═" * 65)
    log.info(f"  Files processed : {stats['files']}")
    log.info(f"  Records parsed  : {stats['records']:,}")
    if not dry_run:
        log.info(f"  Records inserted: {stats['inserted']:,}")
    log.info(f"  Errors          : {stats['errors']}")
    log.info(f"  Time            : {elapsed:.1f}s")
    log.info("")
    log.info("  By domain:")
    for domain, count in sorted(domain_stats.items(), key=lambda x: -x[1]):
        table = DOMAIN_CONFIG[domain]["table"]
        log.info(f"    {domain:<30} → {table:<22} {count:>8,}")
    log.info("═" * 65)

    if not dry_run:
        log.info("")
        log.info("  Next steps:")
        log.info("  1. Kafka UI  → http://localhost:8080 → topics with new messages")
        log.info("  2. Grafana   → http://localhost:3001 → consumer lag rising")
        log.info("  3. Dagster   → http://localhost:3000 → assets materializing")
        log.info("")

    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load Uber Eats JSON exports into PostgreSQL"
    )
    parser.add_argument("--data-dir", required=True, type=Path,
                        help="Directory with JSON files")
    parser.add_argument("--batch",
                        choices=["initial", "incremental", "all"],
                        default="all",
                        help="initial=80%%, incremental=20%%, all=100%% (default: all)")
    parser.add_argument("--domain",
                        help="Process only this domain (e.g. kafka_orders)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Validate without inserting")
    parser.add_argument("--db-url",
                        default=os.getenv("DATABASE_URL"),
                        help="PostgreSQL URL (default: DATABASE_URL env var)")
    args = parser.parse_args()

    if not args.data_dir.is_dir():
        log.error(f"Data directory not found: {args.data_dir}")
        sys.exit(1)

    if not args.dry_run and not args.db_url:
        log.error("No DB URL. Set DATABASE_URL or use --db-url.")
        sys.exit(1)

    result = run(
        data_dir=args.data_dir,
        batch=args.batch,
        domain_filter=args.domain,
        dry_run=args.dry_run,
        db_url=args.db_url,
    )
    sys.exit(1 if result["errors"] > 0 else 0)
