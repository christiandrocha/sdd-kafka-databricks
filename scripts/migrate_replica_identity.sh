#!/bin/bash
# scripts/migrate_replica_identity.sh
# One-time migration for Postgres instances created BEFORE
# DESIGN_DELETE_HANDLING.md shipped — sql/init.sql now sets REPLICA IDENTITY
# FULL on every table at creation time, but docker-entrypoint-initdb.d only
# runs on a fresh volume. Run this once against any already-running stack
# (idempotent — ALTER TABLE ... REPLICA IDENTITY FULL is safe to re-run).
# Usage: ./scripts/migrate_replica_identity.sh
set -euo pipefail

ENV_FILE=".env"
GREEN="\033[92m"; YELLOW="\033[93m"; RED="\033[91m"; RESET="\033[0m"

if [ ! -f "$ENV_FILE" ]; then
    echo -e "${RED}✖   $ENV_FILE not found. Copy .env.example to .env and fill values.${RESET}"
    exit 1
fi
set -a; source "$ENV_FILE"; set +a

TABLES=(
    payment_events orders payments order_items gps_events order_status
    routes receipts driver_shifts search_events recommendations
    support_tickets users_mongo users_mssql restaurants drivers products
    menu_sections ratings inventory
)

echo -e "${YELLOW}⏳  Setting REPLICA IDENTITY FULL on ${#TABLES[@]} tables...${RESET}"
for table in "${TABLES[@]}"; do
    psql "$DATABASE_URL" -c "ALTER TABLE ${table} REPLICA IDENTITY FULL;" > /dev/null
done
echo -e "${GREEN}✅  Done. Verify with: psql \"\$DATABASE_URL\" -c \"SELECT relname, relreplident FROM pg_class WHERE relname = ANY(ARRAY[$(printf "'%s'," "${TABLES[@]}" | sed 's/,$//')]);\"${RESET}"
echo -e "${GREEN}    relreplident should be 'f' (FULL) for every row, not 'd' (DEFAULT).${RESET}"
