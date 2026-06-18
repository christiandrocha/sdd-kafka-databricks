#!/bin/bash
# scripts/register_connectors.sh
# Registers the Debezium PostgreSQL CDC connector (the only connector this
# project needs — Databricks Structured Streaming reads Kafka directly,
# ADR-01, so there is no Sink Connector here).
# Usage: ./scripts/register_connectors.sh [--env local|prod]
set -euo pipefail

ENV_FILE=".env"
CONNECT_URL="http://localhost:8083"
REGISTRY_URL="http://localhost:8081"
CONNECTORS_DIR="$(dirname "$0")/../connectors"

GREEN="\033[92m"; YELLOW="\033[93m"; RED="\033[91m"
CYAN="\033[96m"; GRAY="\033[90m"; RESET="\033[0m"

while [[ $# -gt 0 ]]; do
    case $1 in
        --env) ENV_FILE=".env.${2}"; shift 2 ;;
        *) shift ;;
    esac
done

if [ ! -f "$ENV_FILE" ]; then
    echo -e "${RED}✖   $ENV_FILE not found. Copy .env.example to .env and fill values.${RESET}"
    exit 1
fi
set -a; source "$ENV_FILE"; set +a
echo -e "${GREEN}✅  Loaded credentials from ${ENV_FILE}${RESET}"

echo -e "\n${CYAN}══════════════════════════════════════════════════════════${RESET}"
echo -e "${CYAN}  sdd-kafka-databricks — Register Connectors${RESET}"
echo -e "${CYAN}  20 domains | 1 connector (debezium-postgres-cdc)${RESET}"
echo -e "${CYAN}══════════════════════════════════════════════════════════${RESET}"

# ── Wait for Schema Registry ──────────────────────────────────────────────
echo -e "\n${YELLOW}⏳  Waiting for Schema Registry...${RESET}"
for i in $(seq 1 30); do
    if curl -sf "${REGISTRY_URL}/subjects" > /dev/null 2>&1; then
        echo -e "${GREEN}✅  Schema Registry ready (attempt ${i})${RESET}"; break
    fi
    [ "$i" -eq 30 ] && echo -e "${RED}✖   Timeout: Schema Registry${RESET}" && exit 1
    printf "${GRAY}    waiting... %d/30\r${RESET}" "$i"; sleep 5
done

# ── Set BACKWARD compatibility ───────────────────────────────────────────
# Replaces a dedicated set_compatibility.sh — the only action it would do
# (PUT /config) fits in 4 lines here (YAGNI, see DESIGN Decision/Out of Scope).
echo -e "\n${YELLOW}🔒  Setting global BACKWARD compatibility...${RESET}"
curl -sf -X PUT "${REGISTRY_URL}/config" \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d '{"compatibility": "BACKWARD"}' > /dev/null
echo -e "${GREEN}✅  Compatibility: BACKWARD${RESET}"

# ── Wait for Kafka Connect ───────────────────────────────────────────────
echo -e "\n${YELLOW}⏳  Waiting for Kafka Connect...${RESET}"
for i in $(seq 1 40); do
    if curl -sf "${CONNECT_URL}/connectors" > /dev/null 2>&1; then
        echo -e "${GREEN}✅  Kafka Connect ready (attempt ${i})${RESET}"; break
    fi
    [ "$i" -eq 40 ] && echo -e "${RED}✖   Timeout: Kafka Connect${RESET}" && exit 1
    printf "${GRAY}    waiting... %d/40\r${RESET}" "$i"; sleep 5
done

# ── Register connector ───────────────────────────────────────────────────
register_connector() {
    local name="$1" file="$2"
    echo -e "\n${YELLOW}📡  Registering: ${name}${RESET}"

    RESOLVED=$(envsubst < "$file")

    # No -f here: 409 (already exists) is an expected, handled outcome below,
    # not a script failure — -f would make curl exit non-zero on it and abort
    # the script under set -e before the case statement runs.
    HTTP=$(echo "$RESOLVED" | curl -s -o /tmp/connect_resp.json -w "%{http_code}" \
        -X POST "${CONNECT_URL}/connectors" \
        -H "Content-Type: application/json" -d @-)

    case "$HTTP" in
        201) echo -e "${GREEN}✅  ${name} created (HTTP 201)${RESET}" ;;
        409) echo -e "${YELLOW}⚠️   ${name} already exists (HTTP 409)${RESET}" ;;
        *)   echo -e "${RED}✖   Failed ${name} (HTTP ${HTTP})${RESET}"
             cat /tmp/connect_resp.json 2>/dev/null; exit 1 ;;
    esac
}

register_connector "debezium-postgres-cdc" "${CONNECTORS_DIR}/debezium.json"

# ── Status check ──────────────────────────────────────────────────────────
echo -e "\n${YELLOW}⏳  Waiting for connector to stabilize (15s)...${RESET}"
sleep 15

echo -e "\n${CYAN}── Connector status ──────────────────────────────────────${RESET}"
STATUS=$(curl -sf "${CONNECT_URL}/connectors/debezium-postgres-cdc/status" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['connector']['state'])" 2>/dev/null || echo "UNKNOWN")
[ "$STATUS" = "RUNNING" ] \
    && echo -e "  ${GREEN}✅  debezium-postgres-cdc: ${STATUS}${RESET}" \
    || echo -e "  ${RED}✖   debezium-postgres-cdc: ${STATUS}${RESET}"

echo -e "\n${CYAN}══════════════════════════════════════════════════════════${RESET}"
echo -e "${GREEN}  Connector registered! (20 domains → 1 connector → Kafka)${RESET}\n"
echo -e "  ${GRAY}Kafka UI    →${RESET} http://localhost:8080"
echo -e "  ${GRAY}Connect     →${RESET} http://localhost:8083/connectors"
echo -e "  ${GRAY}Registry    →${RESET} http://localhost:8081/subjects"
echo -e "${CYAN}══════════════════════════════════════════════════════════${RESET}\n"
