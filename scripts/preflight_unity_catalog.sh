#!/bin/bash
# scripts/preflight_unity_catalog.sh
# Provisions the Unity Catalog objects that nothing else in this repo creates:
# the Catalog, the 4 data schemas (bronze/silver/gold/quarantine), a 5th
# operational schema `checkpoints` holding the 2 Volumes (bronze/silver) used
# as Structured Streaming checkpoint locations, and a 6th schema `landing`
# holding the `kafka_export` Volume used by Bronze's source_mode=volume path
# (Free Edition — see DESIGN_FREE_EDITION_BRONZE.md Decision 2).
# /Volumes/<catalog>/<schema>/<volume> is always 3 levels; checkpoint_base and
# landing_base in databricks.yml already assume this layout.
#
# Run once per target, before the first `databricks bundle deploy`:
#   ./scripts/preflight_unity_catalog.sh --target dev
#   ./scripts/preflight_unity_catalog.sh --target prod
#
# Idempotent: every resource is checked with a `get`/`read` before `create`.
set -euo pipefail

GREEN="\033[92m"; YELLOW="\033[93m"; RED="\033[91m"; CYAN="\033[96m"; RESET="\033[0m"

TARGET="dev"
while [[ $# -gt 0 ]]; do
    case $1 in
        --target) TARGET="${2:?--target requires dev or prod}"; shift 2 ;;
        *) shift ;;
    esac
done

if [[ "$TARGET" != "dev" && "$TARGET" != "prod" ]]; then
    echo -e "${RED}✖   --target must be 'dev' or 'prod' (got '${TARGET}')${RESET}"
    exit 1
fi

CATALOG="ubereats_${TARGET}"
DATA_SCHEMAS=(bronze silver gold quarantine)
CHECKPOINT_VOLUMES=(bronze silver)

echo -e "${CYAN}══════════════════════════════════════════════════════════${RESET}"
echo -e "${CYAN}  Unity Catalog pre-flight — target=${TARGET} catalog=${CATALOG}${RESET}"
echo -e "${CYAN}══════════════════════════════════════════════════════════${RESET}"

# ── Auth check — fail fast and clearly, no silent retry (Define A-002) ────
echo -e "\n${YELLOW}🔑  Checking Databricks CLI authentication...${RESET}"
if ! databricks current-user me > /dev/null 2>&1; then
    echo -e "${RED}✖   Databricks CLI not authenticated. Run: databricks auth login${RESET}"
    exit 1
fi
echo -e "${GREEN}✅  Authenticated${RESET}"

ensure_catalog() {
    if databricks catalogs get "$CATALOG" > /dev/null 2>&1; then
        echo -e "  ${YELLOW}⚠️   catalog ${CATALOG} already exists${RESET}"
    else
        databricks catalogs create "$CATALOG" > /dev/null
        echo -e "  ${GREEN}✅  catalog ${CATALOG} created${RESET}"
    fi
}

ensure_schema() {
    local schema="$1"
    if databricks schemas get "${CATALOG}.${schema}" > /dev/null 2>&1; then
        echo -e "  ${YELLOW}⚠️   schema ${CATALOG}.${schema} already exists${RESET}"
    else
        databricks schemas create "$schema" "$CATALOG" > /dev/null
        echo -e "  ${GREEN}✅  schema ${CATALOG}.${schema} created${RESET}"
    fi
}

ensure_volume() {
    local schema="$1" volume="$2"
    if databricks volumes read "${CATALOG}.${schema}.${volume}" > /dev/null 2>&1; then
        echo -e "  ${YELLOW}⚠️   volume ${CATALOG}.${schema}.${volume} already exists${RESET}"
    else
        databricks volumes create "$CATALOG" "$schema" "$volume" MANAGED > /dev/null
        echo -e "  ${GREEN}✅  volume ${CATALOG}.${schema}.${volume} created${RESET}"
    fi
}

echo -e "\n${YELLOW}📦  Catalog${RESET}"
ensure_catalog

echo -e "\n${YELLOW}📁  Data schemas (bronze/silver/gold/quarantine)${RESET}"
for schema in "${DATA_SCHEMAS[@]}"; do ensure_schema "$schema"; done

echo -e "\n${YELLOW}📁  Checkpoints schema${RESET}"
ensure_schema "checkpoints"

echo -e "\n${YELLOW}🗂️   Checkpoint volumes (Structured Streaming — Bronze/Silver only, Gold is batch)${RESET}"
for volume in "${CHECKPOINT_VOLUMES[@]}"; do ensure_volume "checkpoints" "$volume"; done

echo -e "\n${YELLOW}📁  Landing schema (Kafka export → Volume, source_mode=volume)${RESET}"
ensure_schema "landing"

echo -e "\n${YELLOW}🗂️   Landing volume${RESET}"
ensure_volume "landing" "kafka_export"

echo -e "\n${CYAN}══════════════════════════════════════════════════════════${RESET}"
echo -e "${GREEN}  ${CATALOG} ready: 4 data schemas + checkpoints/{bronze,silver} + landing/kafka_export${RESET}"
echo -e "  ${GREEN}→ safe to run: databricks bundle deploy --target ${TARGET}${RESET}"
echo -e "${CYAN}══════════════════════════════════════════════════════════${RESET}\n"
