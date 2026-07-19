#!/usr/bin/env bash
set -euo pipefail

# Usage: ./tests/benchmarking/run-benchmark.sh [LEVEL]
#   LEVEL: LIGHT | MEDIUM | HEAVY (default: MEDIUM)

LEVEL=${1:-MEDIUM}

printf "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
printf "  Relationship Write Benchmark\n"
printf "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"

printf "\n🔧 Starting Docker containers...\n"
docker-compose up -d --build --remove-orphans >/dev/null
printf "✅ Docker containers ready\n"

run_case_kv() {
  local bulk=$1
  local shape=$2
  docker-compose exec -T tests sh -lc "export DB_RELATIONSHIP_BULK_WRITES=${bulk} BENCH_KV=1; php tests/benchmarking/relationship_write_benchmark.php '${LEVEL}' '${shape}'"
}

percent() {
  # args: base opt
  awk -v b="$1" -v o="$2" 'BEGIN { if (b==0) {print 0} else { printf "%.1f", ((b-o)/b)*100 } }'
}

print_row() {
  local label=$1 base=$2 opt=$3
  printf "%-8s | %7dms | %7dms | %6.1f%%\n" "$label" "$base" "$opt" $(percent "$base" "$opt")
}

printf "\n▶ Baseline (bulk=OFF, shape=ids)\n"
BASE_IDS_KV=$(run_case_kv 0 ids)
printf "\n▶ Baseline (bulk=OFF, shape=docs)\n"
BASE_DOCS_KV=$(run_case_kv 0 docs)

printf "\n▶ Optimized (bulk=ON, shape=ids)\n"
OPT_IDS_KV=$(run_case_kv 1 ids)
printf "\n▶ Optimized (bulk=ON, shape=docs)\n"
OPT_DOCS_KV=$(run_case_kv 1 docs)

# Extract values
getv() { echo "$1" | grep -E "^$2=" | head -n1 | cut -d'=' -f2; }

BASE_IDS_M2M=$(getv "$BASE_IDS_KV" M2M); OPT_IDS_M2M=$(getv "$OPT_IDS_KV" M2M)
BASE_IDS_O2M=$(getv "$BASE_IDS_KV" O2M); OPT_IDS_O2M=$(getv "$OPT_IDS_KV" O2M)
BASE_IDS_O2O=$(getv "$BASE_IDS_KV" O2O); OPT_IDS_O2O=$(getv "$OPT_IDS_KV" O2O)
BASE_IDS_M2O=$(getv "$BASE_IDS_KV" M2O); OPT_IDS_M2O=$(getv "$OPT_IDS_KV" M2O)
BASE_IDS_TOTAL=$(getv "$BASE_IDS_KV" TOTAL); OPT_IDS_TOTAL=$(getv "$OPT_IDS_KV" TOTAL)
BASE_IDS_Q=$(getv "$BASE_IDS_KV" QUERIES_TOTAL); OPT_IDS_Q=$(getv "$OPT_IDS_KV" QUERIES_TOTAL)
BASE_IDS_SEL=$(getv "$BASE_IDS_KV" SELECT); OPT_IDS_SEL=$(getv "$OPT_IDS_KV" SELECT)
BASE_IDS_INS=$(getv "$BASE_IDS_KV" INSERT); OPT_IDS_INS=$(getv "$OPT_IDS_KV" INSERT)
BASE_IDS_UPD=$(getv "$BASE_IDS_KV" UPDATE); OPT_IDS_UPD=$(getv "$OPT_IDS_KV" UPDATE)
BASE_IDS_DEL=$(getv "$BASE_IDS_KV" DELETE); OPT_IDS_DEL=$(getv "$OPT_IDS_KV" DELETE)

BASE_DOCS_M2M=$(getv "$BASE_DOCS_KV" M2M); OPT_DOCS_M2M=$(getv "$OPT_DOCS_KV" M2M)
BASE_DOCS_O2M=$(getv "$BASE_DOCS_KV" O2M); OPT_DOCS_O2M=$(getv "$OPT_DOCS_KV" O2M)
BASE_DOCS_O2O=$(getv "$BASE_DOCS_KV" O2O); OPT_DOCS_O2O=$(getv "$OPT_DOCS_KV" O2O)
BASE_DOCS_M2O=$(getv "$BASE_DOCS_KV" M2O); OPT_DOCS_M2O=$(getv "$OPT_DOCS_KV" M2O)
BASE_DOCS_TOTAL=$(getv "$BASE_DOCS_KV" TOTAL); OPT_DOCS_TOTAL=$(getv "$OPT_DOCS_KV" TOTAL)
BASE_DOCS_Q=$(getv "$BASE_DOCS_KV" QUERIES_TOTAL); OPT_DOCS_Q=$(getv "$OPT_DOCS_KV" QUERIES_TOTAL)
BASE_DOCS_SEL=$(getv "$BASE_DOCS_KV" SELECT); OPT_DOCS_SEL=$(getv "$OPT_DOCS_KV" SELECT)
BASE_DOCS_INS=$(getv "$BASE_DOCS_KV" INSERT); OPT_DOCS_INS=$(getv "$OPT_DOCS_KV" INSERT)
BASE_DOCS_UPD=$(getv "$BASE_DOCS_KV" UPDATE); OPT_DOCS_UPD=$(getv "$OPT_DOCS_KV" UPDATE)
BASE_DOCS_DEL=$(getv "$BASE_DOCS_KV" DELETE); OPT_DOCS_DEL=$(getv "$OPT_DOCS_KV" DELETE)

printf "\n════════════════════════════════════════════════════════════════\n"
printf "                         RESULT SUMMARY                         \n"
printf "════════════════════════════════════════════════════════════════\n"
printf "\nTime (ms) — shape: ids\n"
printf "+--------+----------+----------+--------+\n"
printf "| Label  | Baseline | Optimized| Gain   |\n"
printf "+--------+----------+----------+--------+\n"
printf "| %-6s | %8d | %8d | %6.1f%% |\n" M2M "$BASE_IDS_M2M" "$OPT_IDS_M2M" $(percent "$BASE_IDS_M2M" "$OPT_IDS_M2M")
printf "| %-6s | %8d | %8d | %6.1f%% |\n" O2M "$BASE_IDS_O2M" "$OPT_IDS_O2M" $(percent "$BASE_IDS_O2M" "$OPT_IDS_O2M")
printf "| %-6s | %8d | %8d | %6.1f%% |\n" O2O "$BASE_IDS_O2O" "$OPT_IDS_O2O" $(percent "$BASE_IDS_O2O" "$OPT_IDS_O2O")
printf "| %-6s | %8d | %8d | %6.1f%% |\n" M2O "$BASE_IDS_M2O" "$OPT_IDS_M2O" $(percent "$BASE_IDS_M2O" "$OPT_IDS_M2O")
printf "+--------+----------+----------+--------+\n"
printf "| %-6s | %8d | %8d | %6.1f%% |\n" TOTAL "$BASE_IDS_TOTAL" "$OPT_IDS_TOTAL" $(percent "$BASE_IDS_TOTAL" "$OPT_IDS_TOTAL")
printf "+--------+----------+----------+--------+\n"

printf "\nQueries — shape: ids\n"
printf "+---------+----------+----------+\n"
printf "| Metric  | Baseline | Optimized|\n"
printf "+---------+----------+----------+\n"
printf "| %-7s | %8d | %8d |\n" total "$BASE_IDS_Q" "$OPT_IDS_Q"
printf "| %-7s | %8d | %8d |\n" select "$BASE_IDS_SEL" "$OPT_IDS_SEL"
printf "| %-7s | %8d | %8d |\n" insert "$BASE_IDS_INS" "$OPT_IDS_INS"
printf "| %-7s | %8d | %8d |\n" update "$BASE_IDS_UPD" "$OPT_IDS_UPD"
printf "| %-7s | %8d | %8d |\n" delete "$BASE_IDS_DEL" "$OPT_IDS_DEL"
printf "+---------+----------+----------+\n"

printf "\nTime (ms) — shape: docs\n"
printf "+--------+----------+----------+--------+\n"
printf "| Label  | Baseline | Optimized| Gain   |\n"
printf "+--------+----------+----------+--------+\n"
printf "| %-6s | %8d | %8d | %6.1f%% |\n" M2M "$BASE_DOCS_M2M" "$OPT_DOCS_M2M" $(percent "$BASE_DOCS_M2M" "$OPT_DOCS_M2M")
printf "| %-6s | %8d | %8d | %6.1f%% |\n" O2M "$BASE_DOCS_O2M" "$OPT_DOCS_O2M" $(percent "$BASE_DOCS_O2M" "$OPT_DOCS_O2M")
printf "| %-6s | %8d | %8d | %6.1f%% |\n" O2O "$BASE_DOCS_O2O" "$OPT_DOCS_O2O" $(percent "$BASE_DOCS_O2O" "$OPT_DOCS_O2O")
printf "| %-6s | %8d | %8d | %6.1f%% |\n" M2O "$BASE_DOCS_M2O" "$OPT_DOCS_M2O" $(percent "$BASE_DOCS_M2O" "$OPT_DOCS_M2O")
printf "+--------+----------+----------+--------+\n"
printf "| %-6s | %8d | %8d | %6.1f%% |\n" TOTAL "$BASE_DOCS_TOTAL" "$OPT_DOCS_TOTAL" $(percent "$BASE_DOCS_TOTAL" "$OPT_DOCS_TOTAL")
printf "+--------+----------+----------+--------+\n"

printf "\nQueries — shape: docs\n"
printf "+---------+----------+----------+\n"
printf "| Metric  | Baseline | Optimized|\n"
printf "+---------+----------+----------+\n"
printf "| %-7s | %8d | %8d |\n" total "$BASE_DOCS_Q" "$OPT_DOCS_Q"
printf "| %-7s | %8d | %8d |\n" select "$BASE_DOCS_SEL" "$OPT_DOCS_SEL"
printf "| %-7s | %8d | %8d |\n" insert "$BASE_DOCS_INS" "$OPT_DOCS_INS"
printf "| %-7s | %8d | %8d |\n" update "$BASE_DOCS_UPD" "$OPT_DOCS_UPD"
printf "| %-7s | %8d | %8d |\n" delete "$BASE_DOCS_DEL" "$OPT_DOCS_DEL"
printf "+---------+----------+----------+\n"

printf "\nTip: toggle DB_RELATIONSHIP_BULK_WRITES=0/1 to run a single mode.\n"

printf "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
printf "  Benchmark Complete\n"
printf "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
