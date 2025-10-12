#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./tests/benchmarking/run-processor-benchmark.sh [LEVEL] [--assert] [--repeat=N] [--warmup=N]
# Levels:
#   LIGHT | MEDIUM | HEAVY | SPATIAL (default: MEDIUM)

LEVEL=${1:-MEDIUM}
shift || true

printf "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
printf "  Document Processor Benchmark\n"
printf "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"

printf "\n🔧 Starting Docker containers...\n"
docker-compose up -d --build --remove-orphans >/dev/null
printf "✅ Docker containers ready\n"

# Pass through any extra flags to the PHP benchmark script
docker-compose exec -T tests php tests/benchmarking/document_processor_benchmark.php "${LEVEL}" "$@"

printf "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
printf "  Benchmark Complete\n"
printf "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
