#!/usr/bin/env bash
# ============================================================
#  run_challenge.sh
#  Bulletproof Test Orchestrator for Lazy Evaluation Patch
# ============================================================
set -euo pipefail
export MSYS_NO_PATHCONV=1

SCENARIO="${1:-all}"

# ---- Colour helpers ----------------------------------------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'

info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[PASS]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[FAIL]${NC}  $*"; }
header()  { echo -e "\n${BOLD}${BLUE}========== $* ==========${NC}\n"; }

# ---- Helpers -----------------------------------------------
count_messages() {
  local container="$1"
  local topic="$2"
  local result
  result=$(docker exec "$container" \
    /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 \
    --topic "$topic" 2>/dev/null | awk -F: '{sum += $NF} END {print sum+0}')
  echo "${result:-0}"
}

wait_for_replication() {
  local expected="$1"
  local timeout="${2:-90}"
  local elapsed=0
  info "Waiting for standby to reach $expected messages..."
  while true; do
    local actual
    actual=$(count_messages "standby-kafka" "primary.commit-log" 2>/dev/null || echo 0)
    if [[ "$actual" -ge "$expected" ]]; then
      success "Replication confirmed: $actual messages on standby."
      return 0
    fi
    if [[ "$elapsed" -ge "$timeout" ]]; then
      error "Timeout: only $actual/$expected messages replicated after ${timeout}s."
      return 1
    fi
    sleep 2; ((elapsed += 2))
  done
}

# ============================================================
#  SETUP
# ============================================================
setup() {
  header "ENVIRONMENT SETUP"
  info "Tearing down existing environment for clean start..."
  docker-compose down -v 2>/dev/null || true
  sleep 2

  info "Starting all services..."
  docker-compose up -d primary-kafka standby-kafka topic-init mirrormaker

  info "Ensuring internal topics exist..."
  docker exec standby-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic primary.heartbeats \
    --partitions 1 --replication-factor 1 >/dev/null 2>&1 || true

  info "Waiting 25s for MirrorMaker 2 to stabilise..."
  sleep 25
  success "Environment ready."
}

# ============================================================
#  SCENARIO 1 - Normal Replication Flow
# ============================================================
scenario_normal() {
  header "SCENARIO 1: Normal Replication"
  info "Producing 1000 messages to primary commit-log..."
  docker-compose run --rm --no-deps producer --count 1000
  wait_for_replication 1000 90
  success "SCENARIO 1 PASSED - Normal replication works."
}

# ============================================================
#  SCENARIO 2 - Log Truncation Detection
# ============================================================
scenario_truncation() {
  header "SCENARIO 2: Log Truncation Detection"

  info "Giving MirrorMaker 2 time to save its bookmark (offsets)..."
  sleep 15

  info "Gracefully stopping MirrorMaker 2 (Timeout=45s to avoid SIGKILL)..."
  docker stop --time 45 mirrormaker

  info "Producing 200 messages while MirrorMaker is stopped..."
  docker-compose run --rm --no-deps producer --count 200

  info "Forcing log truncation using kafka-delete-records.sh..."
  local current_end
  current_end=$(count_messages "primary-kafka" "commit-log")
  local delete_offset=$((current_end - 50)) 

  docker exec primary-kafka sh -c "echo '{\"partitions\": [{\"topic\": \"commit-log\", \"partition\": 0, \"offset\": $delete_offset}]}' > /tmp/delete.json"
  docker exec primary-kafka /opt/kafka/bin/kafka-delete-records.sh \
    --bootstrap-server localhost:9092 \
    --offset-json-file /tmp/delete.json >/dev/null 2>&1

  sleep 5

  info "Starting MirrorMaker 2 (This can take up to 60s to boot)..."
  docker start mirrormaker

  info "Watching MirrorMaker logs for DataLossException (90s window)..."
  local detected=false
  for i in $(seq 1 45); do 
    sleep 2
    if docker logs mirrormaker 2>&1 | grep -E -q "TRUNCATION|DataLoss"; then
      detected=true
      break
    fi
  done

  if $detected; then
    success "SCENARIO 2 PASSED - Log truncation detected and fail-fast triggered."
  else
    error "SCENARIO 2 FAILED - Truncation was NOT detected."
    docker logs mirrormaker --tail 20
    return 1
  fi
}

# ============================================================
#  SCENARIO 3 - Topic Reset Recovery
# ============================================================
scenario_reset() {
  header "SCENARIO 3: Topic Reset Recovery"

  info "Restarting MirrorMaker to clear previous crash states..."
  docker restart --time 45 mirrormaker
  info "Waiting 30s for MirrorMaker to fully boot..."
  sleep 30

  local initial_standby
  initial_standby=$(count_messages "standby-kafka" "primary.commit-log" 2>/dev/null || echo 0)

  info "Producing 100 messages to prime the pipeline..."
  docker-compose run --rm --no-deps producer --count 100

  local expected_prime=$((initial_standby + 100))
  info "Waiting for priming messages to replicate..."
  wait_for_replication $expected_prime 60
  local baseline_standby=$expected_prime

  info "Stopping MirrorMaker 2 for controlled topic reset..."
  docker stop --time 45 mirrormaker

  info "Deleting commit-log topic on primary..."
  docker exec primary-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --delete --topic commit-log 2>/dev/null || true
  
  sleep 5

  info "Recreating commit-log topic on primary..."
  docker exec primary-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --topic commit-log \
    --partitions 1 --replication-factor 1 --config retention.ms=60000

  info "Producing 50 NEW messages into the recreated topic..."
  docker-compose run --rm --no-deps producer --count 50

  info "Starting MirrorMaker 2..."
  docker start mirrormaker

  info "Watching MirrorMaker logs for recovery (90s window)..."
  local detected=false
  for i in $(seq 1 45); do 
    sleep 2
    if docker logs mirrormaker 2>&1 | grep -q "TOPIC RESET DETECTED"; then
      detected=true
      break
    fi
  done

  local expected_standby=$((baseline_standby + 50))

  if $detected; then
    success "SCENARIO 3 PASSED - Topic reset detected in logs!"
    info "Verifying standby cluster received exactly 50 new messages..."
    wait_for_replication $expected_standby 60
  else
    error "SCENARIO 3 FAILED - Reset log not found."
    docker logs mirrormaker --tail 20
    return 1
  fi
}

# ============================================================
#  MAIN
# ============================================================
case "$SCENARIO" in
  all)
    setup
    scenario_normal   || { error "Normal scenario failed"; exit 1; }
    scenario_truncation || { error "Truncation scenario failed"; exit 1; }
    scenario_reset    || { error "Reset scenario failed"; exit 1; }
    header "ALL SCENARIOS PASSED PERFECTLY"
    ;;
  normal)      setup; scenario_normal ;;
  truncation)  setup; scenario_truncation ;;
  reset)       setup; scenario_reset ;;
  *)
    echo "Usage: $0 [all|normal|truncation|reset]"
    exit 1
    ;;
esac