# Kafka Data Replication Project

Enhanced Apache Kafka MirrorMaker 2 with intelligent fault detection and automatic recovery for Primary → DR cluster replication.

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────┐
│                     PRIMARY CLUSTER                      │
│  Producer → commit-log topic (WAL, 1 partition)         │
│                         │                               │
│              MirrorMaker 2 (Enhanced)                   │
│           reads from commit-log                         │
└──────────────────────────┬───────────────────────────────┘
                           │ replicates
                           ▼
┌──────────────────────────────────────────────────────────┐
│                    STANDBY (DR) CLUSTER                  │
│  primary.commit-log topic (1 partition)                  │
│  Standby services replay from here                      │
└──────────────────────────────────────────────────────────┘
```

### Why this topology?
- `commit-log` acts as a **Write-Ahead Log (WAL)** — the authoritative ordered record of all system state changes
- MirrorMaker 2 continuously replicates it to the DR cluster
- If the primary goes down, DR services can resume from `primary.commit-log` with minimal data loss

---

## Repository Links

- **Kafka Fork**: `https://github.com/chellaraomadine-dev/kafka` (branch: `enhanced-mm2-fault-tolerance`)
- **Pull Request**: `https://github.com/chellaraomadine-dev/kafka/pull/1`
- **This Repo**: `https://github.com/chellaraomadine-dev/kafka-replication-project`

---

## Docker Hub Images

| Image | Tag | Description |
|-------|-----|-------------|
| `chellarao/enhanced-mirrormaker2` | `latest` | Patched MirrorMaker 2 with fault tolerance |
| `chellarao/commit-log-producer` | `latest` | CLI event generator |

---

## Setup Instructions

### Prerequisites
- Docker ≥ 24.x
- Docker Compose ≥ 2.x

### Clone and run
```bash
git clone https://github.com/chellaraomadine-dev/kafka-replication-project.git
cd kafka-replication-project

# Run all 3 scenarios
bash scripts/run_challenge.sh all
```

Each scenario manages its own environment — no manual `docker-compose up` needed before running.

Services started automatically per scenario:
1. `primary-kafka` — Source cluster on port 9092
2. `standby-kafka` — DR cluster on port 9093
3. `topic-init` — Creates topics (one-shot, exits after)
4. `mirrormaker` — Enhanced MirrorMaker 2

---

## Test Execution

### Run all three scenarios
```bash
bash scripts/run_challenge.sh all
```

### Run each scenario independently
Each scenario is fully self-contained — starts its own fresh environment.

```bash
bash scripts/run_challenge.sh normal       # Scenario 1: Normal replication
bash scripts/run_challenge.sh truncation   # Scenario 2: Log truncation detection
bash scripts/run_challenge.sh reset        # Scenario 3: Topic reset recovery
```

### What each scenario does

**Scenario 1 — Normal Replication**
- Starts fresh environment
- Producer generates exactly 1000 messages to `commit-log` on primary
- Waits for all 1000 to appear on `primary.commit-log` on standby
- Shows 3 sample replicated messages to confirm correct JSON format

**Scenario 2 — Log Truncation Detection**
- Starts fresh environment
- Produces 300 messages and waits for complete replication — standby count `S` confirms MM2's committed position
- Pauses MirrorMaker 2 (freezes consumer position at `S`)
- Produces 200 more messages while MM2 is paused (MM2 cannot replicate these)
- Uses `kafka-delete-records` to advance `logStartOffset` to `S+100` — simulates retention purging unreplicated messages
- Unpauses MM2 — on first `poll()` detects `position(S) < logStartOffset(S+100)` → throws `DataLossException`

**Scenario 3 — Topic Reset Recovery**
- Starts fresh environment
- Produces 100 messages, replicates fully
- Advances `logStartOffset` to 50 while MM2 is live — MM2 stores `prevLogStart=50` in its HashMap
- Pauses MirrorMaker 2
- Deletes and recreates `commit-log` topic — new topic `logStartOffset=0`
- Produces 50 fresh messages into new topic
- Unpauses MM2 — detects `prevLogStart(50)>0` AND `logStartOffset==0` → `TOPIC RESET DETECTED`
- MM2 calls `consumer.seek(0)` and resumes replication automatically

### Produce messages manually
```bash
docker-compose run --rm --no-deps producer --count 500
```

### Verify replication manually
```bash
# Windows (Git Bash)
MSYS_NO_PATHCONV=1 docker exec standby-kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic primary.commit-log

# Mac / Linux
docker exec standby-kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic primary.commit-log
```

---

## Log Analysis

### What to watch for

**Normal replication**
```
[MirrorSourceConnector|task-0] task-thread-MirrorSourceConnector-0 replicating 1 topic-partitions primary->standby: [commit-log-0]
[MirrorSourceConnector|task-0|offsets] WorkerSourceTask{id=MirrorSourceConnector-0} Committing offsets for 1000 acknowledged messages
```

**Log truncation detected** (Scenario 2)
```
ERROR [MirrorSourceConnector|task-0] [MirrorSourceTask] LOG TRUNCATION DETECTED on partition commit-log-0.
      Consumer position 300 is below logStartOffset 400.
      Approximately 100 messages were deleted by retention before replication completed.
      Failing fast — operator must inspect DR cluster for gaps starting at offset 300.
org.apache.kafka.connect.mirror.DataLossException: [MirrorSourceTask] LOG TRUNCATION DETECTED ...
```
→ MM2 throws `DataLossException` — Connect marks the task as FAILED immediately.

**Topic reset detected** (Scenario 3)
```
WARN [MirrorSourceConnector|task-0] [MirrorSourceTask] TOPIC RESET DETECTED on partition commit-log-0 at 2026-...
     logStartOffset dropped from 50 to 0.
     Topic was likely deleted and recreated.
     Seeking to offset 0 and resuming replication automatically.
```
→ MM2 seeks to offset 0 and resumes replication — no operator intervention needed.

### Useful commands
```bash
# Live MirrorMaker logs
docker-compose logs -f mirrormaker

# Check for truncation events
docker logs mirrormaker 2>&1 | grep -E "TRUNCATION|DataLoss"

# Check for reset events
docker logs mirrormaker 2>&1 | grep "TOPIC RESET"

# Count messages on standby
MSYS_NO_PATHCONV=1 docker exec standby-kafka \
  /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic primary.commit-log

# Count messages on primary
MSYS_NO_PATHCONV=1 docker exec primary-kafka \
  /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic commit-log
```

---

## Design Rationale

### Why MirrorSourceTask?

`MirrorSourceTask` is the hot path in MirrorMaker 2 — it runs the `poll()` loop that reads from the source cluster. Both failure modes manifest as anomalies in offset arithmetic, making this the natural place to add detection logic with minimal disruption to the existing codebase.

The key Kafka metadata we exploit:

| Offset type | API call | Meaning |
|-------------|----------|---------|
| `logStartOffset` | `consumer.beginningOffsets()` | Lowest offset currently on disk |
| `consumer position` | `consumer.position()` | Next offset MirrorMaker will read |

### Log Truncation Detection

**Condition**: `consumer.position(tp) < beginningOffsets(tp)`

When Kafka's retention deletes old segments, `logStartOffset` advances past the consumer's committed position. Vanilla MirrorMaker 2 catches the resulting `OffsetOutOfRangeException` inside `poll()` and resets to the latest offset — silently skipping the gap and creating invisible data loss in the DR cluster.

Our fix detects the gap *before* calling `poll()` and throws `DataLossException` immediately. This is the **fail-fast** pattern: surface the problem loudly rather than hiding it.

**Tradeoff**: The task goes FAILED and needs manual intervention. The alternative — skip to logStartOffset — would keep replication running but produce a permanently incomplete DR copy. For a WAL, a known gap is worse than a noisy failure.

### Topic Reset Recovery

**Condition**: `prevLogStartOffset > 0 AND currentLogStartOffset == 0`

A topic deletion wipes all segments. When the topic is recreated, `logStartOffset` resets to 0. We detect this by comparing the current `logStartOffset` against the last value stored in `lastKnownLogStartOffset` (a `HashMap<TopicPartition, Long>` maintained across poll cycles).

Our fix calls `consumer.seek(tp, 0)` to restart replication from the beginning of the new topic — **automatic recovery** with no operator intervention.

**Tradeoff**: The DR cluster will contain data from the old topic followed by data from offset 0 of the new topic. For a WAL this is acceptable — a topic reset is an intentional operator action representing a deliberate state transition.

### Integration approach

Changes are confined to two files:
- `MirrorSourceTask.java` — one new field (`lastKnownLogStartOffset`) and one guard block at the top of `poll()`
- `DataLossException.java` — new file, extends `RuntimeException`

No new threads, no new Kafka topics, no new configuration. The existing `consumer.beginningOffsets()` API provides everything needed. Total change: under 80 lines.

---

## Concepts Involved

| Concept | Where it appears |
|---------|-----------------|
| Kafka Write-Ahead Log (WAL) | `commit-log` topic design |
| KRaft (ZooKeeper-free Kafka) | Both cluster configs |
| MirrorMaker 2 / Kafka Connect | Core replication service |
| Consumer offset tracking | Truncation & reset detection |
| logStartOffset vs consumer position | Detection arithmetic |
| Fail-fast error handling | Truncation → `DataLossException` |
| Automatic recovery / idempotent seek | Reset → `consumer.seek(0)` |
| Connect offset store | MM2 committed position persistence |
| kafka-delete-records | Simulating retention-based log truncation |
| Docker Compose service orchestration | Full environment |
| SLF4J structured logging | All error/warn messages |

---

## AI Usage Documentation

Claude AI (Anthropic) was used selectively during development to accelerate specific technical challenges.

**Where AI helped:**

- **Understanding MirrorMaker 2 internals**: I was unfamiliar with the Connect worker lifecycle (`start → poll → commitRecord → stop`). I asked Claude to explain how `SourceTask` lifecycle hooks work and which method is the correct insertion point for pre-poll checks.

- **Offset arithmetic validation**: I described the two failure scenarios and asked Claude to verify that comparing `consumer.position()` against `consumer.beginningOffsets()` is the correct approach. Claude confirmed the logic and noted the edge case where `logStartOffset` drops to 0 on topic recreation.

- **Docker Compose configuration**: Setting up two isolated Kafka clusters in KRaft mode with correct networking was time-consuming. I asked Claude for a working compose template and modified it for this project.

- **Gradle build flags**: When the Docker build was failing due to Java toolchain detection conflicts, I asked Claude what Gradle flags disable toolchain auto-detection. The `-Porg.gradle.java.installations.auto-detect=false` flag resolved the issue.

**What I did independently:**

- Read the actual `MirrorSourceTask.java` source from the Kafka repository to understand the real method signatures before writing the patch
- Identified version-specific APIs by reading compiler errors directly from the Kafka build output
- Debugged the JAR version mismatch (`4.3.0-SNAPSHOT` vs `4.0.0` base image) by analysing the `NoSuchMethodError` stack trace
- Ran all three test scenarios end-to-end and interpreted the log output
- Made the design decision on fail-fast vs auto-recover based on the operational implications for a WAL-based system
