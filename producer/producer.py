#!/usr/bin/env python3
"""
Commit Log Producer
Generates JSON events to the primary Kafka cluster's commit-log topic.
"""

import argparse
import json
import uuid
import time
import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Supported operation types (WAL-style events)
OP_TYPES = ["INSERT", "UPDATE", "DELETE", "UPSERT"]

def generate_event(index: int) -> dict:
    """Generate a single WAL-style JSON event."""
    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": int(time.time()),
        "op_type": OP_TYPES[index % len(OP_TYPES)],
        "key": f"doc:{uuid.uuid4().hex[:4]}",
        "value": {
            "status": ["active", "archived", "pending", "deleted"][index % 4]
        }
    }

def main():
    parser = argparse.ArgumentParser(description="Commit Log Producer for Kafka WAL simulation")
    parser.add_argument("--count", type=int, required=True, help="Number of messages to produce")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="commit-log", help="Target Kafka topic")
    parser.add_argument("--delay-ms", type=float, default=0, help="Delay between messages in milliseconds")
    args = parser.parse_args()

    print(f"[Producer] Connecting to Kafka at {args.bootstrap_servers}")
    print(f"[Producer] Will produce {args.count} messages to topic '{args.topic}'")

    try:
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            # Reliability settings
            acks="all",           # Wait for all replicas to acknowledge
            retries=3,            # Retry on transient failures
            max_block_ms=10000,   # 10s timeout for metadata/buffer
        )
    except KafkaError as e:
        print(f"[Producer] ERROR: Failed to connect to Kafka - {e}", file=sys.stderr)
        sys.exit(1)

    sent = 0
    failed = 0

    for i in range(args.count):
        event = generate_event(i)

        try:
            future = producer.send(
                args.topic,
                key=event["key"],
                value=event
            )
            # Block to confirm delivery (makes offset gaps detectable)
            record_metadata = future.get(timeout=10)
            sent += 1

            if sent % 100 == 0 or sent == args.count:
                print(f"[Producer] Sent {sent}/{args.count} | "
                      f"partition={record_metadata.partition} offset={record_metadata.offset}")

        except KafkaError as e:
            failed += 1
            print(f"[Producer] WARNING: Failed to send event {event['event_id']}: {e}", file=sys.stderr)

        if args.delay_ms > 0:
            time.sleep(args.delay_ms / 1000.0)

    producer.flush()
    producer.close()

    print(f"[Producer] Done. Sent: {sent}, Failed: {failed}")
    if failed > 0:
        sys.exit(1)

if __name__ == "__main__":
    main()
