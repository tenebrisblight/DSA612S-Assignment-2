#!/usr/bin/env bash
# Creates all domain topics explicitly (partitions=3) so partitioning is
# deterministic rather than relying on the broker's auto-create default.
set -e

BOOTSTRAP="${BOOTSTRAP:-kafka:19092}"

TOPICS=(
  orders.created orders.confirmed orders.cancelled orders.status.changed
  payments.completed payments.failed
  kitchen.preparing kitchen.ready kitchen.rejected
  delivery.assigned delivery.picked_up delivery.completed delivery.failed
  restaurant.menu.updated
)

for topic in "${TOPICS[@]}"; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists --topic "$topic" \
    --partitions 3 --replication-factor 1
  echo "topic ready: $topic"
done

echo "all topics created"
