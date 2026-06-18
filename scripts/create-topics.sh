#!/bin/sh
set -e

until /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list; do
  echo "waiting for kafka..."
  sleep 2
done

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic order-events \
  --partitions 3 \
  --replication-factor 1

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic inventory-events \
  --partitions 3 \
  --replication-factor 1

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic payment-events \
  --partitions 3 \
  --replication-factor 1

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic shipping-events \
  --partitions 3 \
  --replication-factor 1

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic dead-letter-events \
  --partitions 3 \
  --replication-factor 1