#!/bin/bash

echo "Waiting 60 seconds for Kafka to be ready..."
sleep 60

echo "Starting Spark job..."
exec spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /app/spark.py
