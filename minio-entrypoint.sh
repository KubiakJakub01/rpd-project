#!/bin/bash

# Start MinIO in the background
minio server /data &

# Wait for MinIO to start
until mc alias set myminio http://localhost:9000 minioadmin minioadmin; do
  echo "Waiting for MinIO to start..."
  sleep 2
done

# Create buckets
mc mb myminio/windows-csv-data
mc mb myminio/windows-realtime-data

# Keep container running
wait
