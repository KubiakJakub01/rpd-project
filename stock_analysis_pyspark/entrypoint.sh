#!/bin/bash -e

check_cassandra_ready() {
    echo "Checking if Cassandra is ready..."
    for i in {1..100}; do
        if cqlsh -e "describe keyspaces" $CASSANDRA_HOST; then
            echo "Cassandra is ready."
            return 0
        else
            echo "Waiting for Cassandra to be ready..."
            sleep 10
        fi
    done
    echo "Cassandra did not become ready in time."
    return 1
}

check_cassandra_ready || exit 1

echo "Starting Dash app"
python -m src.dashboard.app &

while true; do
    sleep 30
    echo "Running the data pipeline"
    if ! spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:$CASSANDRA_CONNECTOR_VERSION \
        src/main.py \
        --minio_endpoint $MINIO_ENDPOINT \
        --minio_access_key $MINIO_ACCESS_KEY \
        --minio_secret_key $MINIO_SECRET_KEY \
        --bucket_name $BUCKET_NAME \
        --cassandra_host $CASSANDRA_HOST \
        --cassandra_port $CASSANDRA_PORT \
        --cassandra_keyspace $CASSANDRA_KEYSPACE \
        --cassandra_table $CASSANDRA_TABLE \
        --save_target cassandra; then
        echo "Data pipeline failed. Exiting."
        exit 1
    fi

done
