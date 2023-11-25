#!/bin/bash

# Run the data pipeline
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 src/main.py --data_path data/weekly_IBM.csv --save_target cassandra
