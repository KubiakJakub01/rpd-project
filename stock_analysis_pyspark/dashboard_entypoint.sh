#!/bin/bash

# Run the dashboard app
python -m src.dashboard.main \
    --host localhost \
    --keyspace stock_analysis \
    --table stock_data \
