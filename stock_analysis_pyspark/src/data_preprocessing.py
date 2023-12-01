import logging
import os
import time
from typing import Literal

import pandas as pd
from cassandra import ReadTimeout
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from pyspark.sql.functions import date_format
from pyspark.sql.functions import col
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    StringType,
    TimestampType,
)

logger = logging.getLogger(__name__)


def load_data(
    spark_session, data_path: str | None = None, bucket_name: str | None = None
):
    """Load data from csv files"""
    if data_path is not None:
        logger.info(f"Reading data from {data_path}")
        df = spark_session.read.csv(data_path, header=True, inferSchema=True)
    if bucket_name is not None:
        logger.info(f"Reading data from {bucket_name}")
        try:
            df = spark_session.read.parquet(f"s3a://{bucket_name}/*.parquet")
        except Exception:
            logger.warning("Failed to read data from S3 bucket")
            return None

    schema = [
        ("timestamp", "timestamp"),
        ("open", "float"),
        ("high", "float"),
        ("low", "float"),
        ("close", "float"),
        ("volume", "float"),
    ]

    for column_name, data_type in schema:
        df = df.withColumn(column_name, col(column_name).cast(data_type))

    return df


def write_data(
    df,
    target=Literal["csv", "cassandra"],
    path: str | None = None,
    cassandra_host: str | None = None,
    cassandra_port: str | None = None,
    cassandra_keyspace: str | None = None,
    cassandra_table: str | None = None,
):
    """Write data to csv file"""
    df = df.toDF(*[c.lower() for c in df.columns])
    df = df.withColumn("timestamp", date_format("timestamp", "yyyy/MM/dd"))
    if target == "csv":
        assert path is not None, "Path must be provided"
        logger.info(f"Saving data to {path}")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.write.csv(path=path, mode="overwrite", header=True)
    elif target == "cassandra":
        assert cassandra_host is not None, "Cassandra host must be provided"
        assert cassandra_port is not None, "Cassandra port must be provided"
        assert cassandra_keyspace is not None, "Cassandra keyspace must be provided"
        assert cassandra_table is not None, "Cassandra table must be provided"
        logger.info("Saving data to Cassandra")
        create_cassandra_table_from_df(
            df, cassandra_host, cassandra_keyspace, cassandra_table, cassandra_port
        )
        df.write.format("org.apache.spark.sql.cassandra").options(
            table=cassandra_table, keyspace=cassandra_keyspace
        ).mode("append").save()


def pyspark_to_cassandra_type(pyspark_type):
    """
    Maps PySpark data types to Cassandra data types.
    """
    type_mapping = {
        StringType: "text",
        IntegerType: "int",
        FloatType: "float",
        TimestampType: "timestamp",
        BooleanType: "boolean",
        DoubleType: "double",
    }
    return type_mapping.get(type(pyspark_type), "text")


def create_cassandra_table_from_df(df, host, keyspace_name, table_name, port=9042):
    """Create a Cassandra table based on the DataFrame schema."""
    logger.info(f"Connecting to Cassandra {host}:{port}")
    cluster = Cluster([host], port=port)
    session = cluster.connect()

    # Check if the table already exists
    logger.info(f"Checking if table {table_name} already exists")
    query = SimpleStatement(
        f"SELECT * FROM system_schema.tables WHERE keyspace_name = '{keyspace_name}' AND table_name = '{table_name}';"
    )
    rows = session.execute(query)
    if rows:
        logger.info(f"Table {table_name} already exists. Skipping table creation.")
        session.shutdown()
        cluster.shutdown()
        return

    # Create the table schema based on the DataFrame
    schema = df.schema
    columns = [
        f"{field.name.lower()} {pyspark_to_cassandra_type(field.dataType)}"
        for field in schema.fields
    ]
    logger.info(f"Creating Cassandra table {table_name} with schema: {columns}")

    # Assuming the first column is the primary key for simplicity
    primary_key = schema.fields[0].name if schema.fields else ""

    # Construct the CREATE TABLE statement
    create_table_statement = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)}, PRIMARY KEY ({primary_key}))"

    # Create keyspace and table
    logger.info(f"Creating Cassandra keyspace {keyspace_name} and table {table_name}")
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
    )
    # Retry mechanism for setting keyspace
    max_retries = 5
    retry_count = 0
    while retry_count < max_retries:
        try:
            session.set_keyspace(keyspace_name)
            break
        except ReadTimeout:
            time.sleep(2)
            retry_count += 1
            logger.info(
                f"Retry {retry_count}/{max_retries}: Waiting for keyspace to be available..."
            )

    if retry_count == max_retries:
        cluster.shutdown()
        raise Exception("Failed to set keyspace after maximum retries. Exiting.")

    session.set_keyspace(keyspace_name)
    session.execute(create_table_statement)

    session.shutdown()
    cluster.shutdown()


def clean_data(df, fill_strategy=Literal["drop", "zeroes", "mean"]):
    """Clean data"""
    df = df.dropDuplicates()
    if fill_strategy == "drop":
        df = df.dropna()
    elif fill_strategy == "zeroes":
        df = df.na.fill(0)
    elif fill_strategy == "mean":
        for column in ["open", "high", "low", "close", "volume"]:
            mean_value = df.select(column).agg({column: "mean"}).collect()[0][0]
            df = df.na.fill({column: mean_value})
    return df


def load_data_from_cassandra(
    hosts: str = "cassandra-node1",
    keyspace: str = "stock_analysis",
    table: str = "stock_prices",
):
    """Load data from a Cassandra table and return a Pandas DataFrame."""
    logger = logging.getLogger(__name__)
    logger.info(f"Connecting to Cassandra {hosts}")
    cluster = Cluster(contact_points=[hosts])
    session = cluster.connect()

    # Check if keyspace exists
    keyspace_check_query = SimpleStatement(
        f"SELECT * FROM system_schema.keyspaces WHERE keyspace_name = '{keyspace}';"
    )
    keyspace_result = session.execute(keyspace_check_query)
    if not list(keyspace_result):
        logger.warning(f"Keyspace '{keyspace}' does not exist.")
        session.shutdown()
        cluster.shutdown()
        return None

    # Check if table exists
    table_check_query = SimpleStatement(
        f"SELECT * FROM system_schema.tables WHERE keyspace_name = '{keyspace}' AND table_name = '{table}';"
    )
    table_result = session.execute(table_check_query)
    if not list(table_result):
        logger.warning(f"Table '{table}' does not exist in keyspace '{keyspace}'.")
        session.shutdown()
        cluster.shutdown()
        return None

    # Proceed to load data
    session.set_keyspace(keyspace)
    logger.info(f"Loading data from Cassandra table {table}")
    query = f"SELECT * FROM {table}"
    rows = session.execute(query)
    df = pd.DataFrame(list(rows))

    # Convert timestamp column to datetime and sort
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df.sort_values(by="timestamp", inplace=True)
    df.reset_index(drop=True, inplace=True)
    logger.info(f"Loaded {len(df.index)} rows from Cassandra")

    session.shutdown()
    cluster.shutdown()
    return df
