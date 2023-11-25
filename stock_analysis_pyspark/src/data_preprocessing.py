import logging
import os
from typing import Literal

from cassandra.cluster import Cluster
from pyspark.sql.functions import date_format
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DoubleType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)


def load_data(spark_session, data_path):
    """Load data from csv files"""
    schema = StructType(
        [
            StructField("timestamp", TimestampType(), True),
            StructField("open", FloatType(), True),
            StructField("high", FloatType(), True),
            StructField("low", FloatType(), True),
            StructField("close", FloatType(), True),
            StructField("volume", FloatType(), True),
        ]
    )
    return spark_session.read.csv(data_path, header=True, schema=schema)


def write_data(df, path, target=Literal["csv", "cassandra"]):
    """Write data to csv file"""
    df = df.toDF(*[c.lower() for c in df.columns])
    if target == "csv":
        logger.info(f"Saving data to {path}")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df = df.withColumn("timestamp", date_format("timestamp", "yyyy/MM/dd"))
        df.write.csv(path=path, mode="overwrite", header=True)
    elif target == "cassandra":
        logger.info(f"Saving data to Cassandra")
        create_cassandra_table_from_df(
            df, "localhost", "stock_analysis", "stock_prices"
        )
        df.write.format("org.apache.spark.sql.cassandra").options(
            table="stock_prices", keyspace="stock_analysis"
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


def create_cassandra_table_from_df(df, host, keyspace_name, table_name):
    """Create a Cassandra table based on the DataFrame schema."""
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
    cluster = Cluster([host], port=9042)
    session = cluster.connect()

    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
    )
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
