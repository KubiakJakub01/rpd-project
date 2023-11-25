import os
from typing import Literal

from pyspark.sql.functions import date_format
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    DataType
)


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
    if target == "csv":
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df = df.withColumn("timestamp", date_format("timestamp", "yyyy/MM/dd"))
        df.write.csv(path=path, mode="overwrite", header=True)
    elif target == "cassandra":
        df.write.format("org.apache.spark.sql.cassandra").options(
            table="stock_prices", keyspace="stock_analysis"
        ).mode("overwrite").save()


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
