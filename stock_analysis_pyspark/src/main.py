import argparse
import logging
import os

from pyspark.sql import SparkSession

from data_preprocessing import clean_data, load_data, write_data
from feature_engineering import add_technical_indicators, add_time_features
from models.linear_regression import add_forecast, train_model


# Set up logging
logging.basicConfig(
    format="%(name)s %(levelname)s %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--data_path",
        type=str,
        default=None,
        help="Path to the data file",
    )
    group.add_argument(
        "--bucket_name",
        type=str,
        default=None,
        help="Name of the bucket",
    )
    parser.add_argument(
        "--minio_endpoint",
        type=str,
        default="http://minio:9000",
        help="MinIO endpoint",
    )
    parser.add_argument(
        "--minio_access_key",
        type=str,
        default="minioadmin",
        help="MinIO access key",
    )
    parser.add_argument(
        "--minio_secret_key",
        type=str,
        default="minioadmin",
        help="MinIO secret key",
    )
    parser.add_argument(
        "--cassandra_host",
        type=str,
        default="cassandra-node1",
        help="Cassandra host",
    )
    parser.add_argument(
        "--cassandra_port",
        type=int,
        default=9042,
        help="Cassandra port",
    )
    parser.add_argument(
        "--cassandra_dc_name",
        type=str,
        default="DC1",
        help="Cassandra DC name",
    )
    parser.add_argument(
        "--cassandra_keyspace",
        type=str,
        default="stock_analysis",
        help="Cassandra keyspace",
    )
    parser.add_argument(
        "--cassandra_table",
        type=str,
        default="stock_prices",
        help="Cassandra table",
    )
    parser.add_argument(
        "--output_path",
        type=str,
        default=os.path.join(os.getcwd(), "data", "stock_prices_clean.csv"),
        help="Path to the output file",
    )
    parser.add_argument(
        "--model_path",
        type=str,
        default=None,
        help="Path to the model file",
    )
    parser.add_argument(
        "--save_target",
        type=str,
        choices=["csv", "cassandra"],
        default="csv",
        help="Target to save the data",
    )
    args = parser.parse_args()
    return args


def main():
    """Main function"""
    logger.info("Starting stock analysis")

    # Parse command line arguments
    args = parse_args()
    logger.info(f"Using data from: {args.data_path}")

    # Create Spark session
    logger.info("Creating Spark session")
    spark = (
        SparkSession.builder.appName("Stock Analysis")
        .config("spark.hadoop.fs.s3a.endpoint", args.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", args.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", args.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", True)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.cassandra.connection.host", args.cassandra_host)
        .config("spark.cassandra.connection.port", str(args.cassandra_port))
        .getOrCreate()
    )

    # Set log level to WARNING
    spark.sparkContext.setLogLevel("WARN")

    # Load data
    df = load_data(spark, args.data_path, args.bucket_name)
    logger.info("Data loaded")
    logger.info(f"Number of rows: {df.count()}")
    logger.info("Data schema:")
    df.printSchema()
    logger.info("Data summary:")
    df.describe().show()
    logger.info("Data sample:")
    df.show()

    # Clean data
    df = clean_data(df)
    logger.info("Data cleaned")
    logger.info(f"Number of rows: {df.count()}")

    # Add time features
    logger.info("Adding time features")
    df = add_time_features(df)

    # Add technical indicators
    logger.info("Adding technical indicators")
    df = add_technical_indicators(df)

    # Train model
    logger.info("Training model")
    model = train_model(df, save_path=args.model_path)

    # Add forecast
    logger.info("Adding forecast")
    df = add_forecast(model, df)

    # Write data
    logger.info("Writing data")
    logger.info(f"Df schema: {df.printSchema()}")
    write_data(
        df,
        target=args.save_target,
        path=args.output_path,
        cassandra_host=args.cassandra_host,
        cassandra_port=args.cassandra_port,
        cassandra_keyspace=args.cassandra_keyspace,
        cassandra_table=args.cassandra_table,
    )

    # Stop Spark session
    logger.info("Stopping Spark session")
    spark.stop()


if __name__ == "__main__":
    main()
