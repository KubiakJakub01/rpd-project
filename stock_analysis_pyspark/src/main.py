import argparse
import os
import logging

from pyspark.sql import SparkSession

from feature_engineering import add_technical_indicators, add_time_features
from data_preprocessing import clean_data, load_data, write_data
from models.linear_regression import train_model, add_forecast



logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data_path",
        type=str,
        default=os.path.join(os.getcwd(), "data", "stock_prices.csv"),
        help="Path to the data file",
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
    spark = SparkSession.builder \
        .appName("Stock Analysis") \
        .config("spark.cassandra.connection.host", "localhost") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

    # Load data
    df = load_data(spark, args.data_path)
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
    write_data(df, args.output_path, args.save_target)

    # Stop Spark session
    logger.info("Stopping Spark session")
    spark.stop()


if __name__ == "__main__":
    main()
