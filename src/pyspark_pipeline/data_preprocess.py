'''Module with PySpark pipeline for data preprocessing'''
import argparse

from pyspark.sql import SparkSession


def get_params():
    '''Get parameters from command line'''
    parser = argparse.ArgumentParser()
    parser.add_argument('--minio_access_key', type=str, required=True)
    parser.add_argument('--minio_secret_key', type=str, required=True)
    parser.add_argument('--minio_endpoint', type=str, required=True)
    parser.add_argument('--minio_bucket', type=str, required=True)

    return parser.parse_args()


def main():
    '''Main function'''
    args = get_params()

    # Create Spark session
    spark = SparkSession.builder \
    .appName('MinIO Data Loading') \
    .config('spark.hadoop.fs.s3a.access.key', args.minio_access_key) \
    .config('spark.hadoop.fs.s3a.secret.key', args.minio_secret_key) \
    .config('spark.hadoop.fs.s3a.endpoint', args.minio_endpoint) \
    .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false') \
    .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
    .getOrCreate()

    # Read data from MinIO
    df = spark.read.csv(f's3a://{args.minio_bucket}/data.csv', header=True)
