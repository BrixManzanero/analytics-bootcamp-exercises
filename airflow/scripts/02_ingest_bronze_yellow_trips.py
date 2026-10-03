
import re
import os
import argparse

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampNTZType, IntegerType, LongType
from traitlets import Integer


SOURCE_FILE = (
    "s3a://data-lake/" # SeaweedFS bucket
    "nyc_yellow_taxi/" # folder
    "yellow_tripdata_{month}.parquet" # file
)

FULL_TABLE_NAME = "lakehouse.bronze.yellow_taxi_trips"
SCHEMA = StructType([
    StructField("VendorID", IntegerType(), False),
    StructField("tpep_pickup_datetime", TimestampNTZType(), False),
    StructField("tpep_dropoff_datetime", TimestampNTZType(), False),
    StructField("passenger_count", IntegerType(), False),
    StructField("trip_distance", DoubleType(), False),
    StructField("RatecodeID", LongType(), False),
    StructField("store_and_fwd_flag", StringType(), False),
    StructField("PULocationID", LongType(), False),
    StructField("DOLocationID", LongType(), False),
    StructField("payment_type", LongType(), False),
    StructField("fare_amount", DoubleType(), False),
    StructField("extra", DoubleType(), False),
    StructField("mta_tax", DoubleType(), False),
    StructField("tip_amount", DoubleType(), False),
    StructField("tolls_amount", DoubleType(), False),
    StructField("improvement_surcharge", DoubleType(), False),
    StructField("total_amount", DoubleType(), False),
    StructField("congestion_surcharge", DoubleType(), False),
    StructField("airport_fee", DoubleType(), False),
    StructField("cbd_congestion_fee", DoubleType(), False),
    StructField("source_file", StringType(), True),
    StructField("etl_datetime", TimestampNTZType(), True),
])

def build_spark(app_name: str = "SparkSession"):
    endpoint = os.getenv("S3_ENDPOINT", "http://asb-seaweedfs:8333")
    access = os.getenv("AWS_ACCESS_KEY_ID", "admin")
    secret = os.getenv("AWS_SECRET_ACCESS_KEY", "password")
    region = os.getenv("AWS_REGION", "us-east-1")

    settings = {
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions."
            "IcebergSparkSessionExtensions"
        ),
        "spark.sql.catalog.lakehouse": (
            "org.apache.iceberg.spark.SparkCatalog"
        ),
        "spark.sql.catalog.lakehouse.type": "rest",
        "spark.sql.catalog.lakehouse.uri": os.getenv(
            "ICEBERG_REST_URI",
            "http://asb-iceberg-rest:8181",
        ),
        "spark.sql.catalog.lakehouse.warehouse": "s3://warehouse/",
        "spark.sql.catalog.lakehouse.io-impl": (
            "org.apache.iceberg.aws.s3.S3FileIO"
        ),
        "spark.sql.catalog.lakehouse.s3.endpoint": endpoint,
        "spark.sql.catalog.lakehouse.s3.path-style-access": "true",
        "spark.sql.catalog.lakehouse.s3.access-key-id": access,
        "spark.sql.catalog.lakehouse.s3.secret-access-key": secret,
        "spark.sql.catalog.lakehouse.client.region": region,

        # Raw Parquet uses Hadoop S3A independently of Iceberg's S3FileIO.
        "spark.hadoop.fs.s3a.impl": (
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        ),
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.endpoint.region": region,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": (
            "false" if endpoint.startswith("http:") else "true"
        ),
        "spark.hadoop.fs.s3a.access.key": access,
        "spark.hadoop.fs.s3a.secret.key": secret,
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ),
        "spark.hadoop.fs.s3a.change.detection.mode": "none",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": "8",
        "spark.sql.ansi.enabled": "true",
    }

    builder = SparkSession.builder.appName(app_name)

    # Dependency downloads are disabled here. The required JARs should be
    # installed in the Spark image or mounted into Spark's classpath.
    #
    # Uncomment only when runtime Maven downloads are intentionally allowed:
    # builder = builder.config(
    #     "spark.jars.packages",
    #     "org.apache.hadoop:hadoop-aws:3.3.4",
    # )
    # builder = builder.config(
    #     "spark.jars.ivy",
    #     "/opt/spark/ivy-cache",
    # )

    for key, value in settings.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()

def create_table_if_not_exists(spark, table_name: str, schema: StructType, partition_columns: list = None):
    if not spark.catalog.tableExists(table_name):
        print(f"[CREATE] {table_name}", flush=True)
        empty_df = spark.createDataFrame([], schema).writeTo(table_name)

        if partition_columns:
            empty_df = empty_df.partitionedBy(*partition_columns)

        empty_df.create()

def delete_by_filename(spark, table_name: str, filename: str):
    if spark.catalog.tableExists(table_name):
        print(f"[DELETE] {table_name}", flush=True)
        spark.sql(f"DELETE FROM {table_name} WHERE source_file = '{filename}';")

def read_parquet(spark, source: str, schema: StructType):
    # Read data from Parquet
    raw = (
        spark.read.format("parquet")
        .load(source)
    )

    # Cast columns 
    expression = [
        F.col(field.name).cast(field.dataType).alias(field.name)
        for field in schema
        if field.name not in ("source_file", "etl_datetime")
    ]

    df = (
        raw.select(*expression)
        .withColumn("source_file", F.lit(source))
        .withColumn("etl_datetime", F.current_timestamp())
    )

    return df

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", required=True, help="Required source month in YYYY-MM format")
    args = parser.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    source_file_fmt = SOURCE_FILE.format(month=args.month)

    try:
        print(f"[START] Ingesting NYC Yellow Taxi trips from {source_file_fmt}", flush=True)

        # Create the Iceberg table if it doesn't exist, and truncate it to ensure a clean load.
        create_table_if_not_exists(spark, FULL_TABLE_NAME, SCHEMA)
        delete_by_filename(spark, FULL_TABLE_NAME, source_file_fmt)
        
        print(f"[READ] {source_file_fmt}", flush=True)
        df = read_parquet(spark, source_file_fmt, SCHEMA)
        row_count = df.count()

        # Write the DataFrame to the Iceberg table in append mode.
        df.writeTo(FULL_TABLE_NAME).append()
        print(f"[SUCCESS] {row_count:,} rows -> {FULL_TABLE_NAME}", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

