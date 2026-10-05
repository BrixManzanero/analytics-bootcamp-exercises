
import os
from pyspark.sql import SparkSession, functions as F, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampNTZType

SOURCE_FILE = (
    "s3a://data-lake/" # SeaweedFS bucket
    "nyc_yellow_taxi/" # folder
    "taxi_zone_lookup.csv" # file
)

FULL_TABLE_NAME = "lakehouse.bronze.taxi_zone_lookup"
SCHEMA = StructType([
    StructField("LocationID", StringType(), False),
    StructField("Borough", StringType(), True),
    StructField("Zone", StringType(), True),
    StructField("service_zone", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("etl_datetime", TimestampNTZType(), True),
])

def build_spark():
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

        # Raw CSV access through Hadoop S3A.
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
        "spark.sql.ansi.enabled": "true",
    }

    builder = SparkSession.builder.appName(
        "NYC Taxi Zones: SeaweedFS to Iceberg"
    )

    # Runtime dependency downloads are intentionally disabled. The Spark image
    # must already contain the S3A and Iceberg AWS JARs.
    #
    # Uncomment only if Maven downloads are intentionally allowed:
    # builder = builder.config(
    #     "spark.jars.packages",
    #     "org.apache.hadoop:hadoop-aws:3.3.4",
    # )

    for key, value in settings.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()

def create_table_if_not_exists(spark, table_name: str, schema: StructType):
    if not spark.catalog.tableExists(table_name):
        print(f"[CREATE] {table_name}", flush=True)
        spark.createDataFrame([], schema).writeTo(table_name).create()

def truncate_table(spark, table_name: str):
    if spark.catalog.tableExists(table_name):
        print(f"[TRUNCATE] {table_name}", flush=True)
        spark.sql(f"TRUNCATE TABLE {table_name};")
        
def read_csv(spark, source: str, schema: StructType) -> DataFrame:
    # Read data from CSV
    raw = (
        spark.read
        .option("header", "true")
        .option("mode", "FAILFAST")
        .schema(schema)
        .csv(source)
    )

    # Add metadata columns
    df = (
        raw
        .withColumn("source_file", F.lit(source))
        .withColumn("etl_datetime", F.current_timestamp())
    )

    return df

def main():

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:

        print(f"[START] Ingesting NYC Taxi Zones from {SOURCE_FILE}", flush=True)

        # Create the Iceberg table if it doesn't exist, and truncate it to ensure a clean load.
        create_table_if_not_exists(spark, FULL_TABLE_NAME, SCHEMA)
        truncate_table(spark, FULL_TABLE_NAME)
        
        print(f"[READ] {SOURCE_FILE}", flush=True)
        df = read_csv(spark, SOURCE_FILE, SCHEMA)
        row_count = df.count()

        # Write the DataFrame to the Iceberg table in append mode.
        df.writeTo(FULL_TABLE_NAME).append()
        print(f"[SUCCESS] {row_count:,} rows -> {FULL_TABLE_NAME}", flush=True)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()

