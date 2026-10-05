"""Create silver taxi zones from the raw lookup CSV stored in SeaweedFS.

Full table overwrite after validation. One row per location_id.
Run with spark-submit in asb-spark-iceberg, using Hadoop AWS 3.3.4 as in
this demo's bronze loader. No bronze zone table is assumed to exist.
Do not run concurrent zone loaders.
"""
import argparse
import os

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType

TARGET = "lakehouse.silver.taxi_zones"
DEFAULT_SOURCE = "s3a://data-lake/nyc_yellow_taxi/taxi_zone_lookup.csv"


def build_spark():
    endpoint = os.getenv("S3_ENDPOINT", "http://asb-seaweedfs:8333")
    settings = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.lakehouse.type": "rest",
        "spark.sql.catalog.lakehouse.uri": os.getenv("ICEBERG_REST_URI", "http://asb-iceberg-rest:8181"),
        "spark.sql.catalog.lakehouse.warehouse": "s3://warehouse/",
        "spark.sql.catalog.lakehouse.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.catalog.lakehouse.s3.endpoint": endpoint,
        "spark.sql.catalog.lakehouse.s3.path-style-access": "true",
        "spark.sql.catalog.lakehouse.s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", "admin"),
        "spark.sql.catalog.lakehouse.s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY", "password"),
        "spark.sql.catalog.lakehouse.client.region": os.getenv("AWS_REGION", "us-east-1"),
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false" if endpoint.startswith("http:") else "true",
        "spark.hadoop.fs.s3a.access.key": os.getenv("AWS_ACCESS_KEY_ID", "admin"),
        "spark.hadoop.fs.s3a.secret.key": os.getenv("AWS_SECRET_ACCESS_KEY", "password"),
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.sql.csv.parser.columnPruning.enabled": "false",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": "8",
        "spark.sql.ansi.enabled": "true",
    }
    builder = SparkSession.builder.appName("NYC Taxi Zones: Raw to Silver")
    for key, value in settings.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def clean_text(column):
    value = F.trim(F.regexp_replace(F.col(column), r"\s+", " "))
    return F.when(value == "", F.lit(None).cast("string")).otherwise(value)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default=DEFAULT_SOURCE, help="Raw lookup CSV URI")
    args = parser.parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    df = None
    try:
        # Explicit string schema preserves names such as 'N/A' and 'Unknown'.
        schema = StructType([StructField(name, StringType(), True) for name in
                             ["LocationID", "Borough", "Zone", "service_zone"]])
        raw = (spark.read.schema(schema).option("header", "true")
               .option("enforceSchema", "false").option("mode", "FAILFAST")
               .csv(args.source))

        df = (raw.select(
                F.expr("try_cast(trim(LocationID) AS BIGINT)").alias("location_id"),
                clean_text("Borough").alias("borough"),
                clean_text("Zone").alias("zone"),
                clean_text("service_zone").alias("service_zone"),
                F.input_file_name().alias("source_file"),
              )
              .withColumn("source_filename", F.regexp_extract("source_file", r"([^/]+)$", 1))
              .withColumn("quality_warnings", F.filter(F.array(
                  F.when(F.col("borough").isNull(), F.lit("MISSING_BOROUGH")),
                  F.when(F.col("zone").isNull(), F.lit("MISSING_ZONE")),
                  F.when(F.col("service_zone").isNull(), F.lit("MISSING_SERVICE_ZONE")),
              ), lambda value: value.isNotNull()))
              .withColumn("has_quality_warnings", F.size("quality_warnings") > 0)
              .withColumn("silver_loaded_at", F.current_timestamp())
              .persist(StorageLevel.MEMORY_AND_DISK))
        count = df.count()
        if count == 0:
            raise ValueError("Lookup is empty; refusing to overwrite silver")
        invalid = (df.where(F.col("location_id").isNull() | (F.col("location_id") <= 0))
                   .select("location_id").limit(5).collect())
        if invalid:
            raise ValueError(f"Lookup contains invalid location IDs: {invalid}")
        duplicates = (df.groupBy("location_id").count().where(F.col("count") > 1)
                      .limit(5).collect())
        if duplicates:
            raise ValueError(f"Duplicate location IDs; refusing ambiguous lookup: {duplicates}")
        warnings = df.where(F.col("has_quality_warnings")).count()
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {TARGET} (
            location_id BIGINT,
            borough STRING,
            zone STRING,
            service_zone STRING,
            source_file STRING,
            source_filename STRING,
            quality_warnings ARRAY<STRING>,
            has_quality_warnings BOOLEAN,
            silver_loaded_at TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')""")
        # Replace every row in one Iceberg commit after all validation succeeds.
        # IDs removed from the latest CSV are removed from silver too.
        df.writeTo(TARGET).overwrite(F.lit(True))
        print(f"[SUCCESS] {count:,} taxi zones -> {TARGET}; "
              f"{warnings:,} rows with warnings (full overwrite)", flush=True)
    finally:
        if df is not None:
            df.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()

