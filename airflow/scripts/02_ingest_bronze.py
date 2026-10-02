"""Load every ingested Yellow Taxi monthly Parquet file into Iceberg.

Run with spark-submit in asb-spark-iceberg. No boto3 dependency.
Each source month is atomically replaced; do not run concurrent loaders.
"""
import argparse
import os
import re
from pyspark.sql import SparkSession, functions as F
from pyspark import StorageLevel

# Explicit casts accommodate numeric schema differences across TLC files.
COLUMNS = {
    "vendor_id": ("VendorID", "bigint"),
    "pickup_datetime": ("tpep_pickup_datetime", "timestamp"),
    "dropoff_datetime": ("tpep_dropoff_datetime", "timestamp"),
    "passenger_count": ("passenger_count", "double"),
    "trip_distance": ("trip_distance", "double"),
    "ratecode_id": ("RatecodeID", "bigint"),
    "store_and_fwd_flag": ("store_and_fwd_flag", "string"),
    "pickup_location_id": ("PULocationID", "bigint"),
    "dropoff_location_id": ("DOLocationID", "bigint"),
    "payment_type": ("payment_type", "bigint"),
    "fare_amount": ("fare_amount", "double"),
    "extra": ("extra", "double"),
    "mta_tax": ("mta_tax", "double"),
    "tip_amount": ("tip_amount", "double"),
    "tolls_amount": ("tolls_amount", "double"),
    "improvement_surcharge": ("improvement_surcharge", "double"),
    "total_amount": ("total_amount", "double"),
    "congestion_surcharge": ("congestion_surcharge", "double"),
    "airport_fee": ("airport_fee", "double"),
    "cbd_congestion_fee": ("cbd_congestion_fee", "double"),
}

def build_spark():
    endpoint = os.getenv("S3_ENDPOINT", "http://asb-seaweedfs:8333")
    access = os.getenv("AWS_ACCESS_KEY_ID", "admin")
    secret = os.getenv("AWS_SECRET_ACCESS_KEY", "password")
    region = os.getenv("AWS_REGION", "us-east-1")
    settings = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.lakehouse.type": "rest",
        "spark.sql.catalog.lakehouse.uri": os.getenv("ICEBERG_REST_URI", "http://asb-iceberg-rest:8181"),
        "spark.sql.catalog.lakehouse.warehouse": "s3://warehouse/",
        "spark.sql.catalog.lakehouse.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.catalog.lakehouse.s3.endpoint": endpoint,
        "spark.sql.catalog.lakehouse.s3.path-style-access": "true",
        "spark.sql.catalog.lakehouse.s3.access-key-id": access,
        "spark.sql.catalog.lakehouse.s3.secret-access-key": secret,
        "spark.sql.catalog.lakehouse.client.region": region,
        # Raw Parquet uses Hadoop S3A, independently of Iceberg's S3FileIO.
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.endpoint.region": region,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false" if endpoint.startswith("http:") else "true",
        "spark.hadoop.fs.s3a.access.key": access,
        "spark.hadoop.fs.s3a.secret.key": secret,
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": "8",
        "spark.sql.ansi.enabled": "true",
    }
    builder = SparkSession.builder.appName("NYC Yellow Taxi: SeaweedFS to Iceberg")
    for key, value in settings.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def find_files(spark, source):
    path = spark._jvm.org.apache.hadoop.fs.Path(source)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    iterator = fs.listFiles(path, True)
    found = {}
    while iterator.hasNext():
        item = iterator.next()
        match = re.fullmatch(r"yellow_tripdata_(\d{4})-(0[1-9]|1[0-2])\.parquet", item.getPath().getName())
        if match:
            month = f"{match[1]}-{match[2]}"
            if month in found:
                raise ValueError(f"Multiple files for {month}; refusing ambiguous overwrite")
            found[month] = item.getPath().toString()
    return sorted(found.items())

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default="s3a://data-lake/nyc_yellow_taxi/")
    parser.add_argument("--month", help="Optional single month, e.g. 2025-01; default loads all files")
    args = parser.parse_args()
    if args.month and not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", args.month):
        parser.error("--month must be YYYY-MM")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    table = "lakehouse.bronze.yellow_taxi_trips"
    try:
        files = find_files(spark, args.source)
        if args.month:
            files = [(month, path) for month, path in files if month == args.month]
        if not files:
            raise RuntimeError("No matching monthly Yellow Taxi Parquet files found")

        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
        ddl = ",\n".join(f"{name} {kind}" for name, (_, kind) in COLUMNS.items())
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {table} (
            {ddl},
            source_month STRING,
            source_file STRING,
            loaded_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (source_month)
        TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')""")

        for month, path in files:
            print(f"[READ] {month}: {path}", flush=True)
            raw = spark.read.parquet(path)
            lookup = {name.lower(): name for name in raw.columns}
            required = {"tpep_pickup_datetime", "tpep_dropoff_datetime"}
            if not required.issubset(lookup):
                raise ValueError(f"{path}: missing Yellow Taxi timestamp columns")
            unknown = [name for name in raw.columns if name.lower() not in {src.lower() for src, _ in COLUMNS.values()}]
            if unknown:
                print(f"[WARN] Columns outside the target schema are omitted: {unknown}", flush=True)
            expressions = []
            for name, (source, kind) in COLUMNS.items():
                actual = lookup.get(source.lower())
                value = F.col(f"`{actual}`") if actual else F.lit(None)
                expressions.append(value.cast(kind).alias(name))
            df = (raw.select(*expressions)
                  .withColumn("source_month", F.lit(month))
                  .withColumn("source_file", F.lit(path))
                  .withColumn("loaded_at", F.current_timestamp())
                  .persist(StorageLevel.DISK_ONLY))
            try:
                count = df.count()
                if count == 0:
                    raise ValueError(
                        f"{path}: empty source; refusing to replace existing data"
                    )

                filename = path.rsplit("/", 1)[-1]

                print(f"[DELETE] Existing rows for {filename}", flush=True)

                spark.sql(
                    f"""
                    DELETE FROM {table}
                    WHERE regexp_extract(source_file, '([^/]+)$', 1) = :filename
                    """,
                    args={"filename": filename},
                )

                df.writeTo(table).append()

                print(
                    f"[SUCCESS] {filename}: {count:,} rows -> {table}",
                    flush=True,
                )
            finally:
                df.unpersist()
        print("[DONE] All selected files loaded", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
