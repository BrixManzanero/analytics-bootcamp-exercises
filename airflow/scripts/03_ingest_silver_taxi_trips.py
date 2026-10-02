"""Build lakehouse.silver.yellow_taxi_trips from the existing bronze table.

Run in asb-spark-iceberg (Spark 3.5). Default: process all bronze source months.
Use --month YYYY-MM to process one month. Each month is atomically overwritten.
Keep all source rows; quality flags describe problems without silently dropping
trips. Do not run concurrent bronze/silver writers during this demo load.
"""
import argparse
import os
import re

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F

SOURCE = "lakehouse.bronze.yellow_taxi_trips"
TARGET = "lakehouse.silver.yellow_taxi_trips"
MONEY = [
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge",
    "airport_fee", "cbd_congestion_fee",
]
BASE_TYPES = {
    "vendor_id": "bigint", "pickup_datetime": "timestamp",
    "dropoff_datetime": "timestamp", "passenger_count": "double",
    "trip_distance": "double", "ratecode_id": "bigint",
    "store_and_fwd_flag": "string", "pickup_location_id": "bigint",
    "dropoff_location_id": "bigint", "payment_type": "bigint",
    **{name: "double" for name in MONEY},
    "source_month": "string", "source_file": "string", "loaded_at": "timestamp",
}


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
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": "8",
        "spark.sql.ansi.enabled": "true",
    }
    builder = SparkSession.builder.appName("NYC Yellow Taxi: Bronze to Silver")
    for key, value in settings.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def flag_array(rules):
    # A null comparison is not a match; explicit missing-value checks are below.
    items = [F.when(F.coalesce(condition, F.lit(False)), F.lit(label))
             for label, condition in rules]
    return F.filter(F.array(*items), lambda item: item.isNotNull())


def transform(bronze):
    missing = sorted(set(BASE_TYPES) - set(bronze.columns))
    if missing:
        raise ValueError(f"Bronze table is missing columns: {missing}")
    df = bronze.select(*[F.col(name).cast(kind).alias(name)
                         for name, kind in BASE_TYPES.items()])
    df = (df.withColumnRenamed("loaded_at", "bronze_loaded_at")
          .withColumn("source_filename", F.regexp_extract("source_file", r"([^/]+)$", 1))
          .withColumn("store_and_fwd_flag", F.upper(F.trim("store_and_fwd_flag")))
          .withColumn("pickup_date", F.to_date("pickup_datetime"))
          .withColumn("dropoff_date", F.to_date("dropoff_datetime"))
          .withColumn("pickup_year", F.year("pickup_datetime"))
          .withColumn("pickup_month", F.month("pickup_datetime"))
          .withColumn("pickup_hour", F.hour("pickup_datetime"))
          # ISO weekday: Monday=1, Sunday=7. All time fields use UTC.
          .withColumn("pickup_day_of_week", ((F.dayofweek("pickup_datetime") + 5) % 7) + 1)
          .withColumn("is_weekend", F.col("pickup_day_of_week").isin(6, 7))
          .withColumn("trip_duration_minutes", F.round(
              (F.col("dropoff_datetime").cast("double") -
               F.col("pickup_datetime").cast("double")) / 60.0, 3)))
    errors = [
        ("MISSING_PICKUP_TIMESTAMP", F.col("pickup_datetime").isNull()),
        ("MISSING_DROPOFF_TIMESTAMP", F.col("dropoff_datetime").isNull()),
        ("DROPOFF_BEFORE_PICKUP", F.col("dropoff_datetime") < F.col("pickup_datetime")),
        ("MISSING_SOURCE_FILE", F.col("source_file").isNull() | (F.trim("source_file") == "")),
    ]
    warnings = [
        ("MISSING_PICKUP_LOCATION", F.col("pickup_location_id").isNull()),
        ("MISSING_DROPOFF_LOCATION", F.col("dropoff_location_id").isNull()),
        ("NONPOSITIVE_PICKUP_LOCATION", F.col("pickup_location_id") <= 0),
        ("NONPOSITIVE_DROPOFF_LOCATION", F.col("dropoff_location_id") <= 0),
        ("PICKUP_OUTSIDE_SOURCE_MONTH", F.date_format("pickup_datetime", "yyyy-MM") != F.col("source_month")),
        ("ZERO_DURATION", F.col("trip_duration_minutes") == 0),
         ("DURATION_OVER_24_HOURS", F.col("trip_duration_minutes") > 1440),
        ("UNKNOWN_STORE_AND_FWD_FLAG", ~F.col("store_and_fwd_flag").isin("Y", "N")),
    ]
    # Keep bronze numeric values (including fractional passenger counts) intact.
    # Monetary values retain bronze double types; no additional rounding here.
    for name in ["passenger_count", "trip_distance"] + MONEY:
        value = F.col(name)
        nonfinite = F.isnan(value) | (F.abs(value) == F.lit(float("inf")))
        errors.append((f"NONFINITE_{name.upper()}", nonfinite))
        warnings.append((f"NEGATIVE_{name.upper()}", value < 0))
    warnings.extend([
        ("MISSING_TRIP_DISTANCE", F.col("trip_distance").isNull()),
        ("ZERO_TRIP_DISTANCE", F.col("trip_distance") == 0),
        ("MISSING_PASSENGER_COUNT", F.col("passenger_count").isNull()),
        ("ZERO_PASSENGER_COUNT", F.col("passenger_count") == 0),
        ("FRACTIONAL_PASSENGER_COUNT", F.col("passenger_count") != F.floor("passenger_count")),
        ("MISSING_TOTAL_AMOUNT", F.col("total_amount").isNull()),
    ])
    return (df.withColumn("quality_errors", flag_array(errors))
            .withColumn("quality_warnings", flag_array(warnings))
            .withColumn("is_valid", F.size("quality_errors") == 0)
            .withColumn("has_quality_warnings", F.size("quality_warnings") > 0)
            .withColumn("silver_loaded_at", F.current_timestamp()))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", help="YYYY-MM; omit to process all months in bronze")
    args = parser.parse_args()
    if args.month and not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", args.month):
        parser.error("--month must be YYYY-MM")
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        # Pin reads to one bronze snapshot for consistent processing of all months.
        snapshots = spark.table(f"{SOURCE}.refs").where(F.col("name") == "main").select("snapshot_id").collect()
        if not snapshots:
            raise RuntimeError("Bronze table has no committed snapshot")
        bronze = (spark.read.format("iceberg").option("snapshot-id", str(snapshots[0]["snapshot_id"]))
                  .load(SOURCE))
        if args.month:
            bronze = bronze.where(F.col("source_month") == args.month)
        months = sorted([row["source_month"] for row in bronze.select("source_month").distinct().collect()],
                        key=lambda value: value or "")
        if not months:
            raise RuntimeError("No bronze rows found for the selected months")
        if any(month is None or not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", month) for month in months):
            raise ValueError("Bronze contains null or invalid source_month values")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
        schema = transform(bronze.limit(0)).schema
        ddl = ",\n".join(f"`{field.name}` {field.dataType.simpleString()}" for field in schema.fields)
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {TARGET} ({ddl})
            USING iceberg PARTITIONED BY (source_month)
            TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')""")
        for month in months:
            df = transform(bronze.where(F.col("source_month") == month)).persist(StorageLevel.DISK_ONLY)
            try:
                stats = df.agg(
                    F.count("*").alias("rows"),
                    F.sum(F.when(~F.col("is_valid"), 1).otherwise(0)).alias("invalid"),
                    F.sum(F.when(F.col("has_quality_warnings"), 1).otherwise(0)).alias("warnings"),
                ).first()
                if stats["rows"] == 0:
                    raise RuntimeError(f"{month}: empty source; refusing to replace silver")
                df.writeTo(TARGET).overwrite(F.col("source_month") == F.lit(month))
                print(f"[SUCCESS] {month}: {stats['rows']:,} rows, "
                      f"{stats['invalid']:,} invalid, {stats['warnings']:,} with warnings -> {TARGET}", flush=True)
            finally:
                df.unpersist()
        print("[DONE] Selected bronze months loaded to silver", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
