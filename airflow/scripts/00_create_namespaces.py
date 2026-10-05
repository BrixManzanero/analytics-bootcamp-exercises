import os 
from pyspark.sql import SparkSession

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

def main():
    spark = build_spark("Create Namespaces")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    spark.stop()

if __name__ == "__main__":
    main()