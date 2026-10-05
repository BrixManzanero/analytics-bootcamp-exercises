import argparse
import tempfile
from pathlib import Path

import boto3
import requests
from botocore.exceptions import ClientError

# ============================================================
# Configuration
# ============================================================

SOURCE_URL = (
    "https://d37ci6vzurychx.cloudfront.net/"
    "trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
)

S3_ENDPOINT = "http://asb-seaweedfs:8333"

AWS_ACCESS_KEY_ID = "admin"
AWS_SECRET_ACCESS_KEY = "password"
AWS_REGION = "us-east-1"

BUCKET = "data-lake"
PREFIX = "nyc_yellow_taxi"

CHUNK_SIZE = 1024 * 1024
TIMEOUT = 120


# ============================================================
# S3 Client
# ============================================================
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

# ============================================================
# Check whether object already exists
# ============================================================
def object_exists(s3, bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True

    except ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 404:
            return False
        raise

# ============================================================
# Validate Parquet
# ============================================================
def is_valid_parquet(file_path: Path):
    try:
        if file_path.stat().st_size < 8:
            return False
        
        with file_path.open("rb") as f:
            # Header
            if f.read(4) != b"PAR1":
                return False

            # Footer
            f.seek(-4, 2)
            if f.read(4) != b"PAR1":
                return False

        return True

    except OSError:
        return False


# ============================================================
# Download + Upload
# ============================================================
def ingest_file(year, month):

    filename = (
        f"yellow_tripdata_"
        f"{year}-{month:02d}.parquet"
    )

    object_key = f"{PREFIX}/{filename}"

    source_url = SOURCE_URL.format(year=year, month=month)

    s3 = get_s3_client()

    # --------------------------------------------------------
    # Skip if object already exists
    # --------------------------------------------------------

    if object_exists(
        s3,
        BUCKET,
        object_key,
    ):

        print(
            f"[SKIP] "
            f"s3://{BUCKET}/{object_key} "
            f"already exists."
        )

        return

    # --------------------------------------------------------
    # Temporary local file
    # --------------------------------------------------------
    with tempfile.TemporaryDirectory() as temp_dir:

        temp_file = Path(temp_dir) / filename

        print(
            f"[DOWNLOAD] {source_url}"
        )

        # ----------------------------------------------------
        # Download
        # ----------------------------------------------------
        with requests.get(
            source_url,
            stream=True,
            timeout=TIMEOUT,
        ) as response:

            if response.status_code == 404:

                raise FileNotFoundError(
                    f"Dataset not available: "
                    f"{source_url}"
                )

            response.raise_for_status()

            with temp_file.open("wb") as f:

                for chunk in response.iter_content(
                    chunk_size=CHUNK_SIZE
                ):

                    if chunk:
                        f.write(chunk)

        # ----------------------------------------------------
        # Validate Parquet
        # ----------------------------------------------------
        if not is_valid_parquet(
            temp_file
        ):

            raise ValueError(
                f"{filename} is not "
                "a valid Parquet file."
            )

        size_mb = (
            temp_file.stat().st_size
            / 1024
            / 1024
        )

        print(
            f"[VALID] {filename} "
            f"({size_mb:.2f} MB)"
        )

        # ----------------------------------------------------
        # Upload to SeaweedFS
        # ----------------------------------------------------
        print(
            f"[UPLOAD] "
            f"s3://{BUCKET}/{object_key}"
        )

        s3.upload_file(
            str(temp_file),
            BUCKET,
            object_key,
        )

        print(
            f"[SUCCESS] "
            f"s3://{BUCKET}/{object_key}"
        )


# ============================================================
# CLI
# ============================================================
def parse_arguments():

    parser = argparse.ArgumentParser(
        description=(
            "Download NYC TLC Yellow Taxi "
            "data to SeaweedFS."
        )
    )

    parser.add_argument(
        "--year",
        type=int,
        required=True,
    )

    parser.add_argument(
        "--month",
        type=int,
        required=True,
        choices=range(1, 13),
    )

    return parser.parse_args()


# ============================================================
# Main
# ============================================================
def main():
    args = parse_arguments()
    ingest_file(year=args.year, month=args.month)


if __name__ == "__main__":
    main()