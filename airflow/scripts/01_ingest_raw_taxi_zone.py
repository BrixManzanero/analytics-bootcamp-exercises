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
    "misc/taxi_zone_lookup.csv"
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
# Download + Upload
# ============================================================
def ingest_file():

    filename = "taxi_zone_lookup.csv"
    object_key = f"{PREFIX}/{filename}"
    source_url = SOURCE_URL

    s3 = get_s3_client()

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
# Main
# ============================================================
def main():
    ingest_file()


if __name__ == "__main__":
    main()