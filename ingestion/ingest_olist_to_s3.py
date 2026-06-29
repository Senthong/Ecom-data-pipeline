"""
ingestion/ingest_olist_to_s3.py

Downloads the Olist Brazilian E-Commerce dataset from Kaggle and
uploads raw CSV files to an S3 raw bucket with a date-partitioned prefix.

Usage:
    python ingestion/ingest_olist_to_s3.py --date 2024-01-15
    python ingestion/ingest_olist_to_s3.py  # uses today's date
"""

import argparse
import hashlib
import logging
import os
import zipfile
from datetime import date, datetime
from pathlib import Path

import boto3
import pandas as pd
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("olist_ingestion")

# CONFIG

KAGGLE_DATASET = "olistbr/brazilian-ecommerce"
LOCAL_DOWNLOAD_DIR = Path("/tmp/olist_raw")
S3_BUCKET = os.environ.get("S3_RAW_BUCKET", "ecom-pipeline-raw-dev")
S3_PREFIX = "olist"

OLIST_FILES = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_products_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_geolocation_dataset.csv",
    "product_category_name_translation.csv",
]

# HELPERS

def get_s3_client():
    return boto3.client(
        "s3",
        region_name=os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-2"),
    )


def compute_md5(file_path: Path) -> str:
    """Compute MD5 for upload integrity check."""
    h = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def download_olist_dataset(download_dir: Path) -> Path:
    """
    Download Olist dataset using Kaggle API.
    Requires ~/.kaggle/kaggle.json or KAGGLE_USERNAME + KAGGLE_KEY env vars.
    """
    import kaggle  # noqa: F401 — triggers auth validation on import

    download_dir.mkdir(parents=True, exist_ok=True)
    zip_path = download_dir / "brazilian-ecommerce.zip"

    if zip_path.exists():
        logger.info("Zip already downloaded, skipping download.")
        return zip_path

    logger.info(f"Downloading dataset '{KAGGLE_DATASET}' from Kaggle...")
    os.system(
        f"kaggle datasets download -d {KAGGLE_DATASET} -p {download_dir} --unzip"
    )
    logger.info("Download complete.")
    return download_dir


def validate_csv(file_path: Path, table_name: str) -> dict:
    """Run basic validation and return stats."""
    df = pd.read_csv(file_path, nrows=5000)  # sample for speed
    stats = {
        "table": table_name,
        "row_count_sample": len(df),
        "column_count": len(df.columns),
        "columns": list(df.columns),
        "null_pct": (df.isnull().sum() / len(df) * 100).to_dict(),
    }
    logger.info(
        f"  ✓ {table_name}: {stats['column_count']} cols, "
        f"{stats['row_count_sample']} rows (sample)"
    )
    return stats


def upload_to_s3(
    s3_client,
    local_path: Path,
    bucket: str,
    s3_key: str,
) -> bool:
    """Upload a file to S3 with metadata tags."""
    try:
        file_size = local_path.stat().st_size
        md5 = compute_md5(local_path)

        logger.info(f"  ↑ Uploading {local_path.name} → s3://{bucket}/{s3_key}")
        s3_client.upload_file(
            str(local_path),
            bucket,
            s3_key,
            ExtraArgs={
                "ContentType": "text/csv",
                "Metadata": {
                    "source": "kaggle-olist",
                    "md5": md5,
                    "file_size_bytes": str(file_size),
                    "ingested_at": datetime.utcnow().isoformat(),
                },
            },
        )
        return True
    except ClientError as e:
        logger.error(f"  ✗ Failed to upload {local_path.name}: {e}")
        return False


def check_already_ingested(s3_client, bucket: str, prefix: str) -> bool:
    """Return True if data for this partition already exists in S3."""
    try:
        resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        return resp.get("KeyCount", 0) > 0
    except ClientError:
        return False


# 
# MAIN
# 

def ingest(run_date: date, force: bool = False) -> dict:
    """
    Full ingestion run:
    1. Download from Kaggle
    2. Validate CSVs
    3. Upload to S3
    Returns a summary dict.
    """
    s3_client = get_s3_client()
    year = run_date.strftime("%Y")
    month = run_date.strftime("%m")
    day = run_date.strftime("%d")
    partition_prefix = f"{S3_PREFIX}/year={year}/month={month}/day={day}/"

    # Idempotency check
    if not force and check_already_ingested(s3_client, S3_BUCKET, partition_prefix):
        logger.info(
            f"Data for {run_date} already exists at s3://{S3_BUCKET}/{partition_prefix}. "
            "Skipping. Use --force to re-ingest."
        )
        return {"status": "skipped", "date": str(run_date)}

    # Download
    data_dir = download_olist_dataset(LOCAL_DOWNLOAD_DIR)

    results = {"status": "success", "date": str(run_date), "files": []}
    validation_stats = []

    logger.info("Starting upload to S3...")
    for filename in OLIST_FILES:
        local_file = LOCAL_DOWNLOAD_DIR / filename
        if not local_file.exists():
            logger.warning(f"  ⚠ File not found locally: {filename}, skipping.")
            continue

        # Validate
        table_name = filename.replace(".csv", "")
        stats = validate_csv(local_file, table_name)
        validation_stats.append(stats)

        # Upload
        s3_key = f"{partition_prefix}{filename}"
        success = upload_to_s3(s3_client, local_file, S3_BUCKET, s3_key)

        results["files"].append(
            {
                "file": filename,
                "s3_key": s3_key,
                "success": success,
                "rows_sample": stats["row_count_sample"],
            }
        )

    successful = sum(1 for f in results["files"] if f["success"])
    logger.info(
        f"\n{''*60}\n"
        f"Ingestion complete: {successful}/{len(results['files'])} files uploaded\n"
        f"S3 prefix: s3://{S3_BUCKET}/{partition_prefix}\n"
        f"{''*60}"
    )

    # Write manifest
    manifest_key = f"{partition_prefix}_manifest.json"
    import json
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=manifest_key,
        Body=json.dumps(results, indent=2),
        ContentType="application/json",
    )
    logger.info(f"Manifest written to s3://{S3_BUCKET}/{manifest_key}")

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest Olist data to S3")
    parser.add_argument(
        "--date",
        type=str,
        default=date.today().isoformat(),
        help="Run date YYYY-MM-DD (default: today)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-ingest even if partition already exists",
    )
    args = parser.parse_args()

    run_date = date.fromisoformat(args.date)
    result = ingest(run_date, force=args.force)
    print(result)
