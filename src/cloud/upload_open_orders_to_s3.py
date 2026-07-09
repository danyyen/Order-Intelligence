"""
src/cloud/upload_open_orders_to_s3.py

Uploads the latest pseudonymized open orders file and its matching
quality report to S3 — mirrors upload_pseudonymized_to_s3.py (order
history), with its own S3 key prefix so the two datasets land in
separate, clearly-labeled paths rather than colliding.

Same two hard gates as the order history uploader:
  1. Quality report batch_id must match the open orders file's batch_id.
  2. Quality report overall_status must be "passed" or "passed_with_warnings".

Configuration (environment variables — same ones the order history
uploader uses, so no separate config needed if you're running both):
    S3_BUCKET   - target bucket (REQUIRED)
    S3_PREFIX   - key prefix (default: "order-intelligence")
    AWS_REGION  - boto3 region (default: "us-east-1")
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
from pathlib import Path

import boto3
from botocore.exceptions import NoCredentialsError, ClientError

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.paths import PSEUDONYMIZED_DIR, QUALITY_DIR

S3_BUCKET = os.environ.get("S3_BUCKET")
S3_PREFIX = os.environ.get("S3_PREFIX", "order-intelligence")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

BATCH_ID_PATTERN = re.compile(r"(\d{8}_\d{6})")
ACCEPTABLE_QUALITY_STATUSES = {"passed", "passed_with_warnings"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("upload_open_orders_to_s3")


def extract_batch_id(filepath: Path) -> str:
    match = BATCH_ID_PATTERN.search(filepath.name)
    if not match:
        raise ValueError(f"Could not find batch_id in filename: {filepath.name}")
    return match.group(1)


def find_latest(directory: Path, pattern: str, label: str) -> Path:
    candidates = sorted(directory.glob(pattern))
    if not candidates:
        raise FileNotFoundError(f"No {label} found in {directory} matching '{pattern}'.")
    return candidates[-1]


def main() -> int:
    if not S3_BUCKET:
        logger.error(
            "S3_BUCKET environment variable is not set. Refusing to guess a "
            "bucket name — set it explicitly before running."
        )
        return 2

    try:
        order_file = find_latest(
            PSEUDONYMIZED_DIR, "open_orders_pseudonymized_*.csv", "pseudonymized open orders file"
        )
        quality_file = find_latest(
            QUALITY_DIR, "quality_report_open_orders_*.json", "open orders quality report"
        )

        order_batch_id = extract_batch_id(order_file)
        quality_batch_id = extract_batch_id(quality_file)

        if quality_batch_id != order_batch_id:
            raise ValueError(
                f"Open orders file batch_id ({order_batch_id}) does not match "
                f"quality report batch_id ({quality_batch_id}). Refusing to "
                f"upload mismatched batches. Run data_quality_gate_open_orders.py "
                f"against the current open orders file before uploading."
            )
        batch_id = order_batch_id

        with open(quality_file, "r", encoding="utf-8") as f:
            quality_report = json.load(f)

        overall_status = quality_report.get("overall_status")
        logger.info(f"Quality report status for open orders batch {batch_id}: {overall_status}")

        if overall_status not in ACCEPTABLE_QUALITY_STATUSES:
            raise RuntimeError(
                f"Quality gate BLOCKED upload for open orders batch {batch_id}. "
                f"overall_status='{overall_status}'. Review {quality_file} before uploading."
            )

        if overall_status == "passed_with_warnings":
            logger.warning(
                f"Quality gate passed with warnings for open orders batch {batch_id}. "
                f"Proceeding to upload — review the quality report for details."
            )
        else:
            logger.info("Quality gate passed. Proceeding to upload.")

        s3 = boto3.client("s3", region_name=AWS_REGION)

        uploads = [
            {
                "local_file": order_file,
                "s3_key": f"{S3_PREFIX}/landing/open_orders/batch_id={batch_id}/{order_file.name}",
            },
            {
                "local_file": quality_file,
                "s3_key": f"{S3_PREFIX}/quality_reports/open_orders/batch_id={batch_id}/{quality_file.name}",
            },
        ]

        results = []
        for item in uploads:
            local_file = item["local_file"]
            s3_key = item["s3_key"]

            if not local_file.exists():
                logger.error(f"SKIPPED (file not found): {local_file}")
                results.append({"file": local_file.name, "status": "missing"})
                continue

            logger.info(f"Uploading {local_file} -> s3://{S3_BUCKET}/{s3_key}")
            try:
                s3.upload_file(Filename=str(local_file), Bucket=S3_BUCKET, Key=s3_key)
                logger.info(f"Uploaded: s3://{S3_BUCKET}/{s3_key}")
                results.append({"file": local_file.name, "status": "success", "s3_key": s3_key})
            except NoCredentialsError:
                raise RuntimeError(
                    "AWS credentials not found. Configure them via `aws configure`, "
                    "environment variables, or an IAM role before running this script."
                )
            except ClientError as e:
                logger.error(f"FAILED: {local_file.name} - {e}")
                results.append({"file": local_file.name, "status": "failed", "error": str(e)})

        logger.info("Upload summary:")
        for r in results:
            logger.info(str(r))

        failures = [r for r in results if r["status"] != "success"]
        if failures:
            raise RuntimeError(f"{len(failures)} upload(s) did not complete successfully. See summary above.")

        logger.info("Open orders S3 upload completed successfully.")
        return 0

    except Exception:
        logger.exception("Open orders S3 upload stage failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())