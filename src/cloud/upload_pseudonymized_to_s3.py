"""
src/cloud/upload_pseudonymized_to_s3.py

Final stage: upload the latest pseudonymized order history file and its
matching quality report to S3 — but only if:
  1. The quality report's batch_id matches the order file's batch_id
     (guards against pairing mismatched files from different runs), and
  2. The quality report's overall_status is "passed" or
     "passed_with_warnings" — never "failed".

This is the last gate before real (pseudonymized) data leaves the
machine, so every check here is a hard stop, not a warning.

Configuration (environment variables, with safe fallbacks):
    S3_BUCKET   - target bucket (REQUIRED — no safe default, will exit 2 if unset)
    S3_PREFIX   - key prefix (default: "order-intelligence")
    AWS_REGION  - boto3 region (default: "us-east-1")
"""

from __future__ import annotations

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

import json

S3_BUCKET = os.environ.get("S3_BUCKET")
S3_PREFIX = os.environ.get("S3_PREFIX", "order-intelligence")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

BATCH_ID_PATTERN = re.compile(r"(\d{8}_\d{6})")

ACCEPTABLE_QUALITY_STATUSES = {"passed", "passed_with_warnings"}


# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("upload_pseudonymized_to_s3")


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> int:
    if not S3_BUCKET:
        logger.error(
            "S3_BUCKET environment variable is not set. Refusing to guess a "
            "bucket name — set it explicitly, e.g.:\n"
            '  setx S3_BUCKET "your-real-bucket-name"   (Windows, new shells)\n'
            '  $env:S3_BUCKET = "your-real-bucket-name"  (current PowerShell session)'
        )
        return 2

    try:
        order_file = find_latest(
            PSEUDONYMIZED_DIR, "order_history_pseudonymized_*.csv", "pseudonymized order file"
        )
        quality_file = find_latest(
            # NOTE: pattern is deliberately "[0-9]*" not just "*" — the
            # quality_reports folder is now shared with open orders
            # (quality_report_open_orders_*.json) and inventory
            # (quality_report_inventory_*.json). A bare "*" wildcard would
            # match those too, and since sorted() sorts filenames as text
            # (digits sort before letters), the wrong dataset's report
            # could get picked as "latest" purely by alphabetical accident.
            QUALITY_DIR, "quality_report_[0-9]*.json", "quality report"
        )

        order_batch_id = extract_batch_id(order_file)
        quality_batch_id = extract_batch_id(quality_file)

        # Hard stop, not a warning — an order file and quality report from
        # different batches must never be paired together for upload.
        if quality_batch_id != order_batch_id:
            raise ValueError(
                f"Order file batch_id ({order_batch_id}) does not match quality "
                f"report batch_id ({quality_batch_id}). Refusing to upload "
                f"mismatched batches. Run data_quality_gate.py against the "
                f"current order file before uploading."
            )
        batch_id = order_batch_id

        # --- Quality gate check ---
        with open(quality_file, "r", encoding="utf-8") as f:
            quality_report = json.load(f)

        overall_status = quality_report.get("overall_status")
        logger.info(f"Quality report status for batch {batch_id}: {overall_status}")

        if overall_status not in ACCEPTABLE_QUALITY_STATUSES:
            raise RuntimeError(
                f"Quality gate BLOCKED upload for batch {batch_id}. "
                f"overall_status='{overall_status}'. Review {quality_file} before uploading."
            )

        if overall_status == "passed_with_warnings":
            logger.warning(
                f"Quality gate passed with warnings for batch {batch_id}. "
                f"Proceeding to upload — review the quality report for details."
            )
        else:
            logger.info("Quality gate passed. Proceeding to upload.")

        # --- Upload ---
        s3 = boto3.client("s3", region_name=AWS_REGION)

        uploads = [
            {
                "local_file": order_file,
                "s3_key": f"{S3_PREFIX}/landing/order_history/batch_id={batch_id}/{order_file.name}",
            },
            {
                "local_file": quality_file,
                "s3_key": f"{S3_PREFIX}/quality_reports/order_history/batch_id={batch_id}/{quality_file.name}",
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

        logger.info("S3 upload completed successfully.")
        return 0

    except Exception:
        logger.exception("S3 upload stage failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())