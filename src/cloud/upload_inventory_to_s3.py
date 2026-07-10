"""
src/cloud/upload_inventory_to_s3.py

Uploads the latest pseudonymized inventory file and its matching
quality report to S3. Uses snapshot_date (not intraday batch_id) as the
linking identifier — same principle as data_quality_gate_inventory.py.

Configuration (same environment variables as the other upload scripts):
    S3_BUCKET, S3_PREFIX, AWS_REGION
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

SNAPSHOT_DATE_PATTERN = re.compile(r"(\d{8})")
ACCEPTABLE_QUALITY_STATUSES = {"passed", "passed_with_warnings"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("upload_inventory_to_s3")


def extract_snapshot_date(filepath: Path) -> str:
    match = SNAPSHOT_DATE_PATTERN.search(filepath.name)
    if not match:
        raise ValueError(f"Could not find snapshot_date in filename: {filepath.name}")
    return match.group(1)


def find_latest(directory: Path, pattern: str, label: str) -> Path:
    candidates = sorted(directory.glob(pattern))
    if not candidates:
        raise FileNotFoundError(f"No {label} found in {directory} matching '{pattern}'.")
    return candidates[-1]


def main() -> int:
    if not S3_BUCKET:
        logger.error("S3_BUCKET environment variable is not set. Refusing to guess a bucket name.")
        return 2

    try:
        inventory_file = find_latest(PSEUDONYMIZED_DIR, "inventory_pseudonymized_*.csv", "pseudonymized inventory file")
        quality_file = find_latest(QUALITY_DIR, "quality_report_inventory_*.json", "inventory quality report")

        inventory_date = extract_snapshot_date(inventory_file)
        quality_date = extract_snapshot_date(quality_file)

        if quality_date != inventory_date:
            raise ValueError(
                f"Inventory file snapshot_date ({inventory_date}) does not match "
                f"quality report snapshot_date ({quality_date}). Refusing to "
                f"upload mismatched snapshots. Run data_quality_gate_inventory.py "
                f"against the current inventory file before uploading."
            )
        snapshot_date = inventory_date

        with open(quality_file, "r", encoding="utf-8") as f:
            quality_report = json.load(f)

        overall_status = quality_report.get("overall_status")
        logger.info(f"Quality report status for inventory snapshot {snapshot_date}: {overall_status}")

        if overall_status not in ACCEPTABLE_QUALITY_STATUSES:
            raise RuntimeError(
                f"Quality gate BLOCKED upload for inventory snapshot {snapshot_date}. "
                f"overall_status='{overall_status}'. Review {quality_file} before uploading."
            )

        if overall_status == "passed_with_warnings":
            logger.warning(f"Quality gate passed with warnings for snapshot {snapshot_date}. Proceeding to upload.")
        else:
            logger.info("Quality gate passed. Proceeding to upload.")

        s3 = boto3.client("s3", region_name=AWS_REGION)

        uploads = [
            {
                "local_file": inventory_file,
                "s3_key": f"{S3_PREFIX}/landing/inventory/snapshot_date={snapshot_date}/{inventory_file.name}",
            },
            {
                "local_file": quality_file,
                "s3_key": f"{S3_PREFIX}/quality_reports/inventory/snapshot_date={snapshot_date}/{quality_file.name}",
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
                raise RuntimeError("AWS credentials not found. Configure them before running this script.")
            except ClientError as e:
                logger.error(f"FAILED: {local_file.name} - {e}")
                results.append({"file": local_file.name, "status": "failed", "error": str(e)})

        logger.info("Upload summary:")
        for r in results:
            logger.info(str(r))

        failures = [r for r in results if r["status"] != "success"]
        if failures:
            raise RuntimeError(f"{len(failures)} upload(s) did not complete successfully.")

        logger.info("Inventory S3 upload completed successfully.")
        return 0

    except Exception:
        logger.exception("Inventory S3 upload stage failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())