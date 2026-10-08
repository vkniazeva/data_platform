import io
import os
from datetime import date, timedelta

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

import logging
logger = logging.getLogger(__name__)

load_dotenv()

TOPICS = ["market.fix.raw", "market.events"]
COMPACTED_KEY = "compacted.parquet"


def _make_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("SEAWEED_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("SEAWEED_ROOT_USER"),
        aws_secret_access_key=os.getenv("SEAWEED_ROOT_PASSWORD"),
    )


def compact_partition(s3, bucket: str, topic: str, partition_date: str) -> None:
    prefix = f"{topic}/{partition_date}/"

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    keys = [
        obj["Key"]
        for page in pages
        for obj in page.get("Contents", [])
        if not obj["Key"].endswith(COMPACTED_KEY)
    ]

    if not keys:
        logger.info(f"[{topic}/{partition_date}] nothing to compact")
        return

    if len(keys) == 1:
        logger.info(f"[{topic}/{partition_date}] single file, skipping")
        return

    tables = []
    for key in sorted(keys):
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        tables.append(pq.read_table(io.BytesIO(body)))

    compacted = pa.concat_tables(tables)

    buf = io.BytesIO()
    pq.write_table(compacted, buf)
    buf.seek(0)

    compacted_key = f"{prefix}{COMPACTED_KEY}"
    s3.put_object(Bucket=bucket, Key=compacted_key, Body=buf.read())
    logger.info(f"[{topic}/{partition_date}] compacted {len(keys)} files → {len(compacted)} rows → {compacted_key}")

    for key in keys:
        s3.delete_object(Bucket=bucket, Key=key)
        logger.debug(f"deleted {key}")


def main(target_date: str | None = None) -> None:
    logging.basicConfig(level=logging.INFO)

    if target_date is None:
        target_date = (date.today() - timedelta(days=1)).isoformat()

    bucket = os.getenv("BUCKET_NAME", "raw-events")
    s3 = _make_s3_client()

    for topic in TOPICS:
        try:
            compact_partition(s3, bucket, topic, target_date)
        except Exception as e:
            logger.error(f"[{topic}/{target_date}] compaction failed: {e}", exc_info=True)


if __name__ == "__main__":
    import sys
    main(sys.argv[1] if len(sys.argv) > 1 else None)
