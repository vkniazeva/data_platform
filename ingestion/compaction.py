import io
import json
import os
import re
from datetime import date, datetime, timedelta, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

import logging
logger = logging.getLogger(__name__)

load_dotenv()

TOPICS = ["market.fix.raw", "market.events"]
MANIFEST_KEY = "compaction_manifest.json"
_VERSION_RE = re.compile(r"compacted\.v(\d+)\.parquet$")


def _version(key: str) -> int | None:
    m = _VERSION_RE.search(key)
    return int(m.group(1)) if m else None


def get_latest_compacted_key(s3, bucket: str, topic: str, partition_date: str) -> str | None:
    prefix = f"{topic}/{partition_date}/"
    paginator = s3.get_paginator("list_objects_v2")
    versioned = [
        (obj["Key"], _version(obj["Key"]))
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
        if _version(obj["Key"]) is not None
    ]
    if not versioned:
        return None
    return max(versioned, key=lambda x: x[1])[0]


def compact_partition(s3, bucket: str, topic: str, partition_date: str) -> None:
    prefix = f"{topic}/{partition_date}/"

    paginator = s3.get_paginator("list_objects_v2")
    all_keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
        if not obj["Key"].endswith(MANIFEST_KEY)
    ]

    versioned = [(k, _version(k)) for k in all_keys if _version(k) is not None]
    flush_keys = sorted(k for k in all_keys if _version(k) is None)

    if not flush_keys:
        logger.info(f"[{topic}/{partition_date}] no new flush files, skipping")
        return

    latest_key, latest_version = max(versioned, key=lambda x: x[1]) if versioned else (None, 0)

    # read previous compacted version + new flush files, track expected row count
    tables = []
    expected_rows = 0

    if latest_key:
        body = s3.get_object(Bucket=bucket, Key=latest_key)["Body"].read()
        prev = pq.read_table(io.BytesIO(body))
        tables.append(prev)
        expected_rows += len(prev)

    for key in flush_keys:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        t = pq.read_table(io.BytesIO(body))
        tables.append(t)
        expected_rows += len(t)

    compacted = pa.concat_tables(tables)

    if len(compacted) != expected_rows:
        raise RuntimeError(
            f"[{topic}/{partition_date}] row count mismatch before write: "
            f"got {len(compacted)}, expected {expected_rows}"
        )

    new_version = latest_version + 1
    new_key = f"{prefix}compacted.v{new_version}.parquet"

    buf = io.BytesIO()
    pq.write_table(compacted, buf)
    buf.seek(0)
    s3.put_object(Bucket=bucket, Key=new_key, Body=buf.read())

    # verify written file before deleting anything
    written = pq.read_table(io.BytesIO(s3.get_object(Bucket=bucket, Key=new_key)["Body"].read()))
    if len(written) != expected_rows:
        s3.delete_object(Bucket=bucket, Key=new_key)
        raise RuntimeError(
            f"[{topic}/{partition_date}] written file has {len(written)} rows, "
            f"expected {expected_rows} — rolled back"
        )

    # write manifest for lineage
    manifest = {
        "compacted_file": new_key,
        "previous_version": latest_key,
        "source_flush_files": flush_keys,
        "row_count": len(written),
        "compacted_at": datetime.now(timezone.utc).isoformat(),
    }
    s3.put_object(
        Bucket=bucket,
        Key=f"{prefix}{MANIFEST_KEY}",
        Body=json.dumps(manifest, indent=2).encode(),
    )

    # safe to delete only after new version is verified
    keys_to_delete = flush_keys + ([latest_key] if latest_key else [])
    for key in keys_to_delete:
        s3.delete_object(Bucket=bucket, Key=key)

    logger.info(
        f"[{topic}/{partition_date}] v{new_version}: "
        f"{len(flush_keys)} flush files + prev → {len(written)} rows → {new_key}"
    )


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


def _make_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("SEAWEED_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("SEAWEED_ROOT_USER"),
        aws_secret_access_key=os.getenv("SEAWEED_ROOT_PASSWORD"),
    )


if __name__ == "__main__":
    import sys
    main(sys.argv[1] if len(sys.argv) > 1 else None)
