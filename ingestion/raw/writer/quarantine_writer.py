import io
import json
import os
import time
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

load_dotenv()

import logging
logger = logging.getLogger(__name__)


class QuarantineWriter:
    def __init__(self, topic: str):
        self.topic = topic
        self.bucket = os.getenv("BUCKET_NAME")
        self._buffer = []
        self._current_date = ""
        self._s3_client = boto3.client(
            "s3",
            endpoint_url=os.getenv("SEAWEED_ENDPOINT_URL"),
            aws_access_key_id=os.getenv("SEAWEED_ROOT_USER"),
            aws_secret_access_key=os.getenv("SEAWEED_ROOT_PASSWORD"),
        )

    def write(self, event: dict, event_date: str, validation_error: str, msg) -> None:
        headers = {k: v.decode("utf-8") for k, v in (msg.headers() or [])}
        record = {
            **{str(k): str(v) for k, v in event.items()},
            "validation_error": validation_error,
            "original_headers": json.dumps(headers),
            "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
            "kafka_offset": msg.offset(),
            "kafka_partition": msg.partition(),
        }

        if event_date != self._current_date:
            if self._current_date:
                self._flush(self._current_date)
            self._current_date = event_date

        self._buffer.append(record)
        if len(self._buffer) >= 100:
            self._flush(event_date)

    def _flush(self, event_date: str) -> None:
        if not self._buffer:
            return

        keys = self._buffer[0].keys()
        transposed = {key: [row[key] for row in self._buffer] for key in keys}
        table = pa.Table.from_pydict(transposed)

        buf = io.BytesIO()
        pq.write_table(table, buf)

        ts_ms = int(time.time() * 1000)
        s3_key = f"quarantine/{self.topic}/{event_date}/{ts_ms}.parquet"
        buf.seek(0)
        self._s3_client.put_object(Bucket=self.bucket, Key=s3_key, Body=buf.read())
        count = len(self._buffer)
        logger.info(f"[quarantine] flushed {count} records → {s3_key}")
        self._buffer = []

    def flush_remaining(self) -> None:
        if self._buffer and self._current_date:
            self._flush(self._current_date)
