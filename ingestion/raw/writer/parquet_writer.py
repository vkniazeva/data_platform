import json
import os
import time

import pyarrow as pa
import pyarrow.parquet as pq
import io

import boto3
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

import logging
logger = logging.getLogger(__name__)

class ParquetWriter:
    def __init__(self, topic):
        self.bucket = os.getenv("BUCKET_NAME")
        self.topic = topic
        self._last_msg = None

        self._buffer = []
        self.current_date = ""

        self._s3_client = boto3.client(
            "s3",
            endpoint_url = os.getenv("SEAWEED_ENDPOINT_URL"),
            aws_access_key_id = os.getenv("SEAWEED_ROOT_USER"),
            aws_secret_access_key = os.getenv("SEAWEED_ROOT_PASSWORD")
        )


    def flush(self, event_date: str) -> str | None:
        """Serialize the buffer to Parquet and upload to S3; returns the S3 key or None if buffer was empty."""
        if not self._buffer:
            return None

        all_keys = set().union(*(row.keys() for row in self._buffer))
        def _to_str(v):
            if v is None:
                return None
            if isinstance(v, (dict, list)):
                return json.dumps(v)
            return str(v)

        transposed = {
            key: [_to_str(row.get(key)) for row in self._buffer]
            for key in all_keys
        }
        schema = pa.schema([(k, pa.string()) for k in all_keys])
        table = pa.Table.from_pydict(transposed, schema=schema)

        buffer = io.BytesIO()
        pq.write_table(table, buffer)

        ts_ms = int(time.time() * 1000)
        s3_key = f"{self.topic}/{event_date}/{ts_ms}.parquet"
        for attempt in range(3):
            try:
                buffer.seek(0)
                t0 = time.monotonic()
                self._s3_client.put_object(
                    Bucket=self.bucket,
                    Key=s3_key,
                    Body=buffer.read()
                )
                elapsed = time.monotonic() - t0
                break
            except Exception as e:
                if attempt < 2:
                    logger.warning(f"Attempt {attempt + 1} failed: {e}, retrying...")
                    time.sleep(attempt + 1)
                else:
                    logger.error(f"All attempts failed: {e}")
                    raise

        count = len(self._buffer)
        size_kb = buffer.tell() / 1024
        logger.info(f"S3 write: {count} messages, {size_kb:.1f} KB → {s3_key} in {elapsed * 1000:.1f} ms")
        self._buffer = []
        return s3_key



    def write_to_buffer(self, event_dict: dict, event_date: str, msg) -> str | None:
        """Append event to buffer; flushes automatically on date change or when buffer reaches 100 messages."""
        self._last_msg = msg
        if event_date != self.current_date:
            key = self.flush(self.current_date) if self.current_date else None
            self.current_date = event_date
            self._buffer.append(event_dict)
            return key
        self._buffer.append(event_dict)
        if len(self._buffer) >= 100:
            return self.flush(event_date)
        return None




