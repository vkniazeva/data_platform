import os
import time

import pyarrow as pa
import pyarrow.parquet as pq
import io

import boto3
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

import logging
logger = logging.getLogger(__name__)

class ParquetWriter:
    def __init__(self, topic):
        self.bucket = os.getenv("BUCKET_NAME")
        self.topic = topic

        self._buffer = []
        self.current_date = datetime.now(timezone.utc)

        self._s3_client = boto3.client(
            "s3",
            endpoint_url = os.getenv("SEAWEED_ENDPOINT_URL"),
            aws_access_key_id = os.getenv("SEAWEED_ROOT_USER"),
            aws_secret_access_key = os.getenv("SEAWEED_ROOT_PASSWORD")
        )


    def flush(self, event_date: str):
        if not self._buffer:
            return

        keys = self._buffer[0].keys()
        transposed = {key: [row[key] for row in self._buffer] for key in keys}
        table = pa.Table.from_pydict(transposed)

        buffer = io.BytesIO()
        pq.write_table(table, buffer)

        s3_key = f"{self.topic}/{event_date}.parquet"
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


    def write_to_buffer(self, event_dict: dict, event_date: str) -> None:
        if event_date != self.current_date:
            self.flush(event_date)
            self.current_date = event_date
        if len(self._buffer) >= 100:
            self.flush(event_date)
            logger.info("Flushed to S3")
        self._buffer.append(event_dict)




