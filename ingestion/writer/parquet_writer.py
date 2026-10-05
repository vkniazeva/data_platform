import os
import pyarrow as pa
import pyarrow.parquet as pq
import io

import boto3
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

class ParquetWriter:
    def __init__(self, bucket, topic):
        self.bucket = bucket
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
        buffer.seek(0)

        self._s3_client.put_object(
            Bucket=self.bucket,
            Key=f"{self.topic}/{event_date}.parquet",
            Body=buffer.read()
        )
        self._buffer = []



    def write_to_buffer(self, event_dict: dict, event_date: str) -> None:
        if event_date != self.current_date:
            self.flush(event_date)
            self.current_date = event_date
        if len(self._buffer) >= 100:
            self.flush(event_date)
        self._buffer.append(event_dict)




