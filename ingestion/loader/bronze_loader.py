import io
import time

import pyarrow.parquet as pq
import pyarrow as pa
import json

import logging
logger = logging.getLogger(__name__)

class BronzeLoader:
    def __init__(self, s3_client, ch_client):
        self.s3_client = s3_client
        self.ch_client = ch_client

    def load_fix_events(self, s3_path: str) -> None:
        t_0 = time.monotonic()
        response = self.s3_client.get_object(Bucket="raw-events", Key=s3_path)
        body = response["Body"].read()
        buffer = io.BytesIO(body)
        table = pq.read_table(buffer)
        table = table.append_column("source_file", pa.array([s3_path] * len(table)))
        table = table.append_column("pipeline_version", pa.array(["1.0"] * len(table)))
        t_1 = time.monotonic()
        self.ch_client.insert_arrow("bronze.fix_events", table)
        t_2 = time.monotonic()
        t_total = t_2 - t_0
        t_insert = t_2 - t_1
        logger.info(f"{len(table)} of records from {s3_path} are stored in the bronze.fix_events table")
        logger.info(f"Raw data processing time: {t_total * 1000:.1f} ms")
        logger.info(f"Raw data insert time: {t_insert * 1000:.1f} ms")

    def load_regional_events(self, s3_path: str) -> None:
        t_0 = time.monotonic()
        response = self.s3_client.get_object(Bucket="raw-events", Key=s3_path)
        body = response["Body"].read()
        buffer = io.BytesIO(body)
        table = pq.read_table(buffer)

        conditions_col = [json.dumps(row.as_py()) if row.is_valid else "{}" for row in table["conditions"]]
        table = table.set_column(table.schema.get_field_index("conditions"), "conditions", pa.array(conditions_col))
        table = table.append_column("source_file", pa.array([s3_path] * len(table)))
        table = table.append_column("pipeline_version", pa.array(["1.0"] * len(table)))
        t_1 = time.monotonic()
        self.ch_client.insert_arrow("bronze.regional_events", table)
        t_2 = time.monotonic()
        t_total = t_2 - t_0
        t_insert = t_2 - t_1
        logger.info(f"{len(table)} of records from {s3_path} are stored in the bronze.regional_events table")
        logger.info(f"Raw data processing time: {t_total * 1000:.1f} ms")
        logger.info(f"Raw data insert time: {t_insert * 1000:.1f} ms")

