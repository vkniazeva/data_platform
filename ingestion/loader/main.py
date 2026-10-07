import os
from dotenv import load_dotenv
import boto3
import clickhouse_connect

import logging

from ingestion.loader.bronze_loader import BronzeLoader

logger = logging.getLogger(__name__)

load_dotenv()

def main():
    logging.basicConfig(level=logging.INFO)
    s3_client = boto3.client(
        "s3",
        endpoint_url=os.getenv("SEAWEED_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("SEAWEED_ROOT_USER"),
        aws_secret_access_key=os.getenv("SEAWEED_ROOT_PASSWORD")
    )

    ch_client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST"),
        port=int(os.getenv("CLICKHOUSE_PORT")),
        username=os.getenv("CLICKHOUSE_USERNAME"),
        password=os.getenv("CLICKHOUSE_PASSWORD") or ""
    )

    fix_files = s3_client.list_objects_v2(Bucket="raw-events", Prefix="market.fix.raw/")
    regional_files = s3_client.list_objects_v2(Bucket="raw-events", Prefix="market.events/")

    bronze_loader = BronzeLoader(s3_client, ch_client)

    for file in fix_files["Contents"]:
        bronze_loader.load_fix_events(file["Key"])

    for file in regional_files["Contents"]:
        bronze_loader.load_regional_events(file["Key"])


if __name__ == "__main__":
    main()