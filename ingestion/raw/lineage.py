from datetime import datetime, timezone


def build_lineage(msg, parser_version: str, producer_id: str) -> dict:
    """Return evidence bundle dict to merge into a parsed event before writing to Parquet."""
    schema_id = None
    if msg.headers():
        for key, value in msg.headers():
            if key == "schema_id":
                schema_id = value.decode("utf-8")
                break

    return {
        "_ingested_at": datetime.now(timezone.utc).isoformat(),
        "_producer_id": producer_id,
        "_parser_version": parser_version,
        "_schema_id": schema_id,
        "_kafka_offset": msg.offset(),
        "_kafka_partition": msg.partition(),
    }
