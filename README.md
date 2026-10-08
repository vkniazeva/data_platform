# metals-data-platform

A local proof-of-concept data platform for commodity market data, built to explore ingestion, replay and idempotency patterns. Runs on a laptop with Docker.

## What it does

A generator produces synthetic data in the style of trading prices. A consumer ingests it, stores the raw events for replay, and loads parsed data into an analytical store.

| Data                             | Shape                   | Transport          |
|----------------------------------|-------------------------|--------------------|
| Trades (FIX messages)            | unstructured text       | Kafka / Redpanda   |
| Trade details                    | semi-structured         | Kafka / Redpanda   |
| Warehouse and barge stock levels | structured              | REST API           |
| Barge positions                  | geospatial, batch files | Kafka / Redpanda   |

## Architecture

generator -> Kafka -> ingestion service -> raw Parquet (object storage, immutable)
                                        -> parsed tables (ClickHouse)
generator -> REST API -> ingestion service -> same

## Design notes

- Idempotency: every event has a natural key, so duplicates and re-sends do not create duplicate rows.
- Replay: raw events are stored as immutable files, so any downstream table can be rebuilt.
- Writes never append to existing files: each batch goes to a new, uniquely named file.

## Ingestion service

The ingestion service runs two parallel workers — one per topic — in a single process using `threading`. Each worker is an independent Kafka consumer in its own consumer group.

**Partition strategy:**
- `market.fix.raw` — 4 partitions, partition key: `metal_id`
- `market.events` — 4 partitions, partition key: `metal_id_region`
- 2 active partitions per topic now, 2 reserved for horizontal scaling
- Adding more workers redistributes partitions automatically via Kafka rebalancing

**Buffering:**
- Messages are buffered in memory and flushed to SeaweedFS as Parquet every 100 messages or on day change
- Files are partitioned by event date (from the event itself, not processing time) and topic: `{topic}/{YYYY-MM-DD}.parquet`

**Error handling:**
- Graceful shutdown on Ctrl+C: buffer is flushed and consumer is closed cleanly before exit
- Worker crash recovery: each worker restarts automatically after 5 seconds if it crashes
- S3 write retry: up to 3 attempts with increasing delay (1s, 2s) before raising
- Manual offset commit: offset is committed only after successful write to S3, guaranteeing at-least-once delivery

**Planned improvements:**
- Batch reads from Kafka for backpressure control
- Dead letter queue for poison messages

## Performance

Measured on a laptop (Apple M-series, Docker, local SeaweedFS and ClickHouse).

**Kafka → S3 (ingestion service):**
| Metric | Value |
|--------|-------|
| Throughput (catching up on backlog) | ~265–446 msg/sec |
| Throughput (steady state, generator-bound) | ~25–50 msg/sec |
| Message parse time | < 0.2 ms |
| S3 write (100 messages, ~7 KB) | 5–15 ms |
| Kafka offset commit (after every 100 messages) | ~300 ms |

**S3 → ClickHouse (bronze loader):**
| Metric | Value |
|--------|-------|
| Total pipeline time (S3 read + transform + CH insert, 100 rows) | 58–75 ms |
| ClickHouse insert time (100 rows) | 55–70 ms |
| First file cold start (connection overhead) | ~375–800 ms |

**End-to-end latency (event produced → visible in ClickHouse):**

Data flows through two independent stages with different triggers:

1. **Event → S3**: buffer flushes every 100 messages. At 25–50 msg/sec per topic, this means **2–4 seconds** under normal load. At low generator rates (1–2 msg/sec) latency grows to **50–100 seconds**.
2. **S3 → ClickHouse**: the bronze loader is a batch job triggered manually. Until it runs, data is not in ClickHouse. Each run takes **60–75 ms per 100 rows**.

Total latency is therefore dominated by how often the bronze loader is scheduled. With continuous ingestion and a loader running every minute, typical end-to-end latency is **~1 minute**. This is a deliberate trade-off: raw events land in immutable Parquet first for replay, ClickHouse is a derived view.

## Roadmap

- [x] Data generator
- [x] Kafka / Redpanda locally
- [x] Ingestion service, raw Parquet in object storage (SeaweedFS)
- [x] ClickHouse bronze loading
- [ ] Silver / gold layers (dbt)
- [ ] Spark and Iceberg experiments
- [ ] API data processing

## Quick start

**1. Start the stack**
```bash
make up
```

**2. Create topics**
```bash
make topics
```

**3. Start the generator** (terminal 1)
```bash
make generator
```

**4. Start the ingestion service** (terminal 2)
```bash
python -m ingestion.main
```

**5. Check Redpanda Console**

Open http://localhost:8080

**6. Verify data in object storage**
```bash
# list files
aws --endpoint-url http://localhost:8333 s3 ls s3://raw-events/ --recursive

# download and inspect a parquet file
aws --endpoint-url http://localhost:8333 s3 cp s3://raw-events/market.fix.raw/2026-10-06.parquet /tmp/fix.parquet
python3 -c "import pyarrow.parquet as pq; print(pq.read_table('/tmp/fix.parquet').to_pandas())"

aws --endpoint-url http://localhost:8333 s3 cp s3://raw-events/market.events/2026-10-06.parquet /tmp/events.parquet
python3 -c "import pyarrow.parquet as pq; print(pq.read_table('/tmp/events.parquet').to_pandas())"
```

**7. Consume raw messages from Redpanda**
```bash
make consume-fix
make consume-events
```

**8. Tear down**
```bash
make down
```

## Events structure

### FIX event

```json 
{
  "topic": "market.fix.raw",
  "key": "AH",
  "value": "8=FIX.4.4|35=8|34=2052|52=20261005-21:12:26.047|17=EX-002052|55=AH|54=1|32=33|31=2664.29|60=20261005-21:04:16.047|",
  "headers": [
    {
      "key": "event_type",
      "value": "trade_execution_report"
    },
    {
      "key": "schema_version",
      "value": "1"
    },
    {
      "key": "source",
      "value": "synthetic_fix"
    },
    {
      "key": "content_type",
      "value": "text/plain"
    },
    {
      "key": "idempotency_key",
      "value": "EX-002052"
    }
  ],
  "timestamp": 1791234746047,
  "partition": 0,
  "offset": 9
}
```

### Regional FIX event
```json
{
  "topic": "market.events",
  "key": "PB_ME",
  "value": "{\"event_type\": \"regional_price_quote\", \"schema_version\": 1, \"execution_id\": \"EX-ME-2039\", \"instrument\": \"PB\", \"side\": \"sell\", \"quantity\": 24, \"price\": 2196.44, \"currency\": \"USD\", \"event_timestamp\": \"20261005-21:11:47.968\", \"conditions\": {\"region\": \"Middle East\", \"warehouse\": {\"warehouse_id\": \"WH_DBX\", \"city\": \"Dubai\"}}}",
  "headers": [
    {
      "key": "event_type",
      "value": "regional_price_quote"
    },
    {
      "key": "schema_version",
      "value": "1"
    },
    {
      "key": "source",
      "value": "synthetic_regional_market"
    },
    {
      "key": "content_type",
      "value": "application/json"
    },
    {
      "key": "idempotency_key",
      "value": "EX-ME-2039"
    }
  ],
  "timestamp": 1791234707968,
  "partition": 0,
  "offset": 9
}

```

## Clickhouse
CREATE DATABASE bronze;
CREATE DATABASE silver;
CREATE DATABASE gold;