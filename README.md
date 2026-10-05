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

## Roadmap

- [ ] Data generator
- [ ] Kafka / Redpanda locally
- [ ] Ingestion service, raw Parquet in MinIO
- [ ] ClickHouse loading
- [ ] Spark and Iceberg experiments

## Quick start

TBD

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