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