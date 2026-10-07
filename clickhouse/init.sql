CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;

-- Bronze layer: raw data as-is from parquet files

CREATE TABLE IF NOT EXISTS bronze.fix_events (
    `8`              String,
    `35`             String,
    `34`             String,
    `52`             String,
    `17`             String,
    `55`             String,
    `54`             String,
    `32`             String,
    `31`             String,
    `60`             String,
    source_file      String,
    ingested_at      DateTime DEFAULT now(),
    pipeline_version String
) ENGINE = MergeTree()
ORDER BY (`17`, `60`);

CREATE TABLE IF NOT EXISTS bronze.regional_events (
    event_type       String,
    schema_version   Int64,
    execution_id     String,
    instrument       String,
    side             String,
    quantity         Int64,
    price            Float64,
    currency         String,
    event_timestamp  String,
    conditions       String,
    source_file      String,
    ingested_at      DateTime DEFAULT now(),
    pipeline_version String
) ENGINE = MergeTree()
ORDER BY (execution_id, event_timestamp);

-- Silver layer: typed, renamed, deduplicated via pipeline

CREATE TABLE IF NOT EXISTS silver.fix_events (
    execution_id     String,
    message_number   Int32,
    event_timestamp  DateTime,
    instrument       String,
    side             Int8,
    quantity         Int32,
    price            Float64,
    trade_timestamp  DateTime,
    source_file      String,
    ingested_at      DateTime DEFAULT now(),
    pipeline_version String
) ENGINE = MergeTree()
ORDER BY (execution_id, trade_timestamp);

CREATE TABLE IF NOT EXISTS silver.regional_events (
    execution_id     String,
    instrument       String,
    side             String,
    quantity         Int32,
    price            Float64,
    currency         String,
    event_timestamp  DateTime,
    region           String,
    conditions       Array(Tuple(
                         warehouse_id         String,
                         warehouse_city       String,
                         delivery_period      Int32,
                         delivery_period_type String
                     )),
    source_file      String,
    ingested_at      DateTime DEFAULT now(),
    pipeline_version String
) ENGINE = MergeTree()
ORDER BY (execution_id, event_timestamp);

-- Gold layer: aggregates for analytics

CREATE TABLE IF NOT EXISTS gold.daily_trade_volume (
    trade_date       Date,
    instrument       String,
    side             Int8,
    total_quantity   Int64,
    trade_count      Int64,
    avg_price        Float64,
    min_price        Float64,
    max_price        Float64,
    source_files     Array(String)
) ENGINE = MergeTree()
ORDER BY (trade_date, instrument);

CREATE TABLE IF NOT EXISTS gold.regional_price_summary (
    event_date       Date,
    instrument       String,
    region           String,
    avg_price        Float64,
    min_price        Float64,
    max_price        Float64,
    trade_count      Int64,
    source_files     Array(String)
) ENGINE = MergeTree()
ORDER BY (event_date, instrument, region);

CREATE TABLE IF NOT EXISTS gold.delivery_summary (
    event_date           Date,
    instrument           String,
    region               String,
    warehouse_id         String,
    warehouse_city       String,
    avg_delivery_period  Float64,
    min_delivery_period  Int32,
    max_delivery_period  Int32,
    delivery_count       Int64,
    source_files         Array(String)
) ENGINE = MergeTree()
ORDER BY (event_date, instrument, region);
