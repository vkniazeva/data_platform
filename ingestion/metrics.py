from prometheus_client import Counter, Histogram, start_http_server

METRICS_PORT = 8000

messages_total = Counter(
    'pipeline_messages_total',
    'Total messages by topic and processing stage',
    ['topic', 'stage']  # stage: read | validated | dlq | quarantine | written
)

dlq_messages_total = Counter(
    'pipeline_dlq_messages_total',
    'Messages routed to DLQ by reason prefix',
    ['topic', 'reason']
)

s3_write_duration = Histogram(
    'pipeline_s3_write_duration_seconds',
    'Duration of S3 Parquet flush',
    ['topic'],
    buckets=[.001, .005, .01, .025, .05, .1, .25, .5, 1.0]
)

s3_batch_size = Histogram(
    'pipeline_s3_batch_size',
    'Number of messages per Parquet flush',
    ['topic', 'reason'],  # reason: buffer_full | date_change | shutdown
    buckets=[1, 5, 10, 25, 50, 75, 100]
)

ch_insert_duration = Histogram(
    'pipeline_ch_insert_duration_seconds',
    'Duration of ClickHouse insert',
    ['topic'],
    buckets=[.005, .01, .025, .05, .1, .25, .5, 1.0, 2.0]
)

worker_restarts_total = Counter(
    'pipeline_worker_restarts_total',
    'Number of worker crashes and restarts',
    ['topic']
)

message_lag_seconds = Histogram(
    'pipeline_message_lag_seconds',
    'End-to-end latency: Kafka produce timestamp → pipeline processing',
    ['topic'],
    buckets=[.1, .5, 1, 2, 5, 10, 30, 60, 120]
)


def start_metrics_server() -> None:
    """Start the Prometheus HTTP metrics endpoint."""
    start_http_server(METRICS_PORT)
