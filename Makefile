.PHONY: up down status logs-redpanda logs-seaweedfs topics delete-topics consume-fix consume-events generator ingestion clickhouse init-clickhouse

up:
	docker compose up -d

down:
	docker compose down

status:
	docker compose ps

logs-redpanda:
	docker compose logs -f redpanda-0

logs-seaweedfs:
	docker compose logs -f seaweedfs

s3-ls:
	docker compose up -d seaweedfs
	@set -a; . ./.env; set +a; SEAWEED_KEY_ID="$$SEAWEED_ROOT_USER" SEAWEED_ACCESS_KEY="$$SEAWEED_ROOT_PASSWORD" aws --endpoint-url http://localhost:8333 s3 ls

topics:
	docker compose exec redpanda-0 rpk topic create market.fix.raw --partitions 4 -c retention.ms=600000
	docker compose exec redpanda-0 rpk topic create market.events --partitions 4 -c retention.ms=600000
	docker compose exec redpanda-0 rpk topic create market.events.dlq --partitions 1 -c retention.ms=86400000
	docker compose exec redpanda-0 rpk topic create market.fix.raw.dlq --partitions 1 -c retention.ms=86400000

register-schemas:
	curl -s -X POST http://localhost:18081/subjects/market.events/versions \
		-H "Content-Type: application/vnd.schemaregistry.v1+json" \
		-d '{"schemaType":"JSON","schema":"{\"type\":\"object\",\"properties\":{\"event_type\":{\"type\":\"string\"},\"schema_version\":{\"type\":\"integer\"},\"execution_id\":{\"type\":\"string\"},\"instrument\":{\"type\":\"string\"},\"side\":{\"type\":\"string\"},\"quantity\":{\"type\":\"integer\"},\"price\":{\"type\":\"number\"},\"currency\":{\"type\":\"string\"},\"event_timestamp\":{\"type\":\"string\"},\"conditions\":{\"type\":\"object\"}},\"required\":[\"event_type\",\"execution_id\",\"instrument\"]}"}'
	curl -s -X POST http://localhost:18081/subjects/market.fix.raw/versions \
		-H "Content-Type: application/vnd.schemaregistry.v1+json" \
		-d '{"schemaType":"JSON","schema":"{\"type\":\"string\",\"description\":\"FIX 4.4 pipe-delimited message\"}"}'

delete-topics:
	docker compose exec redpanda-0 rpk topic delete market.fix.raw
	docker compose exec redpanda-0 rpk topic delete market.events

consume-fix:
	docker compose exec redpanda-0 rpk topic consume market.fix.raw -n 10

consume-events:
	docker compose exec redpanda-0 rpk topic consume market.events -n 10

generator:
	python -m generator.main

ingestion:
	python -m ingestion.main

compact:
	python -m ingestion.compaction

compact-date:
	python -m ingestion.compaction $(DATE)

clickhouse:
	docker compose exec clickhouse clickhouse-client --password clickhouse

init-clickhouse:
	docker compose exec -T clickhouse clickhouse-client --password clickhouse --multiquery < clickhouse/init.sql