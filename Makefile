.PHONY: up down status logs topics consume-fix consume-events generator

up:
	docker compose up -d redpanda-0

down:
	docker compose down

status:
	docker compose ps

logs:
	docker compose logs -f redpanda-0

topics:
	docker compose exec redpanda-0 rpk topic create market.fix.raw
	docker compose exec redpanda-0 rpk topic create market.events

consume-fix:
	docker compose exec redpanda-0 rpk topic consume market.fix.raw -n 10

consume-events:
	docker compose exec redpanda-0 rpk topic consume market.events -n 10

generator:
	python -m generator.main