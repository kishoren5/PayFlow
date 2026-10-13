.PHONY: install test lint api worker benchmark evidence demo migrate up down

install:
	python -m pip install -e ".[dev]"

test:
	pytest

lint:
	ruff check src tests

api:
	uvicorn payflow.gateway.app:app --host 0.0.0.0 --port 8000

worker:
	python -m payflow.worker

benchmark:
	python scripts/benchmark_concurrency.py --requests 200 --concurrency 20

evidence:
	python scripts/run_evidence_suite.py

demo:
	python scripts/demo_flagship.py

migrate:
	alembic upgrade head

up:
	docker compose up -d

down:
	docker compose down
