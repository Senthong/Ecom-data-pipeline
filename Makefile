# Makefile — convenience commands for the ecom-data-pipeline project
.PHONY: help up down logs ingest dbt-run dbt-test dbt-docs tf-plan tf-apply lint

help:
	@echo "Available commands:"
	@echo "  make up          — Start Airflow + all services"
	@echo "  make down        — Stop and remove containers"
	@echo "  make logs        — Tail Airflow logs"
	@echo "  make ingest      — Run ingestion script manually (today's date)"
	@echo "  make dbt-run     — Run all dbt models"
	@echo "  make dbt-test    — Run all dbt tests"
	@echo "  make dbt-docs    — Generate and serve dbt docs on :8081"
	@echo "  make tf-plan     — Terraform plan (requires AWS creds)"
	@echo "  make tf-apply    — Terraform apply"
	@echo "  make lint        — Run Python linting"

up:
	docker compose up -d
	@echo "Airflow UI: http://localhost:8080 (admin / admin)"

down:
	docker compose down -v

logs:
	docker compose logs -f airflow-scheduler airflow-webserver

ingest:
	docker compose exec airflow-scheduler python /usr/app/ingestion/ingest_olist_to_s3.py

dbt-run:
	docker compose exec airflow-scheduler bash -c \
		"cd /usr/app/dbt_project && dbt run --target prod"

dbt-test:
	docker compose exec airflow-scheduler bash -c \
		"cd /usr/app/dbt_project && dbt test --target prod"

dbt-docs:
	docker compose exec airflow-scheduler bash -c \
		"cd /usr/app/dbt_project && dbt docs generate && dbt docs serve --port 8081"

tf-plan:
	cd infrastructure/terraform && terraform init && terraform plan

tf-apply:
	cd infrastructure/terraform && terraform apply

lint:
	black ingestion/ airflow/
	flake8 ingestion/ airflow/ --max-line-length=100
