.PHONY: up down reset logs ps run psql dbt-build test lint

YEAR  ?= 2024
MONTH ?= 1

up:            ## Build images and start the whole stack
	docker compose up -d --build

down:          ## Stop the stack (keeps data)
	docker compose down

reset:         ## Stop the stack and wipe all volumes (warehouse included)
	docker compose down -v

logs:          ## Follow logs of every service
	docker compose logs -f

ps:
	docker compose ps

run:           ## Trigger the pipeline for one month: make run YEAR=2024 MONTH=2
	docker compose exec airflow-scheduler \
		airflow dags trigger nyc_taxi_pipeline -c '{"year": $(YEAR), "month": $(MONTH)}'

psql:          ## Open a psql shell on the warehouse
	docker compose exec postgres psql -U warehouse -d warehouse

dbt-build:     ## Run dbt build inside the Airflow container
	docker compose exec airflow-scheduler bash -c 'cd /opt/airflow/dbt && dbt build --profiles-dir .'

test:
	pytest -q

lint:
	ruff check . && ruff format --check .
