.PHONY: help build reset-db pipeline serve dbt-docs transform

help:
	@echo "Weather Pipeline - Commands:"
	@echo "  make build       - Build the Docker image (Required first step)"
	@echo "  make publish     - Build & Push Universal Image (works on Win/Mac)"
	@echo "  make reset-db    - Reset database to clean state"
	@echo "  make pipeline    - Run the full ingestion and transformation pipeline once"
	@echo "  make serve       - Run the pipeline on an hourly schedule (Long-running)"
	@echo "  make transform   - Run only dbt transformations"
	@echo "  make dbt-docs    - Generate and serve project documentation"

build:
	docker-compose build

publish:
	@echo "Building and Pushing Universal Image (amd64 + arm64)..."
	docker buildx build --platform linux/amd64,linux/arm64 -t trungluu1/weather-data-pipeline:v1 --push .

reset-db:
	@echo "Resetting Database..."
	docker-compose down -v
	rm -rf data/*.db data/*.db.wal data/*.tmp
	docker system prune -f
	@echo "Database cleared. Run 'make pipeline' to re-initialize and ingest data."

pipeline:
	@echo "Triggering one-off pipeline run..."
	docker-compose run --rm weather-app python -m orchestration.flow

serve:
	@echo "Starting hourly automated pipeline..."
	docker-compose run --rm weather-app python -m orchestration.deploy

transform:
	@echo "Running dbt transformations..."
	docker-compose run --rm weather-app bash -c "cd transform && python -m dbt.cli.main deps --profiles-dir . && python -m dbt.cli.main build --profiles-dir ."


dbt-docs:
	docker-compose run --rm weather-app python -m dbt.cli.main docs generate --profiles-dir transform --project-dir transform
	@mkdir -p docs_output
	@docker-compose run --rm -v $(PWD)/docs_output:/output weather-app cp transform/target/index.html /output/index.html
	@docker-compose run --rm -v $(PWD)/docs_output:/output weather-app cp transform/target/catalog.json /output/catalog.json
	@docker-compose run --rm -v $(PWD)/docs_output:/output weather-app cp transform/target/manifest.json /output/manifest.json
	@echo "Documentation generated in 'docs_output/'."
	@echo "Serving at http://localhost:8080..."
	@python3 -m http.server 8080 --directory docs_output
