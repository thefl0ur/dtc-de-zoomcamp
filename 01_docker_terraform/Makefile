include .env

load:
	@wget https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet
	@wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv

ingest:
	@uv run ingest.py

q%:
	@docker compose exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) < q$*.sql