# DSS150P Laboratory Activity #2

## Setup
1. `python -m venv .venv`
2. Activate `.venv`
3. `pip install -r requirements.txt`
4. `docker compose up -d`
5. Load PostgreSQL seed: `docker exec -i dss150p-lab2-postgres psql -U dss150p -d dss150p < sql/seed_support_tickets.sql`
   (note: the container name in `docker-compose.yml` is `dss150p-lab2-postgres`)

## Running
- Terminal A: `python src/local_api_server.py`
- Terminal B:
  - `python src/profile_sources.py` — profiles customers.csv, orders.json, products.parquet
  - `python src/inspect_api.py` — manual paginated API retrieval (Task 1.5 evidence)
  - `python src/ingest_pipeline.py` — file + API ingestion (rerunnable/idempotent)
  - `python src/validate_raw.py` — automated validation of raw outputs and watermark

## Layout
- `config/metadata/` — five source metadata records (Task 2.1)
- `config/schema_*.yml` — logical schemas for customers and API events (Task 2.2)
- `config/data_contract_customers.yml` — completed data contract (Task 2.3)
- `docs/ingestion_design.md` — ingestion design + watermark semantics (Tasks 2.4–2.5)
- `docs/engineering_reflection.md` — reflection answers
- `outputs/profiling_report.md` — profiling report
- `outputs/pipeline_run_log.csv` — operational run log (one row per source per run)
- `outputs/evidence/` — terminal transcripts: PostgreSQL inspection, API pagination,
  first run, idempotent rerun, failure experiment, recovery, clean-room reproducibility
- `raw/`, `state/` — generated at run time; not committed (see `.gitignore`)

Do not commit `.env`, generated raw data, or watermark state.
