# PaySim ETL Pipeline

A project for processing financial transactions using the **PaySim** dataset.

## Implemented Features

- data loading from Kaggle into PostgreSQL
- full snapshot ETL using Spark + onETL
- incremental load using HWM based on the `step` column
- configuration via `config.yaml` and `.env`
- FastAPI API for running ETL jobs and viewing statuses

## Project Structure

```text
bigdata_project/
├── api/
├── config/
├── data/
│   └── hwm/
├── etl/
├── scripts/
├── .env
├── .env.example
├── README.md
└── requirements.txt
```

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuration

Create a `.env` file based on `.env.example`.

Example:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=paysim_db
DB_USER=your_postgres_user
DB_PASSWORD=

KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_api_key
```

Check the `config/config.yaml` file as well.

## Loading Data from Kaggle

Full load:

```bash
python scripts/load_kaggle_data.py --limit 100000
```

Append a new batch:

```bash
python scripts/load_kaggle_data.py --append --offset 100000 --limit 50000
```

## Full Snapshot

```bash
python scripts/run_full_snapshot.py
```

The result is written to the `paysim_full_snapshot` table.

## Incremental Load

```bash
python scripts/run_incremental_load.py
```

- HWM column: `step`
- HWM is stored in `data/hwm`
- only new data after the last HWM value is loaded

## API

Run:

```bash
python scripts/run_api.py
```

Available endpoints:

- `http://127.0.0.1:8000/docs`
- `http://127.0.0.1:8000/redoc`

## Verification in PostgreSQL

```sql
SELECT COUNT(*) FROM paysim_transactions;
SELECT COUNT(*) FROM paysim_full_snapshot;
SELECT COUNT(*) FROM paysim_incremental;
SELECT * FROM etl_job_history ORDER BY started_at DESC;
```
