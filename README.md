# PaySim ETL Pipeline

Проект по обработке финансовых транзакций на датасете **PaySim**.

## Что реализовано

- загрузка данных из Kaggle в PostgreSQL
- full snapshot ETL через Spark + onETL
- incremental load через HWM по колонке `step`
- конфигурация через `config.yaml` и `.env`
- FastAPI API для запуска ETL и просмотра статусов

## Структура проекта

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

## Установка

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Настройка

Создать `.env` по шаблону `.env.example`.

Пример:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=paysim_db
DB_USER=your_postgres_user
DB_PASSWORD=

KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_api_key
```

Проверить `config/config.yaml`.

## Загрузка данных из Kaggle

Полная загрузка:

```bash
python scripts/load_kaggle_data.py --limit 100000
```

Добавление нового батча:

```bash
python scripts/load_kaggle_data.py --append --offset 100000 --limit 50000
```

## Full Snapshot

```bash
python scripts/run_full_snapshot.py
```

Результат записывается в таблицу `paysim_full_snapshot`.

## Incremental Load

```bash
python scripts/run_incremental_load.py
```

- HWM колонка: `step`
- HWM хранится в `data/hwm`
- новые данные загружаются только после последнего значения HWM

## API

Запуск:

```bash
python scripts/run_api.py
```

Документация API:

- `http://127.0.0.1:8000/docs`
- `http://127.0.0.1:8000/redoc`

Эндпоинты:

- `POST /etl/full`
- `POST /etl/incremental`
- `GET /etl/status/{id}`
- `GET /etl/history`

## Проверка в PostgreSQL

```sql
SELECT COUNT(*) FROM paysim_transactions;
SELECT COUNT(*) FROM paysim_full_snapshot;
SELECT COUNT(*) FROM paysim_incremental;
SELECT * FROM etl_job_history ORDER BY started_at DESC;
```
