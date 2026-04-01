import os
import argparse
from pathlib import Path
from uuid import uuid4

import pandas as pd
import yaml
import kagglehub

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, Numeric, String, DateTime


def parse_args():
    parser = argparse.ArgumentParser(description="Load PaySim data from Kaggle to PostgreSQL")
    parser.add_argument("--limit", type=int, default=100000, help="How many rows to load")
    parser.add_argument("--offset", type=int, default=0, help="From which row to start reading")
    parser.add_argument("--append", action="store_true", help="Append rows instead of replacing table data")
    return parser.parse_args()


def load_env():
    project_root = Path(__file__).resolve().parent.parent
    env_path = project_root / ".env"

    if not env_path.exists():
        raise FileNotFoundError(f".env file not found: {env_path}")

    load_dotenv(env_path)

    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD", "")

    kaggle_username = os.getenv("KAGGLE_USERNAME")
    kaggle_key = os.getenv("KAGGLE_KEY")

    print(f"Using .env file: {env_path}")
    print(f"DB_HOST={db_host}, DB_PORT={db_port}, DB_NAME={db_name}, DB_USER={db_user}")

    if not all([db_host, db_port, db_name, db_user]):
        raise ValueError("DB settings are missing in .env")

    if not all([kaggle_username, kaggle_key]):
        raise ValueError("Kaggle settings are missing in .env")

    os.environ["KAGGLE_USERNAME"] = kaggle_username
    os.environ["KAGGLE_KEY"] = kaggle_key

    return db_host, db_port, db_name, db_user, db_password


def load_yaml_config():
    project_root = Path(__file__).resolve().parent.parent
    config_path = project_root / "config" / "config.yaml"

    if not config_path.exists():
        raise FileNotFoundError(f"config.yaml not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    return config


def get_engine(db_host, db_port, db_name, db_user, db_password):
    if db_password:
        db_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    else:
        db_url = f"postgresql+psycopg2://{db_user}@{db_host}:{db_port}/{db_name}"

    return create_engine(db_url)


def download_dataset(dataset_name: str):
    print("Downloading dataset from Kaggle...")
    dataset_path = kagglehub.dataset_download(dataset_name)
    print(f"Dataset downloaded to: {dataset_path}")
    return Path(dataset_path)


def find_csv_file(dataset_path: Path):
    csv_files = list(dataset_path.rglob("*.csv"))

    if not csv_files:
        raise FileNotFoundError("CSV file was not found in downloaded dataset")

    csv_file = csv_files[0]
    print(f"CSV file found: {csv_file}")
    return csv_file


def read_data(csv_file: Path, limit: int, offset: int):
    print(f"Reading data: offset={offset}, limit={limit}")

    if offset > 0:
        df = pd.read_csv(csv_file, skiprows=range(1, offset + 1), nrows=limit)
    else:
        df = pd.read_csv(csv_file, nrows=limit)

    if df.empty:
        raise ValueError("No rows were read from CSV. Check offset/limit.")

    print(f"Loaded {len(df)} rows from CSV")
    return df


def transform_data(df: pd.DataFrame):
    print("Transforming data types...")

    df.columns = [col.lower() for col in df.columns]

    int_columns = ["step", "isfraud", "isflaggedfraud"]
    numeric_columns = [
        "amount",
        "oldbalanceorg",
        "newbalanceorig",
        "oldbalancedest",
        "newbalancedest",
    ]
    string_columns = ["type", "nameorig", "namedest"]

    for col in int_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)

    df["batch_id"] = str(uuid4())
    df["loaded_at"] = pd.Timestamp.now()

    needed_columns = [
        "step",
        "type",
        "amount",
        "nameorig",
        "oldbalanceorg",
        "newbalanceorig",
        "namedest",
        "oldbalancedest",
        "newbalancedest",
        "isfraud",
        "isflaggedfraud",
        "batch_id",
        "loaded_at",
    ]

    missing_columns = [col for col in needed_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing columns in dataset: {missing_columns}")

    print(f"Batch id: {df['batch_id'].iloc[0]}")
    return df[needed_columns]


def create_table_if_not_exists(engine, table_name: str):
    print(f"Creating table {table_name} if not exists...")

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id BIGSERIAL PRIMARY KEY,
        step INTEGER NOT NULL,
        type VARCHAR(32) NOT NULL,
        amount NUMERIC(18,2) NOT NULL,
        nameorig VARCHAR(64) NOT NULL,
        oldbalanceorg NUMERIC(18,2) NOT NULL,
        newbalanceorig NUMERIC(18,2) NOT NULL,
        namedest VARCHAR(64) NOT NULL,
        oldbalancedest NUMERIC(18,2) NOT NULL,
        newbalancedest NUMERIC(18,2) NOT NULL,
        isfraud INTEGER NOT NULL,
        isflaggedfraud INTEGER NOT NULL,
        batch_id VARCHAR(64),
        loaded_at TIMESTAMP
    );
    """

    with engine.begin() as conn:
        conn.execute(text(create_sql))

    print("Table is ready")


def truncate_table(engine, table_name: str):
    print(f"Clearing table {table_name} because --append was not used...")

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY;"))

    print("Table cleared")


def load_to_postgres(df: pd.DataFrame, engine, table_name: str):
    print(f"Loading data into PostgreSQL table {table_name}...")

    dtype_map = {
        "step": Integer(),
        "type": String(32),
        "amount": Numeric(18, 2),
        "nameorig": String(64),
        "oldbalanceorg": Numeric(18, 2),
        "newbalanceorig": Numeric(18, 2),
        "namedest": String(64),
        "oldbalancedest": Numeric(18, 2),
        "newbalancedest": Numeric(18, 2),
        "isfraud": Integer(),
        "isflaggedfraud": Integer(),
        "batch_id": String(64),
        "loaded_at": DateTime(),
    }

    df.to_sql(
        table_name,
        con=engine,
        if_exists="append",
        index=False,
        dtype=dtype_map,
        method="multi",
        chunksize=5000,
    )

    print(f"Inserted {len(df)} rows into {table_name}")


def print_stats(engine, table_name: str):
    with engine.connect() as conn:
        total_rows = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
        print(f"Total rows in table {table_name}: {total_rows}")

        result = conn.execute(
            text(f"""
                SELECT batch_id, COUNT(*)
                FROM {table_name}
                GROUP BY batch_id
                ORDER BY COUNT(*) DESC
            """)
        )

        print("Rows by batch:")
        for row in result:
            print(row)


def main():
    args = parse_args()

    db_host, db_port, db_name, db_user, db_password = load_env()
    config = load_yaml_config()

    dataset_name = config["kaggle"]["dataset"]
    table_name = config["source"]["table"]

    engine = get_engine(db_host, db_port, db_name, db_user, db_password)

    dataset_path = download_dataset(dataset_name)
    csv_file = find_csv_file(dataset_path)

    df = read_data(csv_file, args.limit, args.offset)
    df = transform_data(df)

    create_table_if_not_exists(engine, table_name)

    if not args.append:
        truncate_table(engine, table_name)

    load_to_postgres(df, engine, table_name)
    print_stats(engine, table_name)

    print("Done")


if __name__ == "__main__":
    main()