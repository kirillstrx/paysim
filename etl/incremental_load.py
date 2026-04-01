import os
import time
import logging
from pathlib import Path

import yaml
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from onetl.connection import Postgres
from onetl.db import DBReader, DBWriter
from onetl.strategy import IncrementalStrategy
from onetl.hwm.store import YAMLHWMStore


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


def load_config():
    project_root = Path(__file__).resolve().parent.parent
    env_path = project_root / ".env"
    config_path = project_root / "config" / "config.yaml"

    load_dotenv(env_path)

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    return config


def create_spark(app_name: str, master: str):
    maven_packages = Postgres.get_packages()

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.jars.packages", ",".join(maven_packages))
        .getOrCreate()
    )
    return spark


def create_postgres_connection(spark):
    return Postgres(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD", ""),
        spark=spark,
    )


def transform(df):
    valid_types = ["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]

    df = df.filter(
        (F.col("amount").isNotNull()) &
        (F.col("amount") > 0) &
        (F.col("step").isNotNull()) &
        (F.col("step") >= 0) &
        (F.col("type").isNotNull()) &
        (F.col("type").isin(valid_types)) &
        (F.col("nameorig").isNotNull()) &
        (F.col("namedest").isNotNull()) &
        (F.trim(F.col("nameorig")) != "") &
        (F.trim(F.col("namedest")) != "") &
        (F.col("oldbalanceorg").isNotNull()) &
        (F.col("newbalanceorig").isNotNull()) &
        (F.col("oldbalancedest").isNotNull()) &
        (F.col("newbalancedest").isNotNull())
    )

    df = (
        df.withColumn("balance_change_orig", F.col("newbalanceorig") - F.col("oldbalanceorg"))
          .withColumn("balance_change_dest", F.col("newbalancedest") - F.col("oldbalancedest"))
          .withColumn("transaction_hour", F.col("step") % F.lit(24))
          .withColumn("transaction_day", F.floor(F.col("step") / F.lit(24)))
    )

    df = df.withColumn(
        "amount_category",
        F.when(F.col("amount") < 1000, F.lit("Small"))
         .when((F.col("amount") >= 1000) & (F.col("amount") < 10000), F.lit("Medium"))
         .when((F.col("amount") >= 10000) & (F.col("amount") < 200000), F.lit("Large"))
         .otherwise(F.lit("Massive"))
    )

    return df


def run_incremental_load():
    started_at = time.time()
    config = load_config()

    spark = create_spark(
        app_name=config["incremental"]["app_name"],
        master=config["spark"]["master"],
    )

    try:
        conn = create_postgres_connection(spark)

        source = f'{config["source"]["schema"]}.{config["source"]["table"]}'
        target = f'{config["incremental"]["target"]["schema"]}.{config["incremental"]["target"]["table"]}'

        logging.info("============================================================")
        logging.info("=== INCREMENTAL LOAD STARTED ===")
        logging.info("Source: %s", source)
        logging.info("Target: %s", target)
        logging.info("HWM name: %s", config["incremental"]["hwm"]["name"])
        logging.info("HWM column: %s", config["incremental"]["hwm"]["column"])
        logging.info("============================================================")

        reader = DBReader(
            connection=conn,
            source=source,
            columns=["*"],
            hwm=DBReader.AutoDetectHWM(
                name=config["incremental"]["hwm"]["name"],
                expression=config["incremental"]["hwm"]["column"],
            ),
        )

        hwm_dir = Path(__file__).resolve().parent.parent / "data" / "hwm"
        hwm_dir.mkdir(parents=True, exist_ok=True)

        with YAMLHWMStore(path=hwm_dir):
            with IncrementalStrategy():
                df = reader.run()
                source_count = df.count()
                logging.info("Rows read from source: %s", source_count)

                if source_count == 0:
                    duration_sec = round(time.time() - started_at, 2)

                    logging.info("============================================================")
                    logging.info("=== INCREMENTAL LOAD FINISHED ===")
                    logging.info("No new rows found")
                    logging.info("Rows read: 0")
                    logging.info("Rows written: 0")
                    logging.info("Duration: %s sec", duration_sec)
                    logging.info("HWM directory: %s", hwm_dir)
                    logging.info("============================================================")

                    return {
                        "rows_read": 0,
                        "rows_written": 0,
                        "duration_sec": duration_sec,
                    }

                result_df = transform(df)
                result_count = result_df.count()
                logging.info("Rows after transformations: %s", result_count)

                writer = DBWriter(
                    connection=conn,
                    target=target,
                    options=Postgres.WriteOptions(
                        if_exists=config["incremental"]["write_mode"]
                    ),
                )
                writer.run(result_df)

        duration_sec = round(time.time() - started_at, 2)

        logging.info("============================================================")
        logging.info("=== INCREMENTAL LOAD FINISHED SUCCESSFULLY ===")
        logging.info("Rows read: %s", source_count)
        logging.info("Rows written: %s", result_count)
        logging.info("Duration: %s sec", duration_sec)
        logging.info("HWM directory: %s", hwm_dir)
        logging.info("============================================================")

        return {
            "rows_read": source_count,
            "rows_written": result_count,
            "duration_sec": duration_sec,
        }

    finally:
        spark.stop()


if __name__ == "__main__":
    print(run_incremental_load())