"""Charge le Parquet nettoye (produit par le job Spark, zone processed du
Data Lake) dans la table de staging PostgreSQL du Data Warehouse.

Le fichier est d'abord rapatrie localement via WebHDFS (pas besoin de
libhdfs native), puis lu avec pandas/pyarrow et insere en base via
psycopg2 (insert bulk avec execute_values). On evite volontairement
pandas.to_sql/SQLAlchemy : incompatibilite connue entre pandas>=2.2 et
SQLAlchemy 1.4 (celle imposee par Airflow) qui fait planter to_sql avec
`AttributeError: 'Connection' object has no attribute 'cursor'`.
"""
import argparse
import glob
import os
import tempfile

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from etl.hdfs_utils import download_dir

STAGING_TABLE = "staging_weather"


def build_conn():
    return psycopg2.connect(
        user=os.environ["DWH_POSTGRES_USER"],
        password=os.environ["DWH_POSTGRES_PASSWORD"],
        dbname=os.environ["DWH_POSTGRES_DB"],
        host=os.environ.get("DWH_POSTGRES_HOST", "postgres-dwh"),
        port=os.environ.get("DWH_POSTGRES_INTERNAL_PORT", "5432"),
    )


def load(input_hdfs_dir: str) -> int:
    with tempfile.TemporaryDirectory() as tmp_dir:
        local_dir = download_dir(input_hdfs_dir, tmp_dir)
        parquet_files = glob.glob(os.path.join(local_dir, "**", "*.parquet"), recursive=True)
        if not parquet_files:
            raise FileNotFoundError(f"Aucun fichier .parquet trouve sous {local_dir}")

        df = pd.concat((pd.read_parquet(f) for f in parquet_files), ignore_index=True)
        df = df.astype(object).where(pd.notnull(df), None)
        columns = list(df.columns)
        records = [tuple(row) for row in df.itertuples(index=False, name=None)]

        conn = build_conn()
        try:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    f"INSERT INTO {STAGING_TABLE} ({', '.join(columns)}) VALUES %s",
                    records,
                )
            conn.commit()
        finally:
            conn.close()

        return len(df)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True, help="Repertoire HDFS Parquet (zone processed)")
    args = parser.parse_args()

    n = load(args.input_dir)
    print(f"Loaded {n} rows into {STAGING_TABLE}")


if __name__ == "__main__":
    main()
