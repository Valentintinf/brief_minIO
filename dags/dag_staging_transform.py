"""
DAG #2 — Transformation raw/ → staging/

Pour chaque ligne de production :
  1. Liste les objets CSV dans raw/production_lines/{line}/
  2. Télécharge, harmonise et type les colonnes
  3. Dépose en Parquet (snappy) dans staging/production_lines/{line}/

Schéma cible :
  timestamp        datetime64[ns]
  line_id          object
  temperature      float64
  pressure         float64
  elapsed_time     Float64  (nullable — None sur lignes C, D, E)
  label            int64
  ingestion_ts     datetime64[ns]
  source_file_hash object    (MD5 du CSV source)
"""
from __future__ import annotations

import hashlib
import io
import os
from datetime import datetime, timedelta

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import Config

from airflow import DAG
from airflow.operators.python import PythonOperator

BUCKET_RAW = "raw"
BUCKET_STAGING = "staging"

LINES = ["lineA", "lineB", "lineC", "lineD", "lineE"]

COLUMN_ORDER = [
    "timestamp",
    "line_id",
    "temperature",
    "pressure",
    "elapsed_time",
    "label",
    "ingestion_ts",
    "source_file_hash",
]

default_args = {
    "owner": "data-engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _list_raw_keys(s3, line_name: str) -> list[str]:
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(
        Bucket=BUCKET_RAW,
        Prefix=f"production_lines/{line_name}/",
    ):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv"):
                keys.append(obj["Key"])
    return sorted(keys)


def _harmonize(df: pd.DataFrame, line_name: str, raw_bytes: bytes) -> pd.DataFrame:
    # 1. Normaliser la casse de toutes les colonnes (Temperature→temperature, etc.)
    df.columns = df.columns.str.lower()

    # 2. Parser le timestamp en datetime strict
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="mixed", utc=False)

    # 3. Typer temperature et pressure
    df["temperature"] = df["temperature"].astype("float64")
    df["pressure"] = df["pressure"].astype("float64")

    # 4. elapsed_time nullable — ajouté comme None si absent (lignes C, D, E)
    if "elapsed_time" not in df.columns:
        df["elapsed_time"] = pd.NA
    df["elapsed_time"] = df["elapsed_time"].astype("Float64")

    # 5. Valider label ∈ {0, 1}
    df["label"] = df["label"].astype("int64")
    invalid = df["label"].isin([0, 1])
    if not invalid.all():
        raise ValueError(f"Valeurs de label inattendues : {df.loc[~invalid, 'label'].unique()}")

    # 6. Métadonnées techniques
    df["line_id"] = line_name
    df["ingestion_ts"] = datetime.utcnow()
    df["source_file_hash"] = hashlib.md5(raw_bytes).hexdigest()

    return df[COLUMN_ORDER]


def transform_line(line_name: str, **_) -> None:
    s3 = _s3_client()
    raw_keys = _list_raw_keys(s3, line_name)

    if not raw_keys:
        raise ValueError(
            f"Aucun CSV trouvé dans raw/ pour {line_name}. "
            "Lancez dag_raw_ingestion d'abord."
        )

    print(f"[{line_name}] {len(raw_keys)} fichier(s) à transformer")

    for raw_key in raw_keys:
        # Téléchargement depuis raw/
        resp = s3.get_object(Bucket=BUCKET_RAW, Key=raw_key)
        raw_bytes = resp["Body"].read()

        df = pd.read_csv(io.BytesIO(raw_bytes))
        df_clean = _harmonize(df, line_name, raw_bytes)

        # Partition staging identique à raw (cohérence de lignage)
        ts0 = df_clean["timestamp"].iloc[0]
        year = ts0.year
        month = f"{ts0.month:02d}"
        stem = raw_key.split("/")[-1].replace(".csv", "")
        staging_key = (
            f"production_lines/{line_name}"
            f"/year={year}/month={month}"
            f"/{stem}.parquet"
        )

        # Sérialisation Parquet avec compression snappy
        table = pa.Table.from_pandas(df_clean, preserve_index=False)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")

        s3.put_object(
            Bucket=BUCKET_STAGING,
            Key=staging_key,
            Body=buf.getvalue(),
            ContentType="application/octet-stream",
            Metadata={"line": line_name, "source_key": raw_key},
        )
        print(f"  ✓ staging/{staging_key}  ({len(df_clean)} lignes)")

    print(f"[{line_name}] transformation terminée → {len(raw_keys)} Parquet dans staging/")


with DAG(
    dag_id="dag_staging_transform",
    description="Harmonisation raw/ → staging/ : colonnes lowercase, types, elapsed_time nullable, Parquet snappy",
    default_args=default_args,
    start_date=datetime(2025, 6, 1),
    schedule="@once",
    catchup=False,
    tags=["c19", "staging", "transform"],
) as dag:
    for lname in LINES:
        PythonOperator(
            task_id=f"transform_{lname}",
            python_callable=transform_line,
            op_kwargs={"line_name": lname},
        )
