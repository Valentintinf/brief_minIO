"""
DAG #1 — Ingestion brute : CSV Zenodo → MinIO raw/

Partitionnement : production_lines/{line}/year={YYYY}/month={MM}/
LineA (10 000 lignes) : découpée en chunks de 1 000 pour simuler un flux réel.
Autres lignes (5 000 lignes) : upload en fichier unique.
"""
from __future__ import annotations

import hashlib
import io
import os
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import pandas as pd
from botocore.client import Config

from airflow import DAG
from airflow.operators.python import PythonOperator

DATA_DIR = Path("/opt/airflow/data")
BUCKET_RAW = "raw"
CHUNK_SIZE = 1_000

DATASETS: dict[str, str] = {
    "LineA_Stable_10K.csv":  "lineA",
    "LineB_Flux.csv":        "lineB",
    "LineC_Turbulent.csv":   "lineC",
    "LineD_SpikeControl.csv": "lineD",
    "LineE_SmoothRun.csv":   "lineE",
}

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


def _object_key(line: str, ts: pd.Timestamp, filename: str) -> str:
    return (
        f"production_lines/{line}"
        f"/year={ts.year}/month={ts.month:02d}"
        f"/{filename}"
    )


def _upload_df(s3, line: str, df: pd.DataFrame, obj_name: str) -> None:
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    data = buf.getvalue()
    md5 = hashlib.md5(data).hexdigest()

    first_ts = pd.to_datetime(df["timestamp"].iloc[0])
    key = _object_key(line, first_ts, obj_name)

    s3.put_object(
        Bucket=BUCKET_RAW,
        Key=key,
        Body=data,
        ContentType="text/csv",
        Metadata={"line": line, "source": "zenodo", "local_md5": md5},
    )
    print(f"  ↑ raw/{key}  ({len(df)} lignes, md5={md5[:8]}…)")


def ingest_line(file_name: str, line_name: str, **_) -> None:
    s3 = _s3_client()
    file_path = DATA_DIR / file_name

    if not file_path.exists():
        raise FileNotFoundError(f"Fichier source introuvable : {file_path}")

    stem = Path(file_name).stem

    if line_name == "lineA":
        total = 0
        for i, chunk in enumerate(pd.read_csv(file_path, chunksize=CHUNK_SIZE), start=1):
            _upload_df(s3, line_name, chunk, f"{stem}_chunk_{i:04d}.csv")
            total += len(chunk)
        print(f"[{line_name}] {i} chunks uploadés ({total} lignes) → raw/")
    else:
        df = pd.read_csv(file_path)
        _upload_df(s3, line_name, df, file_name)
        print(f"[{line_name}] fichier uploadé ({len(df)} lignes) → raw/")


with DAG(
    dag_id="dag_raw_ingestion",
    description="Ingestion des 5 CSV capteurs vers raw/ avec partitionnement year=/month=/line=/",
    default_args=default_args,
    start_date=datetime(2025, 6, 1),
    schedule="@once",
    catchup=False,
    tags=["c19", "raw", "ingestion"],
) as dag:
    for fname, lname in DATASETS.items():
        PythonOperator(
            task_id=f"ingest_{lname}",
            python_callable=ingest_line,
            op_kwargs={"file_name": fname, "line_name": lname},
        )
