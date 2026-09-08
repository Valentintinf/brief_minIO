"""
DAG #3 — Archivage ILM : raw/ et staging/ → archive/ au-delà de 180 jours

Complète les règles ILM natives posées par `minio_ilm_setup.py`. L'API S3 de
cycle de vie ne sait qu'expirer un objet ou le transitionner vers une classe de
stockage : elle ne peut pas le déplacer vers un autre bucket. MinIO offre bien
une transition vers un « remote tier », mais celui-ci doit être un déploiement
distinct — le pointer vers le bucket `archive` du même serveur n'est pas
supporté.

Chemin de rétention complet :
    J+0    dépôt dans raw/ puis staging/
    J+180  déplacement vers archive/          ← ce DAG
    J+730  suppression définitive             ← ILM MinIO (règle d'expiration)

Convention de clé dans archive/ : le bucket d'origine devient le premier
segment, ce qui préserve le partitionnement et rend le retour arrière trivial.
    raw/production_lines/lineA/year=2025/month=05/x.csv
      → archive/raw/production_lines/lineA/year=2025/month=05/x.csv
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import boto3
from botocore.client import Config

from airflow import DAG
from airflow.operators.python import PythonOperator

BUCKET_ARCHIVE = "archive"
SOURCE_BUCKETS = ["raw", "staging"]

# Doit rester aligné sur ARCHIVE_AFTER_DAYS dans minio_ilm_setup.py
ARCHIVE_AFTER_DAYS = 180

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


def _expired_objects(s3, bucket: str, cutoff: datetime) -> list[dict]:
    """Objets du bucket dont la dernière modification précède le seuil."""
    paginator = s3.get_paginator("list_objects_v2")
    expired = []
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            if obj["LastModified"] < cutoff:
                expired.append(obj)
    return expired


def archive_bucket(bucket: str, **_) -> None:
    s3 = _s3_client()
    cutoff = datetime.now(timezone.utc) - timedelta(days=ARCHIVE_AFTER_DAYS)
    expired = _expired_objects(s3, bucket, cutoff)

    print(
        f"[{bucket}] seuil d'archivage : {cutoff:%Y-%m-%d} "
        f"({ARCHIVE_AFTER_DAYS} j) — {len(expired)} objet(s) concerné(s)"
    )

    if not expired:
        print(f"[{bucket}] aucun objet à archiver, le DAG se termine sans effet.")
        return

    archived_bytes = 0
    for obj in expired:
        source_key = obj["Key"]
        archive_key = f"{bucket}/{source_key}"

        # Copie d'abord, suppression ensuite : en cas d'échec de la copie
        # l'objet source reste intact et la tâche est rejouable.
        s3.copy_object(
            Bucket=BUCKET_ARCHIVE,
            Key=archive_key,
            CopySource={"Bucket": bucket, "Key": source_key},
            MetadataDirective="REPLACE",
            Metadata={
                "archived_from": bucket,
                "archived_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "original_last_modified": obj["LastModified"].isoformat(timespec="seconds"),
            },
        )
        s3.delete_object(Bucket=bucket, Key=source_key)

        archived_bytes += obj["Size"]
        age_days = (datetime.now(timezone.utc) - obj["LastModified"]).days
        print(f"  → archive/{archive_key}  ({obj['Size']} o, âge {age_days} j)")

    print(
        f"[{bucket}] {len(expired)} objet(s) archivé(s), "
        f"{archived_bytes} octets libérés dans {bucket}/"
    )


with DAG(
    dag_id="dag_ilm_archive",
    description=(
        f"Archivage ILM : déplacement raw/ et staging/ → archive/ "
        f"au-delà de {ARCHIVE_AFTER_DAYS} jours"
    ),
    default_args=default_args,
    start_date=datetime(2025, 6, 1),
    # Quotidien : le seuil est de 180 jours, un balayage par jour suffit et
    # évite de dépendre d'un déclenchement manuel.
    schedule="@daily",
    catchup=False,
    tags=["c20", "ilm", "archive", "gouvernance"],
) as dag:
    for src in SOURCE_BUCKETS:
        PythonOperator(
            task_id=f"archive_{src}",
            python_callable=archive_bucket,
            op_kwargs={"bucket": src},
        )
