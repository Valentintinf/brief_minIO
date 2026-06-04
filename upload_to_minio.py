#!/usr/bin/env python3
"""
Jour 2 — Création des buckets MinIO, policies initiales, upload des 5 CSV
et vérification d'intégrité par hash MD5.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, EndpointConnectionError


# Actions S3 accordées par bucket — principe du moindre privilège
# Les comptes de service (data-analyst, data-engineer) seront créés au Jour 6.
BUCKET_POLICIES: dict[str, list[str]] = {
    "raw":     ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"],
    "staging": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"],
    "curated": ["s3:GetObject", "s3:PutObject",                    "s3:ListBucket", "s3:GetBucketLocation"],
    "archive": ["s3:GetObject",                                     "s3:ListBucket", "s3:GetBucketLocation"],
}

BUCKETS = tuple(BUCKET_POLICIES.keys())
DATASETS = {
    "LineA_Stable_10K.csv": "lineA",
    "LineB_Flux.csv": "lineB",
    "LineC_Turbulent.csv": "lineC",
    "LineD_SpikeControl.csv": "lineD",
    "LineE_SmoothRun.csv": "lineE",
}


@dataclass(frozen=True)
class UploadResult:
    file_name: str
    bucket: str
    object_key: str
    size_bytes: int
    local_md5: str
    remote_md5: str

    @property
    def is_valid(self) -> bool:
        return self.local_md5 == self.remote_md5


def build_s3_client(endpoint_url: str, access_key: str, secret_key: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def compute_md5(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def ensure_bucket(s3_client, bucket: str) -> None:
    try:
        s3_client.head_bucket(Bucket=bucket)
        print(f"✓ Bucket déjà présent : {bucket}")
    except ClientError as error:
        status_code = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status_code not in (403, 404):
            raise
        s3_client.create_bucket(Bucket=bucket)
        print(f"✓ Bucket créé : {bucket}")


_BUCKET_LEVEL_ACTIONS = {"s3:ListBucket", "s3:GetBucketLocation"}


def apply_initial_bucket_policy(s3_client, bucket: str) -> None:
    actions = BUCKET_POLICIES[bucket]
    object_actions = [a for a in actions if a not in _BUCKET_LEVEL_ACTIONS]
    bucket_actions = [a for a in actions if a in _BUCKET_LEVEL_ACTIONS]

    statements = []
    if object_actions:
        statements.append({
            "Sid": f"{bucket.capitalize()}ObjectAccess",
            "Effect": "Allow",
            "Principal": {"AWS": ["arn:aws:iam::*:root"]},
            "Action": object_actions,
            "Resource": [f"arn:aws:s3:::{bucket}/*"],
        })
    if bucket_actions:
        statements.append({
            "Sid": f"{bucket.capitalize()}BucketAccess",
            "Effect": "Allow",
            "Principal": {"AWS": ["arn:aws:iam::*:root"]},
            "Action": bucket_actions,
            "Resource": [f"arn:aws:s3:::{bucket}"],
        })

    policy = {"Version": "2012-10-17", "Statement": statements}
    s3_client.put_bucket_policy(Bucket=bucket, Policy=json.dumps(policy))
    print(f"✓ Policy appliquée ({', '.join(actions)}) : {bucket}")


def upload_file_to_raw(s3_client, path: Path, line_name: str) -> UploadResult:
    object_key = f"production_lines/{line_name}/{path.name}"
    local_md5 = compute_md5(path)
    content_type = mimetypes.guess_type(path.name)[0] or "text/csv"

    s3_client.upload_file(
        str(path),
        "raw",
        object_key,
        ExtraArgs={
            "ContentType": content_type,
            "Metadata": {
                "line": line_name,
                "source": "zenodo",
                "local_md5": local_md5,
            },
        },
    )

    response = s3_client.head_object(Bucket="raw", Key=object_key)
    remote_md5 = response["ETag"].strip('"')

    return UploadResult(
        file_name=path.name,
        bucket="raw",
        object_key=object_key,
        size_bytes=path.stat().st_size,
        local_md5=local_md5,
        remote_md5=remote_md5,
    )


def find_dataset_files(data_dir: Path) -> list[tuple[Path, str]]:
    missing_files = [file_name for file_name in DATASETS if not (data_dir / file_name).exists()]
    if missing_files:
        missing = ", ".join(missing_files)
        raise FileNotFoundError(f"Fichiers CSV manquants dans {data_dir}: {missing}")
    return [(data_dir / file_name, line_name) for file_name, line_name in DATASETS.items()]


def write_manifest(results: list[UploadResult], output_path: Path) -> None:
    lines = [
        "# Manifest d'upload MinIO — Jour 2",
        "",
        "| Fichier | Bucket | Objet | Taille | MD5 local | MD5 MinIO | Statut |",
        "|---|---|---|---:|---|---|---|",
    ]
    for result in results:
        status = "OK" if result.is_valid else "KO"
        lines.append(
            "| "
            f"{result.file_name} | {result.bucket} | `{result.object_key}` | "
            f"{result.size_bytes} | `{result.local_md5}` | `{result.remote_md5}` | {status} |"
        )
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"✓ Manifest écrit : {output_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload des CSV Jour 2 vers MinIO.")
    parser.add_argument("--endpoint-url", default=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"))
    parser.add_argument("--access-key", default=os.getenv("MINIO_ROOT_USER", "minioadmin"))
    parser.add_argument("--secret-key", default=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--manifest", default="JOUR2_UPLOAD_MANIFEST.md")
    parser.add_argument("--skip-policies", action="store_true", help="Ne pas appliquer les bucket policies.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    data_dir = Path(args.data_dir)
    manifest_path = Path(args.manifest)

    try:
        dataset_files = find_dataset_files(data_dir)
        s3_client = build_s3_client(args.endpoint_url, args.access_key, args.secret_key)

        for bucket in BUCKETS:
            ensure_bucket(s3_client, bucket)
            if not args.skip_policies:
                apply_initial_bucket_policy(s3_client, bucket)

        results = []
        for file_path, line_name in dataset_files:
            result = upload_file_to_raw(s3_client, file_path, line_name)
            results.append(result)
            status = "OK" if result.is_valid else "KO"
            print(f"✓ Upload {status} : {result.file_name} → raw/{result.object_key}")

        write_manifest(results, manifest_path)

        invalid_results = [result for result in results if not result.is_valid]
        if invalid_results:
            print("✗ Au moins un hash MD5 ne correspond pas.", file=sys.stderr)
            return 1

        print("✓ Jour 2 terminé : buckets, policies, uploads et MD5 validés.")
        return 0
    except EndpointConnectionError:
        print("✗ MinIO est inaccessible. Lancez d'abord : docker-compose up -d", file=sys.stderr)
        return 1
    except Exception as error:
        print(f"✗ Erreur : {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
