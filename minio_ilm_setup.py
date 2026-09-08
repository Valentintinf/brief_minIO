#!/usr/bin/env python3
"""
Jour 5 — Configuration des règles de cycle de vie (ILM) sur les buckets MinIO.

Politique de rétention demandée par le brief :
  - archivage automatique après 180 jours ;
  - suppression après 2 ans (730 jours).

Deux mécanismes complémentaires sont nécessaires, car l'API S3 de cycle de vie
ne sait pas déplacer un objet d'un bucket vers un autre :

  Couche 1 — ILM natif MinIO (ce script)
      Expiration des objets, évaluée en continu par le scanner MinIO,
      visible dans la console (Bucket → Lifecycle) et via `mc ilm rule ls`.

  Couche 2 — DAG Airflow `dag_ilm_archive` (dags/dag_ilm_archive.py)
      Déplacement raw/ et staging/ → archive/ au-delà de 180 jours.
      MinIO ne peut le faire nativement que via un « remote tier », qui exige
      un second déploiement MinIO distinct : le tiering vers le même serveur
      n'est pas supporté.

Horizon de rétention retenu, compté depuis la création de l'objet :
    J+0    →  dépôt dans raw/ puis staging/
    J+180  →  déplacement vers archive/            (couche 2)
    J+730  →  suppression définitive               (couche 1)
Soit une expiration réglée à 550 jours sur `archive`, puisque les objets y
arrivent déjà âgés de 180 jours (180 + 550 = 730).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, EndpointConnectionError

# Jours de rétention — un seul endroit à modifier pour toute la politique
ARCHIVE_AFTER_DAYS = 180   # raw/ + staging/ → archive/   (appliqué par le DAG)
DELETE_AFTER_DAYS = 730    # 2 ans après création         (appliqué par l'ILM)

# Séjour restant dans archive/ pour totaliser 730 jours depuis la création
ARCHIVE_RETENTION_DAYS = DELETE_AFTER_DAYS - ARCHIVE_AFTER_DAYS


def lifecycle_rules(bucket: str) -> list[dict]:
    """Règles ILM appliquées à un bucket donné.

    Seule l'action Expiration est posée. MinIO rejette (InvalidArgument) une
    règle ne portant qu'un AbortIncompleteMultipartUpload, et ignore
    silencieusement cette action lorsqu'elle accompagne une Expiration : la
    purge des uploads interrompus relève chez MinIO d'un réglage serveur
    (`mc admin config get local api` → stale_uploads_expiry, 24 h par défaut)
    et non du cycle de vie des buckets.
    """
    if bucket == "archive":
        rule_id = "expire-archive-end-of-retention"
        expire_days = ARCHIVE_RETENTION_DAYS
    else:
        # Filet de sécurité : un objet jamais archivé par le DAG (panne
        # d'Airflow, DAG désactivé) reste malgré tout purgé à 2 ans.
        rule_id = f"expire-{bucket}-2-years"
        expire_days = DELETE_AFTER_DAYS

    return [{
        "ID": rule_id,
        "Status": "Enabled",
        "Filter": {"Prefix": ""},
        "Expiration": {"Days": expire_days},
    }]


BUCKETS = ("raw", "staging", "curated", "archive")


def build_s3_client(endpoint_url: str, access_key: str, secret_key: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def apply_lifecycle(s3_client, bucket: str) -> list[dict]:
    rules = lifecycle_rules(bucket)
    s3_client.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={"Rules": rules},
    )
    # Relecture depuis MinIO : on documente ce que le serveur a réellement
    # enregistré, pas ce qu'on croit avoir envoyé.
    applied = s3_client.get_bucket_lifecycle_configuration(Bucket=bucket)["Rules"]
    print(f"✓ ILM appliquée sur {bucket:8s} : {len(applied)} règle(s)")
    for rule in applied:
        print(f"    · {describe_rule(rule)}")
    return applied


def rule_effect(rule: dict) -> str:
    """Effet de la règle, sans son identifiant."""
    if "Expiration" in rule:
        return f"suppression à {rule['Expiration']['Days']} jours"
    return "aucune action reconnue"


def describe_rule(rule: dict) -> str:
    return f"{rule['ID']} → {rule_effect(rule)}"


def write_report(applied: dict[str, list[dict]], output_path: Path) -> None:
    lines = [
        "# Politique ILM MinIO — Jour 5 (C20)",
        "",
        "Généré par `minio_ilm_setup.py`, à partir de la configuration relue sur MinIO.",
        "",
        "## Horizon de rétention",
        "",
        "| Échéance | Événement | Mécanisme |",
        "|---|---|---|",
        "| J+0 | dépôt dans `raw/` puis `staging/` | DAGs d'ingestion et de transformation |",
        f"| J+{ARCHIVE_AFTER_DAYS} | déplacement vers `archive/` | DAG `dag_ilm_archive` |",
        f"| J+{DELETE_AFTER_DAYS} | suppression définitive | règle ILM MinIO |",
        "",
        "## Règles enregistrées sur MinIO",
        "",
        "| Bucket | ID de règle | Statut | Effet |",
        "|---|---|---|---|",
    ]
    for bucket, rules in applied.items():
        for rule in rules:
            lines.append(
                f"| `{bucket}` | `{rule['ID']}` | {rule['Status']} | {rule_effect(rule)} |"
            )
    lines += [
        "",
        "## Vérification",
        "",
        "```bash",
        "# Via le client MinIO, pour chaque bucket",
        "docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin",
        "docker exec minio mc ilm rule ls local/raw",
        "",
        "# Via boto3",
        "python minio_ilm_setup.py --check",
        "```",
        "",
        "Dans la console MinIO : **Buckets → <bucket> → Lifecycle**.",
        "",
        "## Pourquoi deux mécanismes",
        "",
        "L'API S3 de cycle de vie n'expose que deux actions : expirer un objet, ou le",
        "transitionner vers une classe de stockage. Elle ne sait pas le déplacer vers un",
        "autre bucket. MinIO permet bien une transition vers un « remote tier », mais",
        "celui-ci doit être un déploiement MinIO ou S3 **distinct** ; le pointer vers le",
        "bucket `archive` du même serveur n'est pas supporté et fait échouer la commande",
        "`mc ilm tier add`.",
        "",
        "L'archivage à 180 jours est donc réalisé par le DAG `dag_ilm_archive`, et l'ILM",
        "native assure la suppression définitive à 2 ans.",
        "",
        "## Uploads multipart incomplets",
        "",
        "MinIO refuse une règle ILM qui ne porterait qu'un `AbortIncompleteMultipartUpload`,",
        "et ignore silencieusement cette action lorsqu'elle accompagne une expiration :",
        "elle n'apparaît pas dans la configuration relue. Chez MinIO, la purge des",
        "uploads interrompus est un réglage serveur, pas une règle de bucket :",
        "",
        "```bash",
        "docker exec minio mc admin config get local api | tr ' ' '\\n' | grep stale",
        "# stale_uploads_cleanup_interval=6h",
        "# stale_uploads_expiry=24h",
        "```",
        "",
    ]
    output_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"✓ Rapport écrit : {output_path}")


def check_lifecycle(s3_client) -> int:
    """Affiche la configuration ILM courante sans rien modifier."""
    missing = 0
    for bucket in BUCKETS:
        try:
            rules = s3_client.get_bucket_lifecycle_configuration(Bucket=bucket)["Rules"]
        except ClientError as error:
            if error.response["Error"]["Code"] == "NoSuchLifecycleConfiguration":
                print(f"✗ {bucket:8s} : aucune règle ILM")
                missing += 1
                continue
            raise
        print(f"✓ {bucket:8s} : {len(rules)} règle(s)")
        for rule in rules:
            print(f"    · {describe_rule(rule)}")
    return 1 if missing else 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Configure les règles de cycle de vie ILM sur les buckets MinIO."
    )
    parser.add_argument("--endpoint-url", default=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"))
    parser.add_argument("--access-key", default=os.getenv("MINIO_ROOT_USER", "minioadmin"))
    parser.add_argument("--secret-key", default=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
    parser.add_argument("--report", default="JOUR5_ILM_POLICY.md")
    parser.add_argument("--check", action="store_true", help="Afficher l'ILM en place sans la modifier.")
    parser.add_argument("--dump-json", action="store_true", help="Afficher les règles au format JSON.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        s3_client = build_s3_client(args.endpoint_url, args.access_key, args.secret_key)

        if args.check:
            return check_lifecycle(s3_client)

        applied = {bucket: apply_lifecycle(s3_client, bucket) for bucket in BUCKETS}

        if args.dump_json:
            print(json.dumps(applied, indent=2, default=str))

        write_report(applied, Path(args.report))
        print("✓ Jour 5 — règles ILM configurées sur les 4 buckets.")
        return 0
    except EndpointConnectionError:
        print("✗ MinIO est inaccessible. Lancez d'abord : docker-compose up -d", file=sys.stderr)
        return 1
    except ClientError as error:
        print(f"✗ Erreur MinIO : {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
