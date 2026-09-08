#!/usr/bin/env python3
"""
Jours 6-7 (C21) — Comptes de service, policies différenciées et chiffrement.

Le script est idempotent : il peut être relancé sur un MinIO déjà configuré.

Enchaînement :
  1. création des 3 policies MinIO, une par rôle ;
  2. création des 3 comptes de service et rattachement de leur policy ;
  3. activation du chiffrement SSE-S3 sur les buckets de production ;
  4. vérification empirique des droits : chaque compte tente les 4 opérations S3
     sur les 4 buckets, et le résultat réel est comparé à la matrice attendue ;
  5. rapport `JOUR6_ACCESS_MATRIX.md`.

L'étape 4 est le cœur du script. Une policy n'est pas « configurée » parce
qu'elle a été envoyée : elle l'est quand un client muni de ces identifiants se
voit effectivement autoriser ou refuser chaque opération. Le script échoue si un
seul écart apparaît entre la matrice attendue et le comportement observé.

Les comptes MinIO relèvent de l'API d'administration, hors du périmètre de
boto3 : ils sont créés via le client `mc` embarqué dans l'image MinIO.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, EndpointConnectionError

BUCKETS = ("raw", "staging", "curated", "archive")

# Buckets porteurs de données de production, donc potentiellement sensibles
# industriellement : ce sont eux qui reçoivent le chiffrement SSE-S3.
ENCRYPTED_BUCKETS = ("raw", "staging", "curated", "archive")

ACTIONS = ("ListBucket", "GetObject", "PutObject", "DeleteObject")

PROBE_PREFIX = "_verif_acl"


# ─── Les 3 rôles ─────────────────────────────────────────────────────────────
# `expected` décrit les droits attendus bucket par bucket, sous la forme
# (ListBucket, GetObject, PutObject, DeleteObject).
ROLES: dict[str, dict] = {
    "data-analyst": {
        "secret_env": "MINIO_ANALYST_SECRET",
        "secret_default": "Analyst-2025-datalake",
        "description": "Lecture seule sur curated/ — consommation analytique",
        "readwrite": (),
        "readonly": ("curated",),
        "admin": False,
    },
    "data-engineer": {
        "secret_env": "MINIO_ENGINEER_SECRET",
        "secret_default": "Engineer-2025-datalake",
        "description": "Lecture/écriture sur raw/, staging/ et curated/ — pipelines",
        "readwrite": ("raw", "staging", "curated"),
        "readonly": (),
        "admin": False,
    },
    "admin": {
        "secret_env": "MINIO_ADMIN_SECRET",
        "secret_default": "Admin-2025-datalake",
        "description": "Tous droits sur les 4 buckets et l'administration MinIO",
        "readwrite": BUCKETS,
        "readonly": (),
        "admin": True,
    },
}

_OBJECT_ACTIONS = ("s3:GetObject", "s3:PutObject", "s3:DeleteObject")
_BUCKET_ACTIONS = ("s3:ListBucket", "s3:GetBucketLocation")


def build_policy(role: str) -> dict:
    """Document de policy MinIO correspondant au rôle."""
    config = ROLES[role]

    if config["admin"]:
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "TousDroitsS3",
                    "Effect": "Allow",
                    "Action": ["s3:*"],
                    "Resource": ["arn:aws:s3:::*"],
                },
                {
                    # Actions d'administration : créer des comptes, lire la
                    # configuration, consulter les métriques du serveur.
                    "Sid": "AdministrationMinIO",
                    "Effect": "Allow",
                    "Action": ["admin:*"],
                    "Resource": ["arn:aws:s3:::*"],
                },
            ],
        }

    statements = []
    for bucket in config["readwrite"]:
        statements.append({
            "Sid": f"Ecriture{bucket.capitalize()}",
            "Effect": "Allow",
            "Action": list(_OBJECT_ACTIONS),
            "Resource": [f"arn:aws:s3:::{bucket}/*"],
        })
    for bucket in config["readonly"]:
        statements.append({
            "Sid": f"Lecture{bucket.capitalize()}",
            "Effect": "Allow",
            "Action": ["s3:GetObject"],
            "Resource": [f"arn:aws:s3:::{bucket}/*"],
        })

    # Le listing est un droit de bucket, pas d'objet : il porte sur l'ARN du
    # bucket lui-même, sans /*. Sans lui, un client ne peut pas énumérer.
    listable = tuple(config["readwrite"]) + tuple(config["readonly"])
    if listable:
        statements.append({
            "Sid": "ListageBuckets",
            "Effect": "Allow",
            "Action": list(_BUCKET_ACTIONS),
            "Resource": [f"arn:aws:s3:::{bucket}" for bucket in listable],
        })

    return {"Version": "2012-10-17", "Statement": statements}


def expected_rights(role: str, bucket: str) -> tuple[bool, bool, bool, bool]:
    """Droits attendus (List, Get, Put, Delete) du rôle sur le bucket."""
    config = ROLES[role]
    if bucket in config["readwrite"]:
        return (True, True, True, True)
    if bucket in config["readonly"]:
        return (True, True, False, False)
    return (False, False, False, False)


def role_secret(role: str) -> str:
    config = ROLES[role]
    return os.getenv(config["secret_env"], config["secret_default"])


# ─── Pilotage du client mc ───────────────────────────────────────────────────
class MinioAdmin:
    """Opérations d'administration MinIO, via le client `mc` du conteneur."""

    def __init__(self, container: str, alias: str = "local"):
        self.container = container
        self.alias = alias

    def mc(self, *args: str, check: bool = True,
           stdin: str | None = None) -> subprocess.CompletedProcess:
        command = ["docker", "exec"]
        if stdin is not None:
            command.append("-i")
        command += [self.container, "mc", *args]
        result = subprocess.run(
            command, input=stdin, capture_output=True, text=True, timeout=120
        )
        if check and result.returncode != 0:
            raise RuntimeError(
                f"mc {' '.join(args)} a échoué :\n"
                f"{result.stdout.strip()}\n{result.stderr.strip()}"
            )
        return result

    def set_alias(self, endpoint: str, access_key: str, secret_key: str) -> None:
        self.mc("alias", "set", self.alias, endpoint, access_key, secret_key)

    def apply_policy(self, name: str, document: dict) -> None:
        # mc lit le document depuis un fichier : on l'écrit dans le conteneur
        # via l'entrée standard de `tee`, pour ne rien laisser sur l'hôte.
        remote_path = f"/tmp/policy-{name}.json"
        subprocess.run(
            ["docker", "exec", "-i", self.container, "tee", remote_path],
            input=json.dumps(document, indent=2), capture_output=True,
            text=True, check=True, timeout=60,
        )
        # `policy create` est idempotent : il remplace un document existant.
        self.mc("admin", "policy", "create", self.alias, name, remote_path)
        self.mc("rm", "-f", remote_path, check=False)

    def ensure_user(self, access_key: str, secret_key: str) -> None:
        # `user add` crée le compte s'il est absent, et réaligne son secret s'il
        # existe déjà : c'est ce qui rend la rotation possible sans suppression
        # ni recréation, et le script rejouable.
        self.mc("admin", "user", "add", self.alias, access_key, secret_key)

    def attach_policy(self, policy: str, user: str) -> None:
        result = self.mc("admin", "policy", "attach", self.alias, policy,
                         "--user", user, check=False)
        combined = result.stdout + result.stderr
        # Un rattachement déjà en place n'est pas une erreur
        if result.returncode != 0 and "already" not in combined.lower():
            raise RuntimeError(f"Rattachement {policy} → {user} impossible :\n{combined}")

    def enable_sse(self, bucket: str) -> None:
        self.mc("encrypt", "set", "sse-s3", f"{self.alias}/{bucket}")

    def sse_status(self, bucket: str) -> str:
        result = self.mc("encrypt", "info", f"{self.alias}/{bucket}", check=False)
        output = (result.stdout + result.stderr).strip()
        if "sse-s3" in output:
            return "SSE-S3"
        if "sse-kms" in output:
            return "SSE-KMS"
        return "aucun"


# ─── Vérification empirique des droits ───────────────────────────────────────
def s3_client(endpoint: str, access_key: str, secret_key: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4", retries={"max_attempts": 1}),
        region_name="us-east-1",
    )


def _attempt(operation) -> bool:
    """True si l'opération aboutit, False si MinIO la refuse."""
    try:
        operation()
        return True
    except ClientError as error:
        code = error.response["Error"]["Code"]
        if code in ("AccessDenied", "AllAccessDisabled", "InvalidAccessKeyId",
                    "SignatureDoesNotMatch"):
            return False
        # NoSuchKey sur un GetObject signifie que la lecture était autorisée :
        # le refus serait remonté avant l'absence de clé.
        if code in ("NoSuchKey", "NoSuchBucket"):
            return True
        raise


def probe_rights(endpoint: str, role: str, bucket: str) -> tuple[bool, bool, bool, bool]:
    """Teste réellement les 4 opérations du rôle sur le bucket."""
    client = s3_client(endpoint, role, role_secret(role))
    seeded_key = f"{PROBE_PREFIX}/lecture.txt"
    own_key = f"{PROBE_PREFIX}/{role}.txt"

    can_list = _attempt(
        lambda: client.list_objects_v2(Bucket=bucket, MaxKeys=1)
    )
    can_get = _attempt(
        lambda: client.get_object(Bucket=bucket, Key=seeded_key)
    )
    can_put = _attempt(
        lambda: client.put_object(Bucket=bucket, Key=own_key, Body=b"verification")
    )
    # Ne tester la suppression que sur un objet dont on est l'auteur, pour ne
    # jamais risquer d'effacer une donnée du data lake.
    can_delete = _attempt(
        lambda: client.delete_object(Bucket=bucket, Key=own_key)
    ) if can_put else _attempt(
        lambda: client.delete_object(Bucket=bucket, Key=f"{PROBE_PREFIX}/inexistant")
    )

    return (can_list, can_get, can_put, can_delete)


def seed_probes(root_client) -> None:
    """Dépose l'objet que les comptes en lecture seule tenteront de lire."""
    for bucket in BUCKETS:
        root_client.put_object(
            Bucket=bucket,
            Key=f"{PROBE_PREFIX}/lecture.txt",
            Body=b"objet temoin pour la verification des droits\n",
        )


def clear_probes(root_client) -> None:
    for bucket in BUCKETS:
        response = root_client.list_objects_v2(Bucket=bucket, Prefix=f"{PROBE_PREFIX}/")
        for obj in response.get("Contents", []):
            root_client.delete_object(Bucket=bucket, Key=obj["Key"])


def verify_matrix(endpoint: str, root_client) -> tuple[dict, list[str]]:
    """Compare droits observés et droits attendus. Renvoie (matrice, écarts)."""
    seed_probes(root_client)
    matrix: dict[tuple[str, str], tuple] = {}
    deviations: list[str] = []
    try:
        for role in ROLES:
            for bucket in BUCKETS:
                observed = probe_rights(endpoint, role, bucket)
                expected = expected_rights(role, bucket)
                matrix[(role, bucket)] = observed
                for action, got, want in zip(ACTIONS, observed, expected):
                    if got != want:
                        deviations.append(
                            f"{role} / {bucket} / {action} : "
                            f"attendu {'autorisé' if want else 'refusé'}, "
                            f"observé {'autorisé' if got else 'refusé'}"
                        )
    finally:
        clear_probes(root_client)
    return matrix, deviations


# ─── Rapport ─────────────────────────────────────────────────────────────────
def write_report(matrix: dict, sse_status: dict[str, str],
                 output_path: Path, endpoint: str) -> None:
    mark = {True: "✓", False: "—"}
    lines = [
        "# Matrice des droits d'accès MinIO — Jours 6-7 (C21)",
        "",
        "Généré par `minio_governance_setup.py`. Les droits ci-dessous ne sont pas",
        "recopiés des policies : ils sont **observés**, en tentant réellement chaque",
        "opération avec les identifiants de chaque compte.",
        "",
        f"Endpoint testé : `{endpoint}`",
        "",
        "## Comptes de service",
        "",
        "| Compte | Policy | Rôle |",
        "|---|---|---|",
    ]
    for role, config in ROLES.items():
        lines.append(f"| `{role}` | `{role}` | {config['description']} |")

    lines += [
        "",
        "## Droits observés",
        "",
        "`L` = ListBucket · `G` = GetObject · `P` = PutObject · `D` = DeleteObject",
        "",
        "| Compte | Bucket | L | G | P | D |",
        "|---|---|:-:|:-:|:-:|:-:|",
    ]
    for (role, bucket), observed in matrix.items():
        cells = " | ".join(mark[value] for value in observed)
        lines.append(f"| `{role}` | `{bucket}` | {cells} |")

    lines += [
        "",
        "## Chiffrement au repos",
        "",
        "| Bucket | Chiffrement par défaut |",
        "|---|---|",
    ]
    for bucket, status in sse_status.items():
        lines.append(f"| `{bucket}` | {status} |")

    lines += [
        "",
        "Tout objet déposé dans ces buckets est chiffré par MinIO sans que le client",
        "ait à le demander. Vérification :",
        "",
        "```bash",
        "python minio_governance_setup.py --check",
        "docker exec minio mc encrypt info local/raw",
        "```",
        "",
        "## Reproduire",
        "",
        "```bash",
        "python minio_governance_setup.py",
        "```",
        "",
        "Le script échoue si un seul droit observé s'écarte de la matrice attendue.",
        "",
    ]
    output_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"✓ Rapport écrit : {output_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Comptes de service, policies différenciées et chiffrement MinIO."
    )
    parser.add_argument("--endpoint-url", default=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"))
    parser.add_argument("--internal-endpoint", default="http://localhost:9000",
                        help="Endpoint vu depuis le conteneur MinIO, pour l'alias mc.")
    parser.add_argument("--root-user", default=os.getenv("MINIO_ROOT_USER", "minioadmin"))
    parser.add_argument("--root-password", default=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
    parser.add_argument("--container", default="minio")
    parser.add_argument("--report", default="JOUR6_ACCESS_MATRIX.md")
    parser.add_argument("--check", action="store_true",
                        help="Vérifier les droits et le chiffrement sans rien modifier.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        admin = MinioAdmin(args.container)
        root = s3_client(args.endpoint_url, args.root_user, args.root_password)

        # L'alias mc est nécessaire même en --check, pour lire l'état du
        # chiffrement : un conteneur recréé n'en conserve aucun.
        admin.set_alias(args.internal_endpoint, args.root_user, args.root_password)

        if not args.check:
            for role in ROLES:
                admin.apply_policy(role, build_policy(role))
                print(f"✓ Policy créée : {role}")

            for role in ROLES:
                admin.ensure_user(role, role_secret(role))
                admin.attach_policy(role, role)
                print(f"✓ Compte de service : {role} ← policy {role}")

            for bucket in ENCRYPTED_BUCKETS:
                admin.enable_sse(bucket)
                print(f"✓ Chiffrement SSE-S3 activé : {bucket}")

        print("\n… vérification des droits (3 comptes × 4 buckets × 4 opérations)")
        matrix, deviations = verify_matrix(args.endpoint_url, root)

        mark = {True: "✓", False: "—"}
        for role in ROLES:
            row = "  ".join(
                f"{bucket}:" + "".join(mark[v] for v in matrix[(role, bucket)])
                for bucket in BUCKETS
            )
            print(f"    {role:14s} {row}")

        sse_status = {bucket: admin.sse_status(bucket) for bucket in BUCKETS}
        print(f"\n✓ Chiffrement : "
              + ", ".join(f"{b}={s}" for b, s in sse_status.items()))

        if deviations:
            print(f"\n✗ {len(deviations)} écart(s) entre droits attendus et observés :",
                  file=sys.stderr)
            for deviation in deviations:
                print(f"    · {deviation}", file=sys.stderr)
            return 1

        checked = len(ROLES) * len(BUCKETS) * len(ACTIONS)
        print(f"✓ Aucun écart : les {checked} droits observés correspondent "
              "à la matrice attendue.")
        write_report(matrix, sse_status, Path(args.report), args.endpoint_url)
        print("✓ Vérification terminée." if args.check
              else "✓ Jours 6-7 — comptes, policies et chiffrement en place.")
        return 0

    except EndpointConnectionError:
        print("✗ MinIO est inaccessible. Lancez d'abord : docker-compose up -d",
              file=sys.stderr)
        return 1
    except (RuntimeError, subprocess.SubprocessError) as error:
        print(f"✗ {error}", file=sys.stderr)
        return 1
    except ClientError as error:
        print(f"✗ Erreur MinIO : {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
