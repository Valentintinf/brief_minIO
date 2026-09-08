#!/usr/bin/env python3
"""
Jour 5 — Catalogage OpenMetadata des 5 lignes de production.

Le script est idempotent : il peut être relancé sans créer de doublons, ce qui
permet de rejouer entièrement le Jour 5 sur un environnement neuf.

Enchaînement :
  1. authentification admin sur l'API REST ;
  2. service `minio_datalake` de type Datalake, connecté au bucket `staging` ;
  3. pipeline d'ingestion planifié, qui découvre les tables Parquet et exclut
     les chunks de LineA (une fiche par ligne, pas une par fichier) ;
  4. équipe et propriétaire métier ;
  5. classification et glossaire décrivant la sémantique de `label` ;
  6. enrichissement des 5 fiches : description, propriétaire, source,
     fréquence de collecte, rétention, documentation de chaque colonne ;
  7. rapport Markdown récapitulatif.

L'ingestion passe par un pipeline déployé sur l'Airflow d'OpenMetadata, et non
par un appel direct à la CLI `metadata ingest`. C'est délibéré : le serveur
injecte lui-même le JWT de l'`ingestion-bot` dans le DAG qu'il génère. Lancée à
la main avec le token admin, la CLI échoue sur un SignatureDoesNotMatch côté
MinIO, car l'API renvoie les secrets de connexion masqués à tout porteur autre
que ce bot.
"""

from __future__ import annotations

import argparse
import base64
import json
import sys
import time
from pathlib import Path
from urllib.parse import quote

import requests

SERVICE_NAME = "minio_datalake"
PIPELINE_NAME = "minio_datalake_metadata"
STAGING_BUCKET = "staging"
TEAM_NAME = "maintenance-industrielle"
OWNER_NAME = "responsable.maintenance"
CLASSIFICATION_NAME = "MaintenancePredictive"
GLOSSARY_NAME = "MaintenancePredictive"

ZENODO_URL = "https://zenodo.org/records/15277168"
SOURCE_LABEL = (
    "Synthetic Data from Industrial Sensor Monitoring — "
    "Polytechnic Institute of Porto / INESC TEC — Zenodo, avril 2025"
)

# Cadence réelle des mesures, mesurée sur les CSV : un point par minute.
COLLECTION_FREQUENCY = "1 mesure/minute (1440 points/jour/ligne)"

# Rétention alignée sur minio_ilm_setup.py : archivage 180 j, suppression 730 j
RETENTION_PERIOD_ISO = "P730D"


# ─── Fiches métier des 5 lignes ──────────────────────────────────────────────
# Statistiques recalculées depuis data/*.csv (cf. JOUR1_ARCHITECTURE.md §2).
LINES: dict[str, dict] = {
    "lineA": {
        "file": "LineA_Stable_10K.parquet",
        "display": "Ligne A — régime stable",
        "rows": 10_000,
        "period": "2025-05-01 → 2025-05-07",
        "temp_range": (179.49, 185.19, 180.01),
        "pressure_range": (156.80, 160.63, 160.00),
        "elapsed_range": (34.27, 37.23, 35.00),
        "anomalies": (18, 0.18),
        "behaviour": (
            "Comportement le plus stable des cinq lignes : température et pression "
            "restent dans une bande étroite autour de leur moyenne. Ligne de "
            "référence pour établir la baseline nominale des modèles de détection "
            "d’anomalies."
        ),
        "ingestion_note": (
            "Seule ligne ingérée par lots : 10 chunks de 1 000 enregistrements, pour "
            "simuler un flux continu. Les chunks sont exclus du catalogue, la fiche "
            "décrit le jeu consolidé."
        ),
    },
    "lineB": {
        "file": "LineB_Flux.parquet",
        "display": "Ligne B — flux moyen",
        "rows": 5_000,
        "period": "2025-04-01 → 2025-04-04",
        "temp_range": (186.76, 202.72, 190.12),
        "pressure_range": (108.95, 123.53, 119.90),
        "elapsed_range": (18.65, 22.98, 20.02),
        "anomalies": (50, 1.00),
        "behaviour": (
            "Régime intermédiaire, avec une pression sensiblement plus basse que la "
            "ligne A et une dispersion modérée. Taux d’anomalies de 1 %, "
            "représentatif d’une ligne en fonctionnement normal."
        ),
        "ingestion_note": (
            "Ingérée en fichier unique. Seule ligne, avec la A, à fournir "
            "**elapsed_time** — nommé **Elapsed_time** dans le CSV source."
        ),
    },
    "lineC": {
        "file": "LineC_Turbulent.parquet",
        "display": "Ligne C — régime turbulent",
        "rows": 5_000,
        "period": "2025-03-01 → 2025-03-04",
        "temp_range": (194.65, 216.27, 200.45),
        "pressure_range": (88.32, 104.73, 99.63),
        "elapsed_range": None,
        "anomalies": (200, 4.00),
        "behaviour": (
            "Ligne la plus instable du parc : amplitude de température de plus de "
            "21 °C et 4 % d’anomalies, soit le taux le plus élevé des cinq lignes. "
            "Jeu de données le plus riche pour l’entraînement d’un modèle de "
            "maintenance prédictive, mais aussi le plus exigeant en surveillance."
        ),
        "ingestion_note": (
            "Ingérée en fichier unique. Le CSV source ne fournit pas **elapsed_time** : "
            "la colonne est présente dans le schéma harmonisé mais entièrement nulle."
        ),
    },
    "lineD": {
        "file": "LineD_SpikeControl.parquet",
        "display": "Ligne D — pics contrôlés",
        "rows": 5_000,
        "period": "2025-02-01 → 2025-02-04",
        "temp_range": (194.81, 213.11, 200.03),
        "pressure_range": (90.43, 104.23, 99.96),
        "elapsed_range": None,
        "anomalies": (15, 0.30),
        "behaviour": (
            "Moyennes très proches de la ligne E, mais avec des pics ponctuels de "
            "température allant jusqu’à 213 °C. Le taux d’anomalies reste faible "
            "(0,30 %) : les pics sont majoritairement étiquetés nominaux, ce qui en "
            "fait un cas d’usage utile pour calibrer le seuil de faux positifs."
        ),
        "ingestion_note": (
            "Ingérée en fichier unique. Pas de **elapsed_time** dans le CSV source, et "
            "**Pressure** y porte une majuscule."
        ),
    },
    "lineE": {
        "file": "LineE_SmoothRun.parquet",
        "display": "Ligne E — fonctionnement lissé",
        "rows": 5_000,
        "period": "2025-01-01 → 2025-01-04",
        "temp_range": (198.38, 209.60, 200.04),
        "pressure_range": (96.75, 100.88, 99.98),
        "elapsed_range": None,
        "anomalies": (25, 0.50),
        "behaviour": (
            "Pression très resserrée (amplitude de 4 hPa seulement) pour une "
            "température comparable aux lignes C et D. Profil d’une ligne bien "
            "régulée, avec 0,50 % d’anomalies."
        ),
        "ingestion_note": (
            "Ingérée en fichier unique. Pas de **elapsed_time** dans le CSV source."
        ),
    },
}

# Partitions raw/ et staging/ des 5 lignes, pour reconstituer le FQN des tables
LINE_PARTITIONS = {
    "lineA": "year=2025/month=05",
    "lineB": "year=2025/month=04",
    "lineC": "year=2025/month=03",
    "lineD": "year=2025/month=02",
    "lineE": "year=2025/month=01",
}


def fr_number(value: int) -> str:
    """10000 → « 10 000 ». Séparateur de milliers à la française."""
    return f"{value:,}".replace(",", "\u202f")


def column_docs(line_key: str) -> dict[str, str]:
    """Documentation des 8 colonnes du schéma harmonisé, pour une ligne donnée."""
    info = LINES[line_key]
    t_min, t_max, t_avg = info["temp_range"]
    p_min, p_max, p_avg = info["pressure_range"]

    if info["elapsed_range"] is None:
        elapsed_doc = (
            "**Durée de cycle** — unité : secondes (s). Absente du CSV source de "
            f"cette ligne : la colonne existe pour garantir un schéma identique "
            "sur les cinq lignes, mais elle est entièrement nulle. Toute agrégation "
            "sur ce champ doit exclure cette ligne."
        )
    else:
        e_min, e_max, e_avg = info["elapsed_range"]
        elapsed_doc = (
            f"**Durée de cycle** — unité : secondes (s). Plage observée : "
            f"{e_min} → {e_max} s, moyenne {e_avg} s. Renommée depuis "
            "`elapsed_time`/`Elapsed_time` par l'harmonisation."
        )

    return {
        "timestamp": (
            "**Horodatage de la mesure** — pas de temps constant de 60 s. "
            f"Période couverte : {info['period']}. Normalisé en datetime par le DAG "
            "`dag_staging_transform` ; sert aussi de clé de partitionnement "
            "`year=`/`month=` dans MinIO."
        ),
        "line_id": (
            "**Identifiant de la ligne de production** — valeur constante "
            f"`{line_key}`. Colonne ajoutée à la transformation : elle est absente "
            "des CSV sources et rend les cinq jeux empilables en une table unique."
        ),
        "temperature": (
            "**Température du capteur** — unité : degrés Celsius (°C). Plage "
            f"nominale observée : {t_min} → {t_max} °C, moyenne {t_avg} °C. "
            "Harmonisée depuis `Temperature`/`temperature` selon la ligne source."
        ),
        "pressure": (
            "**Pression du capteur** — unité : hectopascals (hPa). Plage nominale "
            f"observée : {p_min} → {p_max} hPa, moyenne {p_avg} hPa. Harmonisée "
            "depuis `Pressure`/`pressure` selon la ligne source."
        ),
        "elapsed_time": elapsed_doc,
        "label": (
            "**Étiquette d'état** — variable cible de la maintenance prédictive. "
            "`0` = fonctionnement nominal, `1` = anomalie détectée. "
            f"Sur cette ligne : {info['anomalies'][0]} anomalies sur "
            f"{fr_number(info['rows'])} mesures, soit {info['anomalies'][1]} %. "
            "Domaine validé par le DAG de transformation, qui échoue sur toute "
            "autre valeur."
        ),
        "ingestion_ts": (
            "**Horodatage d'ingestion** — date UTC à laquelle le DAG "
            "`dag_staging_transform` a produit l'enregistrement. Colonne technique "
            "de traçabilité, à ne pas confondre avec `timestamp` (date de mesure)."
        ),
        "source_file_hash": (
            "**Empreinte MD5 du fichier `raw/` d'origine** — permet de relier chaque "
            "enregistrement de `staging/` à l'objet brut dont il provient et de "
            "détecter une réingestion sur un fichier source modifié."
        ),
    }


def table_description(line_key: str) -> str:
    """Fiche métier de la table, en Markdown.

    OpenMetadata assainit la description des tables contre les injections HTML :
    apostrophes droites, guillemets, esperluettes, accents graves et signes
    égal y sont échappés en entités, et tout ce qui ressemble à une balise est
    supprimé.
    Le texte ci-dessous n'emploie donc que des apostrophes typographiques et du
    gras, jamais de `code` ni de caractère échappable. Les descriptions de
    colonnes, elles, ne subissent pas ce filtre et peuvent rester en Markdown
    complet.
    """
    info = LINES[line_key]
    anom_count, anom_pct = info["anomalies"]
    t_min, t_max, t_avg = info["temp_range"]
    p_min, p_max, p_avg = info["pressure_range"]

    if info["elapsed_range"] is None:
        elapsed_row = "| Durée de cycle | s | absente du CSV source | — | — |"
    else:
        e_min, e_max, e_avg = info["elapsed_range"]
        elapsed_row = f"| Durée de cycle | s | {e_min} | {e_max} | {e_avg} |"

    # Le sanitizer échappe aussi le signe égal : la partition Hive
    # « year=2025/month=05 » est donc énoncée en clair.
    year, month = (part.split("=")[1] for part in LINE_PARTITIONS[line_key].split("/"))
    location = (
        f"s3://{STAGING_BUCKET}/production_lines/{line_key} — partition Hive "
        f"year {year} / month {month} — fichier {info['file']}"
    )

    return "\n".join([
        f"## {info['display']}",
        "",
        info["behaviour"],
        "",
        "### Identité du jeu de données",
        "",
        f"- **Source** : {SOURCE_LABEL}",
        f"- **Lien source** : {ZENODO_URL}",
        "- **Propriétaire** : Responsable maintenance, équipe Maintenance industrielle",
        f"- **Fréquence de collecte** : {COLLECTION_FREQUENCY}",
        f"- **Volumétrie** : {fr_number(info['rows'])} enregistrements",
        f"- **Période couverte** : {info['period']}",
        f"- **Emplacement** : {location}",
        "- **Format** : Parquet, compression snappy",
        "",
        "### Plages observées",
        "",
        "| Grandeur | Unité | Min | Max | Moyenne |",
        "|---|---|---:|---:|---:|",
        f"| Température | °C | {t_min} | {t_max} | {t_avg} |",
        f"| Pression | hPa | {p_min} | {p_max} | {p_avg} |",
        elapsed_row,
        "",
        f"**Anomalies** : {anom_count} enregistrements portent la valeur 1 dans la "
        f"colonne label, soit {anom_pct} % du jeu.",
        "",
        "### Chaîne d’alimentation",
        "",
        "CSV Zenodo → **dag_raw_ingestion** → couche raw → "
        "**dag_staging_transform** → couche staging, soit la présente table.",
        "",
        info["ingestion_note"],
        "",
        "### Cycle de vie",
        "",
        "Archivage vers la couche archive à 180 jours par **dag_ilm_archive**, "
        "suppression définitive à 730 jours par la règle ILM MinIO. "
        "Détail dans JOUR5_ILM_POLICY.md.",
    ])


class OpenMetadataClient:
    """Client minimal de l'API REST OpenMetadata."""

    def __init__(self, host_port: str, email: str, password: str):
        self.api = host_port.rstrip("/") + "/api/v1"
        self.session = requests.Session()
        self.token = self._login(email, password)
        self.session.headers["Authorization"] = f"Bearer {self.token}"

    def _login(self, email: str, password: str) -> str:
        # OpenMetadata attend le mot de passe encodé en base64
        encoded = base64.b64encode(password.encode()).decode()
        response = requests.post(
            f"{self.api}/users/login",
            json={"email": email, "password": encoded},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()["accessToken"]

    def get(self, path: str, **params) -> dict:
        response = self.session.get(f"{self.api}{path}", params=params, timeout=60)
        response.raise_for_status()
        return response.json()

    def find(self, path: str, **params) -> dict | None:
        """GET tolérant : renvoie None sur 404 au lieu de lever."""
        response = self.session.get(f"{self.api}{path}", params=params, timeout=60)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def post(self, path: str, payload: dict) -> dict:
        response = self.session.post(f"{self.api}{path}", json=payload, timeout=120)
        response.raise_for_status()
        return response.json()

    def put(self, path: str, payload: dict) -> dict:
        response = self.session.put(f"{self.api}{path}", json=payload, timeout=120)
        response.raise_for_status()
        return response.json()

    def patch(self, path: str, operations: list[dict]) -> dict:
        response = self.session.patch(
            f"{self.api}{path}",
            data=json.dumps(operations),
            headers={"Content-Type": "application/json-patch+json"},
            timeout=120,
        )
        response.raise_for_status()
        return response.json()


def ensure_service(client: OpenMetadataClient, minio_endpoint: str,
                   access_key: str, secret_key: str) -> dict:
    """Service Datalake pointant sur le bucket `staging` de MinIO."""
    payload = {
        "name": SERVICE_NAME,
        "displayName": "MinIO — Data Lake industriel",
        "serviceType": "Datalake",
        "description": (
            "Stockage objet S3-compatible du data lake industriel. Ce service "
            f"expose la couche `{STAGING_BUCKET}` : données capteurs harmonisées "
            "au format Parquet, une partition `year=`/`month=` par ligne de "
            "production."
        ),
        "connection": {"config": {
            "type": "Datalake",
            "configSource": {"securityConfig": {
                "awsAccessKeyId": access_key,
                "awsSecretAccessKey": secret_key,
                "awsRegion": "us-east-1",
                "endPointURL": minio_endpoint,
            }},
            "bucketName": STAGING_BUCKET,
        }},
    }
    # PUT est idempotent côté OpenMetadata : crée ou met à jour
    service = client.put("/services/databaseServices", payload)
    print(f"✓ Service Datalake : {service['fullyQualifiedName']} → {minio_endpoint}")
    return service


def ensure_pipeline(client: OpenMetadataClient, service: dict, schedule: str) -> dict:
    """Pipeline d'ingestion planifié, déployé sur l'Airflow d'OpenMetadata."""
    payload = {
        "name": PIPELINE_NAME,
        "displayName": "Catalogage MinIO staging",
        "pipelineType": "metadata",
        "service": {"id": service["id"], "type": "databaseService"},
        "sourceConfig": {"config": {
            "type": "DatabaseMetadata",
            # Les 10 chunks de LineA ne doivent pas produire 10 fiches : le
            # catalogue documente une ligne de production, pas un fichier.
            "tableFilterPattern": {"excludes": [".*_chunk_.*"]},
        }},
        "airflowConfig": {"scheduleInterval": schedule, "startDate": "2025-06-01"},
        "loggerLevel": "INFO",
    }
    existing = client.find(f"/services/ingestionPipelines/name/{SERVICE_NAME}.{PIPELINE_NAME}")
    pipeline = client.put("/services/ingestionPipelines", payload) if existing \
        else client.post("/services/ingestionPipelines", payload)

    # Le déploiement génère un DAG dans l'Airflow d'OpenMetadata. C'est le
    # serveur qui y place le JWT de l'ingestion-bot, seul porteur à qui l'API
    # livre les secrets de connexion en clair.
    client.post(f"/services/ingestionPipelines/deploy/{pipeline['id']}", {})
    print(f"✓ Pipeline déployé : {PIPELINE_NAME} (planification « {schedule} »)")
    return pipeline


def run_pipeline(client: OpenMetadataClient, pipeline: dict, timeout_s: int = 300) -> str:
    """Déclenche l'ingestion et attend son état terminal."""
    client.post(f"/services/ingestionPipelines/trigger/{pipeline['id']}", {})
    print("  … ingestion déclenchée, attente du résultat")

    fqn = f"{SERVICE_NAME}.{PIPELINE_NAME}"
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        time.sleep(10)
        status = client.get(
            f"/services/ingestionPipelines/name/{fqn}", fields="pipelineStatuses"
        ).get("pipelineStatuses") or {}
        state = status.get("pipelineState")
        if state in ("success", "partialSuccess", "failed"):
            steps = status.get("status") or []
            step = steps[0] if steps else {}
            print(
                f"✓ Ingestion « {state} » : {step.get('records', 0)} nouvelle(s), "
                f"{step.get('updated_records', 0)} mise(s) à jour, "
                f"{step.get('filtered', 0)} filtrée(s) (chunks LineA), "
                f"{step.get('errors', 0)} erreur(s)"
            )
            return state
    raise TimeoutError(f"L'ingestion n'a pas abouti en {timeout_s} s")


def ensure_owner(client: OpenMetadataClient) -> dict:
    """Équipe et utilisateur propriétaire des fiches."""
    team = client.find(f"/teams/name/{TEAM_NAME}")
    if team is None:
        team = client.post("/teams", {
            "name": TEAM_NAME,
            "displayName": "Maintenance industrielle",
            "description": (
                "Service responsable des lignes de production instrumentées et "
                "propriétaire métier des données capteurs du data lake."
            ),
            "teamType": "Group",
        })
    print(f"✓ Équipe : {team['displayName']}")

    owner = client.find(f"/users/name/{OWNER_NAME}")
    if owner is None:
        owner = client.post("/users", {
            "name": OWNER_NAME,
            "displayName": "Responsable maintenance",
            "email": f"{OWNER_NAME}@datalake.local",
            "description": (
                "Propriétaire métier des jeux de données capteurs : valide les "
                "plages nominales, arbitre les demandes d'accès et qualifie les "
                "anomalies remontées par les modèles."
            ),
            # L'API attend des UUID d'équipes, pas des noms
            "teams": [team["id"]],
        })
    print(f"✓ Propriétaire : {owner['displayName']} ({owner['email']})")
    return owner


def ensure_glossary(client: OpenMetadataClient) -> None:
    """Glossaire métier documentant la sémantique de `label`.

    Les descriptions de glossaire passent par le même sanitizer que celles des
    tables : ni backtick ni apostrophe droite ici non plus.
    """
    client.put("/glossaries", {
        "name": GLOSSARY_NAME,
        "displayName": "Maintenance prédictive",
        "description": (
            "Vocabulaire métier du data lake industriel : états d’équipement et "
            "grandeurs mesurées par les capteurs de ligne."
        ),
    })
    print("✓ Glossaire : Maintenance prédictive")

    terms = [
        ("EtatNominal", "État nominal", (
            "Fonctionnement de la ligne conforme aux plages attendues. Correspond "
            "à la valeur 0 de la colonne label dans les tables staging, soit 96 à "
            "99,8 % des mesures selon la ligne."
        )),
        ("Anomalie", "Anomalie", (
            "Écart de comportement détecté sur la ligne de production. Correspond "
            "à la valeur 1 de la colonne label dans les tables staging. C’est la "
            "variable cible des futurs modèles de maintenance prédictive : la "
            "prédire à l’avance permet d’intervenir avant l’arrêt non planifié."
        )),
        ("DureeDeCycle", "Durée de cycle", (
            "Temps écoulé sur un cycle de production, en secondes, porté par la "
            "colonne elapsed_time. Renseignée uniquement sur les lignes A et B : "
            "les CSV sources des lignes C, D et E ne fournissent pas cette "
            "grandeur."
        )),
    ]
    for name, display, description in terms:
        # PUT plutôt que POST : une relance corrige une description existante
        # au lieu de la laisser telle quelle.
        term = client.put("/glossaryTerms", {
            "name": name,
            "displayName": display,
            "description": description,
            "glossary": GLOSSARY_NAME,
        })
        altered = "" if term.get("description") == description else \
            "  ⚠ description réécrite par le sanitizer"
        print(f"    · terme : {display}{altered}")


def table_fqn(line_key: str) -> str:
    """FQN OpenMetadata de la table d'une ligne, tel que produit par l'ingestion."""
    path = (
        f"production_lines/{line_key}/{LINE_PARTITIONS[line_key]}/"
        f"{LINES[line_key]['file']}"
    )
    return f'{SERVICE_NAME}.default.{STAGING_BUCKET}."{path}"'


def enrich_table(client: OpenMetadataClient, line_key: str, owner: dict) -> dict:
    """Pose description, propriétaire, source, rétention et doc des colonnes."""
    fqn = table_fqn(line_key)
    table = client.find(f"/tables/name/{quote(fqn, safe='')}", fields="columns,owners")
    if table is None:
        raise LookupError(
            f"Table absente du catalogue : {fqn}\n"
            "L'ingestion a-t-elle bien tourné ? Vérifiez que staging/ est peuplé "
            "(DAG dag_staging_transform)."
        )

    operations = [
        {"op": "add", "path": "/displayName", "value": LINES[line_key]["display"]},
        {"op": "add", "path": "/description", "value": table_description(line_key)},
        {"op": "add", "path": "/sourceUrl", "value": ZENODO_URL},
        {"op": "add", "path": "/retentionPeriod", "value": RETENTION_PERIOD_ISO},
        {"op": "add", "path": "/owners",
         "value": [{"id": owner["id"], "type": "user"}]},
    ]

    # Les colonnes sont adressées par index dans un JSON Patch : on résout le
    # nom vers sa position dans le schéma réellement découvert par l'ingestion.
    docs = column_docs(line_key)
    documented = 0
    for index, column in enumerate(table.get("columns", [])):
        doc = docs.get(column["name"])
        if doc is None:
            continue
        operations.append(
            {"op": "add", "path": f"/columns/{index}/description", "value": doc}
        )
        documented += 1

    updated = client.patch(f"/tables/{table['id']}", operations)

    # Le sanitizer d'OpenMetadata peut réécrire la description sans prévenir :
    # on relit ce qui a réellement été stocké plutôt que de le supposer.
    sent = table_description(line_key)
    altered = "" if updated.get("description") == sent else \
        "  ⚠ description réécrite par le sanitizer OpenMetadata"
    print(f"✓ Fiche {line_key} : propriétaire, source, rétention, "
          f"{documented}/{len(docs)} colonnes documentées{altered}")
    return updated


def write_report(client: OpenMetadataClient, tables: dict[str, dict],
                 output_path: Path, host_port: str) -> None:
    lines = [
        "# Catalogue OpenMetadata — Jour 5 (C20)",
        "",
        "Généré par `openmetadata_catalog.py`, à partir des fiches relues sur le",
        f"serveur OpenMetadata ({client.get('/system/version')['version']}).",
        "",
        f"Console : <{host_port}> — `admin@open-metadata.org` / `admin`",
        "",
        "## Service et pipeline",
        "",
        f"| Élément | Valeur |",
        "|---|---|",
        f"| Service | `{SERVICE_NAME}` (type Datalake) |",
        f"| Couche cataloguée | bucket `{STAGING_BUCKET}` |",
        f"| Pipeline d'ingestion | `{PIPELINE_NAME}` |",
        f"| Propriétaire | Responsable maintenance / Maintenance industrielle |",
        f"| Source | {SOURCE_LABEL} |",
        f"| Fréquence de collecte | {COLLECTION_FREQUENCY} |",
        "",
        "## Les 5 fiches",
        "",
        "| Ligne | Fiche | Enregistrements | Anomalies | Colonnes documentées | Propriétaire |",
        "|---|---|---:|---:|---:|---|",
    ]
    for line_key, table in tables.items():
        info = LINES[line_key]
        documented = sum(1 for c in table.get("columns", []) if c.get("description"))
        owners = ", ".join(o.get("displayName", o["name"]) for o in table.get("owners", []))
        lines.append(
            f"| {line_key} | {info['display']} | {info['rows']:,} ".replace(",", " ")
            + f"| {info['anomalies'][0]} ({info['anomalies'][1]} %) "
            f"| {documented}/{len(table.get('columns', []))} | {owners} |"
        )

    lines += [
        "",
        "## Schéma harmonisé documenté",
        "",
        "Les cinq fiches partagent le même schéma, ce qui valide l'harmonisation du",
        "Jour 3-4 : c'est l'ingestion OpenMetadata qui a lu les Parquet et découvert",
        "ces colonnes, elles n'ont pas été déclarées à la main.",
        "",
        "| Colonne | Type | Unité | Rôle |",
        "|---|---|---|---|",
        "| `timestamp` | DATETIME | — | horodatage de la mesure, pas de 60 s |",
        "| `line_id` | STRING | — | identifiant de ligne, ajouté à la transformation |",
        "| `temperature` | FLOAT | °C | température capteur |",
        "| `pressure` | FLOAT | hPa | pression capteur |",
        "| `elapsed_time` | FLOAT | s | durée de cycle, nulle sur C/D/E |",
        "| `label` | INT | — | **0 = nominal, 1 = anomalie** (variable cible) |",
        "| `ingestion_ts` | DATETIME | — | date de production de l'enregistrement |",
        "| `source_file_hash` | STRING | — | MD5 du fichier `raw/` d'origine |",
        "",
        "## Glossaire",
        "",
        "Glossaire « Maintenance prédictive » : `État nominal` (label = 0),",
        "`Anomalie` (label = 1), `Durée de cycle` (`elapsed_time`).",
        "",
        "## Rejouer le catalogage",
        "",
        "```bash",
        "docker-compose -f docker-compose.yml -f docker-compose.openmetadata.yml up -d",
        "python openmetadata_catalog.py",
        "```",
        "",
        "Le script est idempotent : une relance met à jour les fiches sans créer de",
        "doublons.",
        "",
    ]
    output_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"✓ Rapport écrit : {output_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Catalogue les 5 lignes de production dans OpenMetadata."
    )
    parser.add_argument("--host-port", default="http://localhost:8585")
    parser.add_argument("--email", default="admin@open-metadata.org")
    parser.add_argument("--password", default="admin")
    parser.add_argument("--minio-endpoint", default="http://minio:9000",
                        help="Endpoint MinIO vu depuis les conteneurs OpenMetadata.")
    parser.add_argument("--access-key", default="minioadmin")
    parser.add_argument("--secret-key", default="minioadmin")
    parser.add_argument("--schedule", default="0 3 * * *",
                        help="Planification cron du rafraîchissement du catalogue.")
    parser.add_argument("--skip-ingestion", action="store_true",
                        help="Ne pas relancer l'ingestion, enrichir les fiches existantes.")
    parser.add_argument("--report", default="JOUR5_CATALOG.md")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        client = OpenMetadataClient(args.host_port, args.email, args.password)
        print(f"✓ Connecté à OpenMetadata "
              f"{client.get('/system/version')['version']} ({args.host_port})")

        service = ensure_service(client, args.minio_endpoint,
                                 args.access_key, args.secret_key)

        if not args.skip_ingestion:
            pipeline = ensure_pipeline(client, service, args.schedule)
            state = run_pipeline(client, pipeline)
            if state == "failed":
                print("✗ L'ingestion a échoué. Consultez les logs du pipeline dans "
                      "OpenMetadata (Ingestions → Logs).", file=sys.stderr)
                return 1

        owner = ensure_owner(client)
        ensure_glossary(client)

        tables = {key: enrich_table(client, key, owner) for key in LINES}
        write_report(client, tables, Path(args.report), args.host_port)

        print(f"✓ Jour 5 — {len(tables)} fiches catalographiées dans OpenMetadata.")
        return 0
    except requests.exceptions.ConnectionError:
        print(f"✗ OpenMetadata est inaccessible sur {args.host_port}.\n"
              "  Lancez : docker-compose -f docker-compose.yml "
              "-f docker-compose.openmetadata.yml up -d", file=sys.stderr)
        return 1
    except requests.exceptions.HTTPError as error:
        body = error.response.text[:600] if error.response is not None else ""
        print(f"✗ Erreur API OpenMetadata : {error}\n  {body}", file=sys.stderr)
        return 1
    except (LookupError, TimeoutError) as error:
        print(f"✗ {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
