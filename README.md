# Data Lake industriel — MinIO

Conception et déploiement d'un data lake moderne pour centraliser, documenter et sécuriser des données issues de lignes de production instrumentées, en préparation d'un projet de maintenance prédictive.

**Contexte** : 8 jours de brief — titre professionnel, compétences C18 à C21.

---

## Architecture en couches (Medallion)

```
raw/        Données brutes inchangées, partitionnées year=/month=/
staging/    Données harmonisées, typées, format Parquet (snappy)
curated/    Données prêtes à l'analyse métier
archive/    Données expirées selon règles ILM (180 j → archivage, 2 ans → suppression)
```

Schéma technique détaillé : [architecture_datalake_v2.drawio](architecture_datalake_v2.drawio)

---

## Stack technique

| Composant | Rôle |
|---|---|
| MinIO Community | Stockage objet S3-compatible (4 buckets) |
| Apache Airflow 2.9.3 | Orchestration des pipelines (LocalExecutor) |
| PostgreSQL 15 | Base de métadonnées Airflow |
| boto3 | SDK Python pour l'accès S3 |
| pandas + pyarrow | Transformation et sérialisation Parquet |
| Docker Compose | Infrastructure locale reproductible |

---

## Données source

**Synthetic Data from Industrial Sensor Monitoring** — Polytechnic Institute of Porto / INESC TEC — [Zenodo, avril 2025](https://zenodo.org/records/15277168)

| Fichier | Ligne | Enregistrements | Particularité |
|---|---|---|---|
| `LineA_Stable_10K.csv` | A | 10 000 | Ingestion en 10 chunks de 1 000 |
| `LineB_Flux.csv` | B | 5 000 | Flux moyen |
| `LineC_Turbulent.csv` | C | 5 000 | Pas de colonne `elapsed_time` |
| `LineD_SpikeControl.csv` | D | 5 000 | Pas de colonne `elapsed_time` |
| `LineE_SmoothRun.csv` | E | 5 000 | Pas de colonne `elapsed_time` |

Hétérogénéités à traiter : casse des colonnes (`Temperature` / `temperature`, `Pressure` / `pressure`) et présence conditionnelle de `elapsed_time`.

---

## Démarrage rapide

### Prérequis

- Docker + Docker Compose v2
- Python 3.11+ avec les dépendances (`pip install -r requirements.txt`)
- Fichier `.env` à créer depuis `.env.example` :

```bash
cp .env.example .env
# Sur Linux/WSL : ajuster l'UID
echo "AIRFLOW_UID=$(id -u)" >> .env
```

### Lancer l'infrastructure

```bash
docker-compose up -d
```

Services démarrés :

| Service | URL | Identifiants |
|---|---|---|
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| Airflow Webserver | http://localhost:8080 | `admin` / `admin` |

> Sur WSL2, si `localhost` ne répond pas depuis Windows, utiliser l'IP WSL : `hostname -I | awk '{print $1}'`

### Ingestion et transformation

```bash
# DAG #1 — CSV → raw/ (partitionné, LineA en 10 chunks)
docker exec airflow-scheduler airflow dags trigger dag_raw_ingestion

# DAG #2 — raw/ → staging/ (Parquet snappy, colonnes harmonisées)
docker exec airflow-scheduler airflow dags trigger dag_staging_transform
```

---

## Structure du dépôt

```
brief_minIO/
├── data/                          ← CSV sources (non commités)
├── dags/
│   ├── dag_raw_ingestion.py       ← DAG #1 : ingestion vers raw/
│   └── dag_staging_transform.py   ← DAG #2 : transformation vers staging/
├── logs/                          ← Logs Airflow (ignorés par git)
├── minio-data/                    ← Données MinIO persistées (ignorées par git)
├── architecture_datalake.drawio   ← Schéma v1
├── architecture_datalake_v2.drawio← Schéma v2 (version finale)
├── docker-compose.yml             ← Infrastructure complète
├── upload_to_minio.py             ← Script Jour 2 (upload + intégrité MD5)
├── requirements.txt
├── .env.example                   ← Template à copier en .env
├── JOUR1_ARCHITECTURE.md          ← Analyse des données + choix d'architecture
├── JOUR2_README.md                ← MinIO, buckets, policies, upload boto3
└── JOUR3_README.md                ← Airflow, DAGs, procédure complète
```

---

## Livrables réalisés

| Compétence | Livrable | Détail |
|---|---|---|
| C18 | Architecture + schéma draw.io | Analyse des 5 lignes, choix Medallion, `architecture_datalake_v2.drawio` |
| C19 | MinIO, buckets, policies, upload MD5 | 4 buckets créés, policies différenciées par bucket, intégrité vérifiée |
| C19 | DAGs Airflow ingestion + staging | DAG #1 raw partitionné, LineA en chunks, DAG #2 Parquet snappy harmonisé |
