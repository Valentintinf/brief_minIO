# Data Lake industriel — MinIO

Conception et déploiement d'un data lake moderne pour centraliser, documenter et sécuriser des données issues de lignes de production instrumentées, en préparation d'un projet de maintenance prédictive.

**Contexte** : 8 jours de brief — titre professionnel, compétences C18 à C21.

---

## Architecture en couches (Medallion)

```
raw/        Données brutes inchangées, partitionnées year=/month=/
staging/    Données harmonisées, typées, format Parquet (snappy)
curated/    Données prêtes à l'analyse métier
archive/    Données archivées à 180 j, supprimées à 2 ans (ILM MinIO + DAG)
```

Schéma technique détaillé : [architecture_datalake_v2.drawio](architecture_datalake_v2.drawio)

---

## Stack technique

| Composant | Rôle |
|---|---|
| MinIO Community | Stockage objet S3-compatible (4 buckets) |
| Apache Airflow 2.9.3 | Orchestration des pipelines (LocalExecutor) |
| PostgreSQL 15 | Base de métadonnées Airflow |
| OpenMetadata 1.13.5 | Catalogue de données, glossaire, propriétaires |
| MySQL 8 + Elasticsearch | Stockage et index d'OpenMetadata |
| KMS interne MinIO | Clé maîtresse du chiffrement SSE-S3 |
| `audit-collector` | Réception et persistance des logs d'audit MinIO |
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
# Data lake : MinIO + Airflow + PostgreSQL
docker-compose up -d

# Avec le catalogue OpenMetadata (compter ~6 Go de RAM)
docker-compose -f docker-compose.yml -f docker-compose.openmetadata.yml up -d
```

Services démarrés :

| Service | URL | Identifiants |
|---|---|---|
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| Airflow Webserver | http://localhost:8080 | `admin` / `admin` |
| OpenMetadata | http://localhost:8585 | `admin@open-metadata.org` / `admin` |
| Airflow OpenMetadata | http://localhost:8081 | `admin` / `admin` |

> Les deux Airflow sont distincts : celui du port 8080 orchestre les pipelines de
> données, celui du port 8081 n'exécute que les workflows de catalogage
> d'OpenMetadata.

> Sur WSL2, si `localhost` ne répond pas depuis Windows, utiliser l'IP WSL : `hostname -I | awk '{print $1}'`

### Ingestion et transformation

```bash
# DAG #1 — CSV → raw/ (partitionné, LineA en 10 chunks)
docker exec airflow-scheduler airflow dags trigger dag_raw_ingestion

# DAG #2 — raw/ → staging/ (Parquet snappy, colonnes harmonisées)
docker exec airflow-scheduler airflow dags trigger dag_staging_transform
```

### Catalogue et cycle de vie

```bash
# Fiches OpenMetadata des 5 lignes (idempotent)
python openmetadata_catalog.py

# Règles ILM sur les 4 buckets
python minio_ilm_setup.py

# DAG #3 — archivage raw|staging → archive/ au-delà de 180 jours
docker exec airflow-scheduler airflow dags unpause dag_ilm_archive
```

### Gouvernance et contrôle d'accès

```bash
# 3 comptes de service, policies différenciées, SSE-S3
# (vérifie ensuite les 48 droits réels et échoue au moindre écart)
python minio_governance_setup.py

# Analyse du journal d'audit MinIO
python analyze_audit_logs.py --report JOUR7_AUDIT.md
```

| Compte de service | Périmètre |
|---|---|
| `data-analyst` | Lecture seule sur `curated` |
| `data-engineer` | Lecture/écriture sur `raw`, `staging`, `curated` |
| `admin` | Tous droits + administration MinIO |

Politique complète : [JOUR6_GOUVERNANCE.md](JOUR6_GOUVERNANCE.md)

---

## Structure du dépôt

```
brief_minIO/
├── data/                          ← CSV sources (non commités)
├── dags/
│   ├── dag_raw_ingestion.py       ← DAG #1 : ingestion vers raw/
│   ├── dag_staging_transform.py   ← DAG #2 : transformation vers staging/
│   └── dag_ilm_archive.py         ← DAG #3 : archivage ILM à 180 jours
├── logs/                          ← Logs Airflow (ignorés par git)
├── minio-data/                    ← Données MinIO persistées (ignorées par git)
├── audit-logs/                    ← Journal d'audit MinIO (ignoré par git)
├── architecture_datalake.drawio   ← Schéma v1
├── architecture_datalake_v2.drawio← Schéma v2 (version finale)
├── docker-compose.yml             ← Infrastructure data lake
├── docker-compose.openmetadata.yml← Overlay catalogue OpenMetadata
├── upload_to_minio.py             ← Script Jour 2 (upload + intégrité MD5)
├── openmetadata_catalog.py        ← Script Jour 5 (5 fiches + glossaire)
├── minio_ilm_setup.py             ← Script Jour 5 (règles ILM)
├── minio_governance_setup.py      ← Script Jours 6-7 (comptes, policies, SSE-S3)
├── audit_collector.py             ← Webhook receveur des logs d'audit MinIO
├── analyze_audit_logs.py          ← Analyse d'une session d'accès
├── requirements.txt
├── .env.example                   ← Template à copier en .env
├── JOUR1_ARCHITECTURE.md          ← Analyse des données + choix d'architecture
├── JOUR2_README.md                ← MinIO, buckets, policies, upload boto3
├── JOUR3_README.md                ← Airflow, DAGs, procédure complète
├── JOUR5_README.md                ← OpenMetadata, catalogue, ILM
├── JOUR5_CATALOG.md               ← Rapport catalogue (généré)
├── JOUR5_ILM_POLICY.md            ← Rapport ILM (généré)
├── JOUR6_README.md                ← Comptes, chiffrement, audit
├── JOUR6_GOUVERNANCE.md           ← Politique de gouvernance (livrable C21)
├── JOUR6_ACCESS_MATRIX.md         ← Matrice des droits observés (généré)
└── JOUR7_AUDIT.md                 ← Analyse du journal d'audit (généré)
```

---

## Livrables réalisés

| Compétence | Livrable | Détail |
|---|---|---|
| C18 | Architecture + schéma draw.io | Analyse des 5 lignes, choix Medallion, `architecture_datalake_v2.drawio` |
| C19 | MinIO, buckets, policies, upload MD5 | 4 buckets créés, policies différenciées par bucket, intégrité vérifiée |
| C19 | DAGs Airflow ingestion + staging | DAG #1 raw partitionné, LineA en chunks, DAG #2 Parquet snappy harmonisé |
| C20 | Catalogue OpenMetadata | 5 fiches complètes, 8 colonnes documentées chacune, glossaire, pipeline planifié |
| C20 | Politique ILM | Règles d'expiration sur les 4 buckets, DAG #3 d'archivage à 180 j |
| C21 | Comptes et policies différenciées | 3 comptes de service, 48 droits vérifiés empiriquement |
| C21 | Chiffrement et audit | SSE-S3 sur les 4 buckets, journal d'audit collecté et analysé |
| C21 | Politique de gouvernance | Matrice des droits, RACI, conditions d'octroi, signaux de détection |
