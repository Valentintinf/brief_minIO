# Jour 3 — Airflow : DAGs d'ingestion et de transformation

**Compétence visée :** C19 — Intégration et ingestion des données  
**Branche :** `feature/jour-2` (suite)

---

## Objectifs du jour

1. Déployer Airflow via Docker Compose (LocalExecutor + PostgreSQL).
2. **DAG #1** : ingérer les 5 CSV vers `raw/` avec partitionnement `year=/month=/line=/`.
3. **DAG #2** : harmoniser les colonnes et produire des Parquet dans `staging/`.
4. Traiter LineA (10 000 lignes) en chunks de 1 000 pour simuler un flux réel.

---

## Architecture des DAGs

```
DAG #1 — dag_raw_ingestion          DAG #2 — dag_staging_transform
─────────────────────────           ──────────────────────────────
ingest_lineA  (10 chunks)           transform_lineA
ingest_lineB  (1 fichier)           transform_lineB
ingest_lineC  (1 fichier)    →  →   transform_lineC
ingest_lineD  (1 fichier)           transform_lineD
ingest_lineE  (1 fichier)           transform_lineE

(tâches indépendantes, exécution parallèle dans chaque DAG)
```

---

## Prérequis

- Docker + Docker Compose v2 opérationnels (`docker compose version`)
- MinIO démarré et buckets créés (Jour 2)

---

## Étape 1 — Préparer les répertoires Airflow

```bash
# Créer les dossiers montés dans les conteneurs
mkdir -p dags logs

# Donner les droits d'écriture au user airflow du conteneur (UID 50000)
chmod 777 logs/
```

---

## Étape 2 — Démarrer tous les services

```bash
docker compose up -d
```

L'ordre de démarrage est géré automatiquement :
1. `postgres` → base de métadonnées Airflow
2. `minio` → stockage objet
3. `airflow-init` → migration DB + création utilisateur admin
4. `airflow-webserver` + `airflow-scheduler` → interface et orchestration

Vérifier que tout est sain (~2 min au premier démarrage, le temps d'installer boto3/pandas/pyarrow) :

```bash
docker compose ps
```

```
NAME                 STATUS
minio                running (healthy)
airflow-postgres     running (healthy)
airflow-init         exited (0)        ← normal, one-shot
airflow-webserver    running (healthy)
airflow-scheduler    running
```

---

## Étape 3 — Accéder à Airflow

Console web : **http://localhost:8080**  
Identifiants : `admin` / `admin`

> La console MinIO reste accessible sur **http://localhost:9001** (`minioadmin` / `minioadmin`).

---

## Étape 4 — Exécuter les DAGs (dans l'ordre)

### DAG #1 — Ingestion raw

Dans l'interface Airflow :
1. Aller dans **DAGs** → `dag_raw_ingestion`
2. Activer le toggle (Enable)
3. Cliquer **Trigger DAG** ▶

Ou en ligne de commande :

```bash
docker exec airflow-scheduler airflow dags trigger dag_raw_ingestion
```

**Résultat attendu dans MinIO `raw/` :**

```
production_lines/
├── lineA/year=2025/month=05/
│   ├── LineA_Stable_10K_chunk_0001.csv   (1 000 lignes)
│   ├── LineA_Stable_10K_chunk_0002.csv
│   …
│   └── LineA_Stable_10K_chunk_0010.csv   (10 chunks)
├── lineB/year=2025/month=04/LineB_Flux.csv
├── lineC/year=2025/month=03/LineC_Turbulent.csv
├── lineD/year=2025/month=02/LineD_SpikeControl.csv
└── lineE/year=2025/month=01/LineE_SmoothRun.csv
```

### DAG #2 — Transformation staging

Une fois DAG #1 terminé avec succès :

```bash
docker exec airflow-scheduler airflow dags trigger dag_staging_transform
```

**Résultat attendu dans MinIO `staging/` :**

```
production_lines/
├── lineA/year=2025/month=05/
│   ├── LineA_Stable_10K_chunk_0001.parquet
│   …
│   └── LineA_Stable_10K_chunk_0010.parquet
├── lineB/year=2025/month=04/LineB_Flux.parquet
├── lineC/year=2025/month=03/LineC_Turbulent.parquet
├── lineD/year=2025/month=02/LineD_SpikeControl.parquet
└── lineE/year=2025/month=01/LineE_SmoothRun.parquet
```

---

## Transformations appliquées par DAG #2

| Opération | Détail |
|---|---|
| Normalisation casse | `df.columns.str.lower()` → `Temperature` → `temperature`, `Pressure` → `pressure`, `Elapsed_time` → `elapsed_time` |
| Parsing timestamp | `pd.to_datetime()` → `datetime64[ns]` |
| `elapsed_time` nullable | Colonne absente sur C/D/E → ajoutée avec `pd.NA` (`Float64`) |
| Validation `label` | Vérifié ∈ {0, 1}, erreur levée sinon |
| Ajout `line_id` | Identifiant de la ligne (`lineA`, `lineB`, …) |
| Ajout `ingestion_ts` | Horodatage UTC de l'ingestion |
| Ajout `source_file_hash` | MD5 du CSV source (traçabilité) |
| Format de sortie | Parquet, compression **snappy** |

---

## Suivi des exécutions

```bash
# Logs du scheduler (erreurs DAG)
docker logs airflow-scheduler --tail=50

# Logs d'une tâche spécifique
docker exec airflow-scheduler \
  airflow tasks logs dag_raw_ingestion ingest_lineA <run_id>

# Statut des dernières runs
docker exec airflow-scheduler \
  airflow dags list-runs -d dag_raw_ingestion
```

---

## Structure des fichiers

```
brief_minIO/
├── dags/
│   ├── dag_raw_ingestion.py      ← DAG #1 : CSV → raw/ (partitionné)
│   └── dag_staging_transform.py  ← DAG #2 : raw/ → staging/ (Parquet)
├── logs/                         ← logs Airflow (ignoré par git)
├── docker-compose.yml            ← MinIO + Postgres + Airflow
└── requirements.txt              ← boto3, pandas, pyarrow
```

---

## Arrêter les services

```bash
docker compose down          # arrêt, données conservées
docker compose down -v       # arrêt + suppression des volumes (reset complet)
```
