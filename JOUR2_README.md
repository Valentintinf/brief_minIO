# Jour 2 — MinIO : déploiement, buckets, policies et upload

**Compétence visée :** C19 — Intégration et ingestion des données  
**Branche :** `feature/jour-2`

---

## Objectifs du jour

1. Déployer MinIO via Docker Compose et accéder à la console web.
2. Créer les 4 buckets (`raw`, `staging`, `curated`, `archive`).
3. Configurer les policies d'accès initiales différenciées par bucket.
4. Uploader les 5 CSV via `boto3` dans `raw/production_lines/lineX/`.
5. Vérifier l'intégrité des fichiers déposés par hash MD5.

---

## Prérequis

| Outil | Version minimale | Vérification |
|---|---|---|
| Docker | 24.x | `docker --version` |
| Docker Compose v2 | intégré à Docker | `docker compose version` |
| Python | 3.11+ | `python --version` |
| pip | 23+ | `pip --version` |

---

## Structure du répertoire

```
brief_minIO/
├── docker-compose.yml          ← définition du service MinIO
├── upload_to_minio.py          ← script d'upload + vérification MD5
├── requirements.txt            ← dépendances Python
├── minio-data/                 ← volume Docker (données persistées)
├── data/
│   ├── LineA_Stable_10K.csv
│   ├── LineB_Flux.csv
│   ├── LineC_Turbulent.csv
│   ├── LineD_SpikeControl.csv
│   └── LineE_SmoothRun.csv
└── JOUR2_UPLOAD_MANIFEST.md    ← généré après l'upload
```

---

## Étape 1 — Démarrer MinIO

```bash
docker compose up -d
```

Vérifier que le conteneur est sain :

```bash
docker compose ps
# minio   running (healthy)   0.0.0.0:9000->9000/tcp, 0.0.0.0:9001->9001/tcp
```

La console web est accessible sur **http://localhost:9001**  
Identifiants : `minioadmin` / `minioadmin`

---

## Étape 2 — Installer les dépendances Python

```bash
python -m venv .venv
source .venv/bin/activate      # Windows : .venv\Scripts\activate
pip install -r requirements.txt
```

---

## Étape 3 — Créer les buckets, appliquer les policies et uploader

```bash
python upload_to_minio.py
```

Le script fait dans l'ordre :

1. Se connecte à MinIO sur `http://localhost:9000`.
2. Crée les 4 buckets si absents : `raw`, `staging`, `curated`, `archive`.
3. Applique une policy d'accès initiale sur chaque bucket (voir tableau ci-dessous).
4. Uploade les 5 CSV dans `raw/production_lines/<line>/`.
5. Vérifie le hash MD5 local vs. ETag MinIO pour chaque fichier.
6. Génère `JOUR2_UPLOAD_MANIFEST.md` avec le résultat ligne par ligne.

### Policies initiales par bucket

| Bucket | Lecture | Écriture | Suppression | Remarque |
|---|:---:|:---:|:---:|---|
| `raw` | ✓ | ✓ | ✓ | ingestion brute |
| `staging` | ✓ | ✓ | ✓ | transformation Airflow |
| `curated` | ✓ | ✓ | — | exposition analystes |
| `archive` | ✓ | — | — | géré par ILM MinIO |

> Les comptes de service (`data-analyst`, `data-engineer`, `admin`) avec policies strictement différenciées seront créés au Jour 6.

### Sortie attendue

```
✓ Bucket déjà présent : raw
✓ Policy appliquée (s3:GetObject, s3:PutObject, ...) : raw
...
✓ Upload OK : LineA_Stable_10K.csv → raw/production_lines/lineA/LineA_Stable_10K.csv
✓ Upload OK : LineB_Flux.csv       → raw/production_lines/lineB/LineB_Flux.csv
...
✓ Manifest écrit : JOUR2_UPLOAD_MANIFEST.md
✓ Jour 2 terminé : buckets, policies, uploads et MD5 validés.
```

---

## Étape 4 — Vérifier via la console web

1. Ouvrir **http://localhost:9001** → `minioadmin` / `minioadmin`.
2. Naviguer dans **Buckets → raw → Browse**.
3. Vérifier la présence de `production_lines/lineA/`, `lineB/`, etc.
4. Cliquer sur un fichier → onglet **Tags** pour voir les métadonnées (`line`, `source`, `local_md5`).

---

## Options du script

```bash
# Endpoint personnalisé
python upload_to_minio.py --endpoint-url http://mon-serveur:9000

# Variables d'environnement (alternative aux arguments)
MINIO_ENDPOINT=http://localhost:9000 \
MINIO_ROOT_USER=minioadmin \
MINIO_ROOT_PASSWORD=minioadmin \
python upload_to_minio.py

# Sans appliquer les policies (si déjà faites)
python upload_to_minio.py --skip-policies

# Répertoire de données alternatif
python upload_to_minio.py --data-dir /chemin/vers/csv
```

---

## Structure des objets dans MinIO après upload

```
raw/
└── production_lines/
    ├── lineA/
    │   └── LineA_Stable_10K.csv   (775 680 o)
    ├── lineB/
    │   └── LineB_Flux.csv          (390 706 o)
    ├── lineC/
    │   └── LineC_Turbulent.csv     (295 024 o)
    ├── lineD/
    │   └── LineD_SpikeControl.csv  (295 098 o)
    └── lineE/
        └── LineE_SmoothRun.csv     (295 081 o)
```

> Le partitionnement `year=/month=/line=/` sera ajouté par les DAGs Airflow au Jour 3.

---

## Vérification d'intégrité MD5

Le script calcule le MD5 local **avant** l'upload et le compare à l'ETag retourné par MinIO après dépôt. Pour les fichiers non multi-part (< 5 Go), l'ETag S3 est le MD5 exact du fichier. Tout écart déclenche un statut `KO` et un code de retour non nul.

```python
# Extrait de upload_to_minio.py
local_md5  = compute_md5(path)          # hashlib.md5, lecture par chunks de 1 Mo
remote_md5 = response["ETag"].strip('"') # ETag MinIO = MD5 pour upload simple
assert local_md5 == remote_md5
```

Le manifest complet est dans `JOUR2_UPLOAD_MANIFEST.md`.

---

## Arrêter MinIO

```bash
docker compose down        # arrêt sans suppression des données
docker compose down -v     # arrêt + suppression du volume (reset complet)
```

---

## Liens utiles

- Documentation MinIO : https://min.io/docs/minio/container/index.html
- boto3 S3 : https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
- Prochain livrable : Jour 3 — DAGs Airflow (ingestion + transformation)
