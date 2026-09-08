# Jour 5 — Catalogue OpenMetadata et gouvernance du cycle de vie (C20)

## Objectifs du jour

| Attendu du brief | Réalisation |
|---|---|
| Installer OpenMetadata via Docker et le connecter à MinIO | `docker-compose.openmetadata.yml` + service Datalake `minio_datalake` |
| Créer les fiches métadonnées des 5 lignes (description, propriétaire, source, fréquence) | `openmetadata_catalog.py` → `JOUR5_CATALOG.md` |
| Documenter les colonnes clés (unités, plages normales, sens de `label`) | 8 colonnes documentées sur chacune des 5 fiches |
| Configurer l'ILM MinIO : archivage à 180 j, suppression à 2 ans | `minio_ilm_setup.py` + `dags/dag_ilm_archive.py` → `JOUR5_ILM_POLICY.md` |

---

## Étape 1 — Démarrer la stack complète

OpenMetadata est livré comme un **overlay** de `docker-compose.yml`, pas comme un
fichier autonome : ses conteneurs rejoignent ainsi le réseau du projet et
atteignent MinIO par son nom de service, `http://minio:9000`.

```bash
docker-compose -f docker-compose.yml -f docker-compose.openmetadata.yml up -d
```

Cela ajoute quatre conteneurs à la stack du Jour 3 :

| Conteneur | Rôle | Port hôte |
|---|---|---|
| `openmetadata_mysql` | base de métadonnées d'OpenMetadata | 3307 |
| `openmetadata_elasticsearch` | index de recherche du catalogue | 9200 |
| `openmetadata_server` | API REST et interface web | 8585 |
| `openmetadata_ingestion` | Airflow dédié aux workflows de catalogage | 8081 |

> **Ports** : l'Airflow d'OpenMetadata écoute sur **8081**, car **8080** est déjà
> occupé par l'Airflow du data lake. Ce sont deux Airflow distincts, avec deux
> rôles distincts — ne pas les confondre.

### Ressources requises

Comptez environ **6 Go de RAM** et 5 Go d'images. Le premier démarrage prend
plusieurs minutes : le conteneur `execute_migrate_all` doit migrer le schéma
MySQL avant que le serveur ne démarre.

Attendre que le serveur réponde :

```bash
until curl -sf http://localhost:8585/api/v1/system/version; do sleep 10; done
```

### Accès

| Service | URL | Identifiants |
|---|---|---|
| OpenMetadata | <http://localhost:8585> | `admin@open-metadata.org` / `admin` |
| Airflow OpenMetadata | <http://localhost:8081> | `admin` / `admin` |
| Airflow data lake | <http://localhost:8080> | `admin` / `admin` |
| Console MinIO | <http://localhost:9001> | `minioadmin` / `minioadmin` |

---

## Étape 2 — Peupler le catalogue

Prérequis : `staging/` doit être peuplé, donc les DAGs des Jours 3-4 doivent
avoir tourné (`dag_raw_ingestion` puis `dag_staging_transform`).

```bash
python openmetadata_catalog.py
```

Le script est **idempotent** : on peut le relancer sans créer de doublons.

Il enchaîne :

1. **Authentification** — connexion admin, puis récupération du JWT de
   l'`ingestion-bot`.
2. **Service Datalake `minio_datalake`** — connexion S3 vers `http://minio:9000`,
   cataloguant le bucket `staging`.
3. **Pipeline d'ingestion planifié** — déployé sur l'Airflow d'OpenMetadata,
   planifié à 03 h 00 chaque jour. Il lit les fichiers Parquet et en déduit le
   schéma. Un filtre `.*_chunk_.*` exclut les 10 chunks de LineA : le catalogue
   documente une **ligne de production**, pas un fichier.
4. **Propriétaire** — équipe « Maintenance industrielle » et utilisateur
   « Responsable maintenance ».
5. **Glossaire « Maintenance prédictive »** — les termes *État nominal*,
   *Anomalie* et *Durée de cycle*, qui fixent le vocabulaire métier.
6. **Enrichissement des 5 fiches** — description, propriétaire, lien source
   Zenodo, période de rétention, et documentation des 8 colonnes.
7. **Rapport** — `JOUR5_CATALOG.md`, écrit à partir des fiches **relues** sur le
   serveur, et non de ce que le script croit avoir envoyé.

Sortie attendue :

```
✓ Connecté à OpenMetadata 1.13.5 (http://localhost:8585)
✓ Service Datalake : minio_datalake → http://minio:9000
✓ Pipeline déployé : minio_datalake_metadata (planification « 0 3 * * * »)
  … ingestion déclenchée, attente du résultat
✓ Ingestion « success » : 7 nouvelle(s), 0 mise(s) à jour, 10 filtrée(s) (chunks LineA), 0 erreur(s)
✓ Équipe : Maintenance industrielle
✓ Propriétaire : Responsable maintenance (responsable.maintenance@datalake.local)
✓ Glossaire : Maintenance prédictive
✓ Fiche lineA : propriétaire, source, rétention, 8/8 colonnes documentées
...
✓ Jour 5 — 5 fiches catalographiées dans OpenMetadata.
```

### Options

```bash
# Enrichir les fiches sans relancer l'ingestion
python openmetadata_catalog.py --skip-ingestion

# Changer la planification du rafraîchissement du catalogue
python openmetadata_catalog.py --schedule "0 */6 * * *"
```

### Vérifier dans l'interface

1. <http://localhost:8585> → **Explore → Tables** : 5 tables, une par ligne.
2. Ouvrir *Ligne C — régime turbulent* : description métier, propriétaire,
   lien Zenodo, rétention `P730D`.
3. Onglet **Schema** : les 8 colonnes, chacune avec unité et plage nominale.
4. **Settings → Services → Databases → minio_datalake → Ingestions** : le
   pipeline et l'historique de ses exécutions.
5. **Govern → Glossaries → Maintenance prédictive** : les 3 termes.

---

## Étape 3 — Configurer le cycle de vie (ILM)

```bash
python minio_ilm_setup.py
```

Politique appliquée, comptée depuis la création de l'objet :

| Échéance | Événement | Mécanisme |
|---|---|---|
| J+0 | dépôt dans `raw/` puis `staging/` | DAGs des Jours 3-4 |
| J+180 | déplacement vers `archive/` | DAG `dag_ilm_archive` |
| J+730 | suppression définitive | règle ILM MinIO |

Activer le DAG d'archivage, qui balaie les buckets une fois par jour :

```bash
docker exec airflow-scheduler airflow dags unpause dag_ilm_archive
```

### Pourquoi deux mécanismes et non une seule règle ILM

L'API S3 de cycle de vie n'offre que deux actions : **expirer** un objet, ou le
**transitionner** vers une classe de stockage. Elle ne sait pas le déplacer d'un
bucket vers un autre.

MinIO propose bien une transition vers un *remote tier*, mais celui-ci doit être
un déploiement MinIO ou S3 **distinct**. Le faire pointer vers le bucket
`archive` du même serveur n'est pas supporté : la commande
`mc ilm tier add … --endpoint http://localhost:9000 --bucket archive` reste
bloquée sans jamais aboutir.

L'archivage à 180 jours est donc réalisé par `dag_ilm_archive`, qui copie puis
supprime — dans cet ordre, pour qu'un échec de copie laisse la source intacte et
la tâche rejouable. Les objets archivés conservent leur chemin d'origine,
préfixé du bucket de provenance :

```
raw/production_lines/lineA/year=2025/month=05/x.csv
  → archive/raw/production_lines/lineA/year=2025/month=05/x.csv
```

et reçoivent trois métadonnées de traçabilité : `archived_from`, `archived_at`,
`original_last_modified`.

L'expiration à 730 jours reste posée sur `raw`, `staging` et `curated` comme
**filet de sécurité** : un objet que le DAG n'aurait jamais archivé (Airflow en
panne, DAG en pause) est malgré tout purgé à l'échéance. Sur `archive`,
l'expiration est réglée à **550 jours**, puisque les objets y arrivent déjà âgés
de 180 jours : 180 + 550 = 730.

### Vérifier les règles

```bash
python minio_ilm_setup.py --check

docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec minio mc ilm rule ls local/raw
```

Dans la console MinIO : **Buckets → raw → Lifecycle**.

---

## Points d'attention rencontrés

Trois comportements non documentés ont conditionné l'implémentation.

### 1. Seul l'`ingestion-bot` reçoit les secrets en clair

OpenMetadata renvoie les secrets de connexion **masqués** à tout porteur de token
autre que l'`ingestion-bot` — le token `admin` compris. Une ingestion lancée à la
main depuis le conteneur échoue donc côté MinIO :

```bash
docker exec openmetadata_ingestion metadata ingest -c /tmp/workflow.yaml
# ERROR  ListBuckets-An error occurred (SignatureDoesNotMatch) …
```

C'est la raison pour laquelle `openmetadata_catalog.py` ne passe pas par la CLI
mais **déploie un pipeline** via l'API : le serveur génère lui-même le DAG Airflow
et y injecte le JWT du bot. Aucun secret ne transite par le script.

Pour un diagnostic manuel, le JWT du bot se récupère ainsi :

```
GET /api/v1/bots/name/ingestion-bot        → botUser.id
GET /api/v1/users/auth-mechanism/{id}      → config.JWTToken
```

### 2. La description des tables est assainie, celle des colonnes non

OpenMetadata filtre la description **de table** contre les injections HTML :
apostrophes droites, guillemets, esperluettes, accents graves et signes égal sont
convertis en entités (`&#39;`, `&#96;`, `&#61;`), et tout ce qui ressemble à une
balise est **supprimé**. Les descriptions **de colonnes** échappent à ce filtre.

Les descriptions de **glossaire** subissent le même filtre que celles de table.

Fiches de table et termes de glossaire n'emploient donc qu'apostrophes
typographiques et gras, sans backtick ni signe égal ; les colonnes gardent du
Markdown complet. Le script relit chaque description après écriture et signale
toute réécriture par le serveur, plutôt que de supposer que l'envoi a été fidèle.

### 3. MinIO n'accepte pas `AbortIncompleteMultipartUpload` en ILM

MinIO rejette (`InvalidArgument`) une règle qui ne porterait que cette action, et
l'**ignore silencieusement** lorsqu'elle accompagne une expiration — la requête
réussit, mais l'action n'apparaît pas dans la configuration relue. Chez MinIO, la
purge des uploads interrompus est un réglage serveur :

```bash
docker exec minio mc admin config get local api | tr ' ' '\n' | grep stale
# stale_uploads_cleanup_interval=6h
# stale_uploads_expiry=24h
```

---

## Fichiers produits

```
docker-compose.openmetadata.yml   ← overlay OpenMetadata 1.13.5
openmetadata_catalog.py           ← service, pipeline, propriétaire, glossaire, 5 fiches
minio_ilm_setup.py                ← règles ILM sur les 4 buckets
dags/dag_ilm_archive.py           ← archivage raw|staging → archive à 180 j
JOUR5_CATALOG.md                  ← rapport catalogue (généré)
JOUR5_ILM_POLICY.md               ← rapport ILM (généré)
```

---

## Arrêter la stack

```bash
# Arrêt en conservant les données
docker-compose -f docker-compose.yml -f docker-compose.openmetadata.yml stop

# Suppression des conteneurs (les volumes MySQL et MinIO subsistent)
docker-compose -f docker-compose.yml -f docker-compose.openmetadata.yml down
```
