# Catalogue OpenMetadata — Jour 5 (C20)

Généré par `openmetadata_catalog.py`, à partir des fiches relues sur le
serveur OpenMetadata (1.13.5).

Console : <http://localhost:8585> — `admin@open-metadata.org` / `admin`

## Service et pipeline

| Élément | Valeur |
|---|---|
| Service | `minio_datalake` (type Datalake) |
| Couche cataloguée | bucket `staging` |
| Pipeline d'ingestion | `minio_datalake_metadata` |
| Propriétaire | Responsable maintenance / Maintenance industrielle |
| Source | Synthetic Data from Industrial Sensor Monitoring — Polytechnic Institute of Porto / INESC TEC — Zenodo, avril 2025 |
| Fréquence de collecte | 1 mesure/minute (1440 points/jour/ligne) |

## Les 5 fiches

| Ligne | Fiche | Enregistrements | Anomalies | Colonnes documentées | Propriétaire |
|---|---|---:|---:|---:|---|
| lineA | Ligne A — régime stable | 10 000 | 18 (0.18 %) | 8/8 | Responsable maintenance |
| lineB | Ligne B — flux moyen | 5 000 | 50 (1.0 %) | 8/8 | Responsable maintenance |
| lineC | Ligne C — régime turbulent | 5 000 | 200 (4.0 %) | 8/8 | Responsable maintenance |
| lineD | Ligne D — pics contrôlés | 5 000 | 15 (0.3 %) | 8/8 | Responsable maintenance |
| lineE | Ligne E — fonctionnement lissé | 5 000 | 25 (0.5 %) | 8/8 | Responsable maintenance |

## Schéma harmonisé documenté

Les cinq fiches partagent le même schéma, ce qui valide l'harmonisation du
Jour 3-4 : c'est l'ingestion OpenMetadata qui a lu les Parquet et découvert
ces colonnes, elles n'ont pas été déclarées à la main.

| Colonne | Type | Unité | Rôle |
|---|---|---|---|
| `timestamp` | DATETIME | — | horodatage de la mesure, pas de 60 s |
| `line_id` | STRING | — | identifiant de ligne, ajouté à la transformation |
| `temperature` | FLOAT | °C | température capteur |
| `pressure` | FLOAT | hPa | pression capteur |
| `elapsed_time` | FLOAT | s | durée de cycle, nulle sur C/D/E |
| `label` | INT | — | **0 = nominal, 1 = anomalie** (variable cible) |
| `ingestion_ts` | DATETIME | — | date de production de l'enregistrement |
| `source_file_hash` | STRING | — | MD5 du fichier `raw/` d'origine |

## Glossaire

Glossaire « Maintenance prédictive » : `État nominal` (label = 0),
`Anomalie` (label = 1), `Durée de cycle` (`elapsed_time`).

## Rejouer le catalogage

```bash
docker-compose -f docker-compose.yml -f docker-compose.openmetadata.yml up -d
python openmetadata_catalog.py
```

Le script est idempotent : une relance met à jour les fiches sans créer de
doublons.
