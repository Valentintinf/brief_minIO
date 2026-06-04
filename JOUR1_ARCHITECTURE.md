# JOUR 1 — Architecture du Data Lake industriel

**Date :** 4 juin 2026  
**Compétence visée :** C18 — Analyse du besoin, conception d'architecture et justification des choix techniques  
**Livrable associé :** `architecture_datalake.drawio`

## 1. Objectif du jour

La DSI souhaite centraliser des données capteurs aujourd'hui dispersées et hétérogènes afin de préparer un futur cas d'usage de maintenance prédictive.

Le Jour 1 couvre donc :

1. l'exploration des 5 fichiers CSV fournis ;
2. l'identification des écarts de schéma ;
3. la conception d'une architecture en couches `raw / staging / curated / archive` ;
4. la production d'un schéma technique annoté au format draw.io ;
5. la justification des choix retenus.

## 2. Analyse des données sources

Les mesures ci-dessous ont été recalculées depuis les fichiers présents dans `data/`.

### 2.1 Volumétrie

| Ligne | Fichier | Lignes | Colonnes | Taille | Période couverte |
|---|---:|---:|---:|---:|---|
| A | `LineA_Stable_10K.csv` | 10 000 | 5 | 775 680 o | 2025-05-01 00:00 → 2025-05-07 22:39 |
| B | `LineB_Flux.csv` | 5 000 | 5 | 390 706 o | 2025-04-01 00:00 → 2025-04-04 11:19 |
| C | `LineC_Turbulent.csv` | 5 000 | 4 | 295 024 o | 2025-03-01 00:00 → 2025-03-04 11:19 |
| D | `LineD_SpikeControl.csv` | 5 000 | 4 | 295 098 o | 2025-02-01 00:00 → 2025-02-04 11:19 |
| E | `LineE_SmoothRun.csv` | 5 000 | 4 | 295 081 o | 2025-01-01 00:00 → 2025-01-04 11:19 |
| **Total** | 5 CSV | **30 000** | — | **2 051 589 o** | Janvier → mai 2025 |

**Lecture métier :** la volumétrie initiale est faible, mais l'architecture doit être pensée pour de la collecte continue. Le choix d'un stockage objet compatible S3 permet de conserver un historique long, nécessaire à l'entraînement futur de modèles de maintenance prédictive.

### 2.2 Hétérogénéité des schémas

| Ligne | Colonnes observées | Particularités |
|---|---|---|
| A | `timestamp`, `Temperature`, `pressure`, `elapsed_time`, `label` | `Temperature` avec majuscule |
| B | `timestamp`, `temperature`, `pressure`, `Elapsed_time`, `label` | `Elapsed_time` avec majuscule |
| C | `timestamp`, `Temperature`, `pressure`, `label` | pas de `elapsed_time` |
| D | `timestamp`, `temperature`, `Pressure`, `label` | `Pressure` avec majuscule, pas de `elapsed_time` |
| E | `timestamp`, `Temperature`, `pressure`, `label` | pas de `elapsed_time` |

**Problème central du brief :** les fichiers ne peuvent pas être consommés directement comme une table unique. Les transformations doivent harmoniser la casse, ajouter une colonne `line_id`, gérer `elapsed_time` comme champ nullable, puis produire un schéma cible commun.

### 2.3 Qualité et anomalies

| Ligne | Température min/max | Pression min/max | `elapsed_time` min/max | Labels anomalie |
|---|---:|---:|---:|---:|
| A | 179.4901 → 185.1866 | 156.7976 → 160.6271 | 34.2690 → 37.2324 | 18 / 10 000 = 0,18 % |
| B | 186.7587 → 202.7194 | 108.9465 → 123.5291 | 18.6498 → 22.9783 | 50 / 5 000 = 1,00 % |
| C | 194.6519 → 216.2686 | 88.3177 → 104.7289 | absent | 200 / 5 000 = 4,00 % |
| D | 194.8140 → 213.1090 | 90.4277 → 104.2349 | absent | 15 / 5 000 = 0,30 % |
| E | 198.3794 → 209.5966 | 96.7530 → 100.8823 | absent | 25 / 5 000 = 0,50 % |

Contrôles observés :

- aucun timestamp invalide ;
- aucun doublon complet ;
- aucun champ numérique vide sur les colonnes présentes ;
- `label` est cohérent avec le domaine attendu : `0` nominal, `1` anomalie.

## 3. Architecture cible

L'architecture suit un modèle en couches de type médaillon, adapté à MinIO.

```text
Sources CSV Zenodo
      ↓
Airflow / Python boto3
      ↓
raw      → données brutes inchangées
      ↓
staging  → données harmonisées et contrôlées
      ↓
curated  → données prêtes analyse, BI et ML
      ↓
archive  → données gérées par règles de cycle de vie
```

### 3.1 Couche `raw`

**Rôle :** conserver les fichiers originaux sans modification.

Structure proposée :

```text
raw/
  production_lines/
    lineA/year=2025/month=05/LineA_Stable_10K.csv
    lineB/year=2025/month=04/LineB_Flux.csv
    lineC/year=2025/month=03/LineC_Turbulent.csv
    lineD/year=2025/month=02/LineD_SpikeControl.csv
    lineE/year=2025/month=01/LineE_SmoothRun.csv
```

Justification :

- la donnée brute reste rejouable en cas d'erreur de transformation ;
- le partitionnement par ligne puis `year=/month=` limite les scans inutiles ;
- les hash MD5 permettent de vérifier l'intégrité après upload.

### 3.2 Couche `staging`

**Rôle :** produire un schéma harmonisé, typé et exploitable par les traitements.

Schéma cible :

| Colonne | Type cible | Règle |
|---|---|---|
| `timestamp` | datetime | parsing strict, rejet ou quarantaine si invalide |
| `line_id` | string | valeur `A`, `B`, `C`, `D` ou `E` |
| `temperature` | float | mapping depuis `Temperature` ou `temperature` |
| `pressure` | float | mapping depuis `Pressure` ou `pressure` |
| `elapsed_time` | float nullable | mapping si présent, `null` sinon |
| `label` | int | valeur autorisée : `0` ou `1` |
| `ingestion_ts` | datetime | date technique d'ingestion |
| `source_file_hash` | string | hash MD5 du fichier source |

**Décision importante :** `elapsed_time` n'est pas inventé pour les lignes C, D et E. Il est ajouté comme colonne nullable afin de préserver la vérité source et d'éviter une imputation métier non validée.

### 3.3 Couche `curated`

**Rôle :** exposer des données métier prêtes à l'analyse.

Jeux de données attendus :

- séries temporelles harmonisées par ligne ;
- agrégats horaires ou journaliers ;
- taux d'anomalie par ligne et par période ;
- features futures pour maintenance prédictive : moyennes glissantes, écarts-types, variations de pression/température.

### 3.4 Couche `archive`

**Rôle :** appliquer les règles de rétention.

Règles prévues par la consigne :

- archivage automatique après 180 jours ;
- suppression après 2 ans ;
- isolation dans un bucket dédié pour simplifier les droits et l'audit.

## 4. Choix technologiques

| Composant | Choix | Justification |
|---|---|---|
| Stockage objet | MinIO Community | compatible S3, simple en Docker, adapté aux buckets par couche |
| Ingestion | Python / boto3 | API S3 standard, vérification MD5, automatisable |
| Orchestration | Airflow | DAGs reproductibles, dépendances `raw → staging → curated`, monitoring |
| Catalogue | OpenMetadata | fiches datasets, propriétaires, colonnes, lignage |
| Format staging/curated | Parquet | colonnes typées, compression, lecture analytique efficace |
| Gouvernance | Policies MinIO + SSE-S3 + audit logs | contrôle d'accès, chiffrement, traçabilité |

## 5. Sécurité et gouvernance prévues

La séparation en buckets facilite la gouvernance :

| Rôle | Accès prévu |
|---|---|
| `data-analyst` | lecture seule sur `curated/` |
| `data-engineer` | lecture/écriture sur `raw/`, `staging/`, `curated/` |
| `admin` | droits complets |

Mesures associées :

- chiffrement SSE-S3 sur les buckets de production ;
- logs d'audit MinIO activés ;
- documentation des propriétaires dans OpenMetadata ;
- règles ILM documentées et testables.

## 6. Diagramme draw.io

Le diagramme technique est disponible dans :

```text
architecture_datalake_v2.drawio   ← version finale
architecture_datalake.drawio      ← version initiale (conservée)
```

Il contient :

- les 5 sources CSV avec volumétrie et anomalies ;
- les deux flux d'ingestion/transformation ;
- les 4 couches MinIO ;
- les choix de partitionnement ;
- OpenMetadata, sécurité, rôles et consommateurs ;
- les annotations justifiant les choix d'architecture.
## 7. Conclusion Jour 1

Le besoin de maintenance prédictive impose de conserver des données historiques fiables, documentées et homogènes. L'architecture proposée répond à ce besoin en séparant clairement :

- la conservation brute (`raw`) ;
- la normalisation technique (`staging`) ;
- l'exploitation analytique (`curated`) ;
- la rétention long terme (`archive`).

Cette conception prépare directement les travaux des jours suivants : déploiement MinIO, ingestion automatisée, transformations Airflow, catalogue OpenMetadata et gouvernance des accès.
