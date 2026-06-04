# Brief — Conception et déploiement d'un Data Lake industriel

## Contexte

En tant que **data engineer**, concevoir l'architecture d'un data lake, déployer l'infrastructure de stockage objet, mettre en place les pipelines d'ingestion, cataloguer les données et implémenter les règles de gouvernance et de contrôle d'accès.

Leurs données sont aujourd'hui stockées en vrac, sans structure ni gouvernance. La DSI vous confie la mission de concevoir et déployer un data lake moderne pour centraliser, documenter et sécuriser l'ensemble de ces flux, en vue d'un futur projet de maintenance prédictive.

Ce client collecte en continu des données issues de plusieurs lignes de production instrumentées. Ces données sont actuellement dispersées et hétérogènes, ce qui complique leur exploitation et limite leur valeur métier.

La mise en place d'un datalake permet de centraliser l'ensemble des données dans une plateforme unique, capable de stocker aussi bien les données brutes que les données transformées. Une organisation structurée des données facilite leur découverte, leur traçabilité, leur sécurisation et leur réutilisation par les différentes parties prenantes.

Dans une perspective de maintenance prédictive, disposer de données fiables, documentées et harmonisées est indispensable. Les modèles d'intelligence artificielle utilisés pour détecter les anomalies ou anticiper les pannes nécessitent un historique important de mesures cohérentes provenant de plusieurs équipements.

Le datalake constitue ainsi la fondation technique qui permettra, à terme, d'entraîner des modèles capables d'identifier les signes précurseurs de défaillance et de réduire les arrêts non planifiés des lignes de production.

## Ressources sur la maintenance prédictive

- <https://www.ibm.com/fr-fr/think/topics/predictive-maintenance>
- <https://aws.amazon.com/fr/what-is/predictive-maintenance/>
- <https://www.sap.com/france/products/scm/apm/what-is-predictive-maintenance.html#:~:text=La%20maintenance%20pr%C3%A9dictive%20consiste%20%C3%A0,et%20d'%C3%A9viter%20une%20panne.>
- <https://www.synergy.fr/wp-content/uploads/2022/02/datailku.pdf>
- <https://www.praxedo.fr/notre-blog-specialise/comment-passer-a-la-maintenance-predictive/>

## Source de données

Source de données pour ce brief : **Synthetic Data from Industrial Sensor Monitoring** — Polytechnic Institute of Porto / INESC TEC — Zenodo, avril 2025.

Lien : <https://zenodo.org/records/15277168>

### Fichiers attendus

5 fichiers CSV représentant 5 lignes de production aux comportements distincts :

- **Line A** stable — 10 000 enregistrements ;
- **Line B** à flux moyen ;
- **Line C** turbulente ;
- **Line D** avec pics ;
- **Line E** variable.

### Point d'attention

Les schémas diffèrent légèrement d'une ligne à l'autre :

- casse des colonnes ;
- présence ou absence du champ `elapsed_time`.

Cette hétérogénéité est volontaire et constitue une difficulté centrale du brief.

## Modalités pédagogiques

| Élément | Détail |
|---|---|
| Durée | 2 semaines, 8 jours |
| Technologies | MinIO Community (Docker), OpenMetadata, Airflow, Python / boto3, Docker Compose, Git |

## Planning

### Semaine 1 — Architecture & Intégration

#### Jour 1 (C18)

- Télécharger les 5 CSV depuis Zenodo et les explorer : volumétrie, colonnes, types, anomalies.
- Identifier les différences de schéma entre lignes : `Temperature` vs `temperature`, présence/absence de `elapsed_time`.
- Modéliser l'architecture en couches `Raw / Staging / Curated / Archive`.
- Produire un schéma d'architecture technique `draw.io` avec justification des choix.

#### Jour 2 (C19)

- Installer MinIO via Docker Compose et accéder à la console.
- Créer les buckets `raw`, `staging`, `curated`, `archive`.
- Configurer les policies d'accès initiales : lecture/écriture selon bucket.
- Uploader les 5 CSV via `boto3` dans `raw/production_lines/lineX/`.
- Vérifier l'intégrité des fichiers déposés : hash MD5.

#### Jours 3-4 (C19)

- Créer un DAG Airflow qui ingère chaque CSV vers `raw/` avec partitionnement `year=/month=/line=/`.
- Créer un second DAG de transformation :
  - harmoniser les noms de colonnes ;
  - normaliser les formats de timestamp ;
  - déposer en `staging/`.
- Gérer la ligne LineA (10 000 entr.) en batch par chunks pour simuler un flux réel.
- Documenter la procédure d'intégration dans un README.

### Semaine 2 — Catalogue & Gouvernance

#### Jour 5 (C20)

- Installer OpenMetadata via Docker et connecter au MinIO.
- Créer les fiches métadonnées pour chaque ligne de production :
  - description ;
  - propriétaire, par exemple responsable maintenance ;
  - source ;
  - fréquence de collecte.
- Documenter les colonnes clés :
  - unités ;
  - plages normales ;
  - signification du champ `label` : `0 = nominal`, `1 = anomalie`.
- Configurer les règles de cycle de vie MinIO ILM :
  - archivage automatique après 180 jours ;
  - suppression après 2 ans.

#### Jours 6-7 (C21)

- Créer 3 comptes de service avec policies MinIO différenciées :
  - `data-analyst` → lecture seule sur `curated/` ;
  - `data-engineer` → lecture/écriture sur `raw/`, `staging/` et `curated/` ;
  - `admin` → tous droits.
- Activer le chiffrement SSE-S3 sur les buckets contenant des données de production, potentiellement sensibles industriellement.
- Activer les logs d'audit MinIO et analyser une session d'accès.
- Rédiger la politique de gouvernance :
  - qui accède à quelles lignes ;
  - sous quelles conditions ;
  - responsabilités par rôle.

#### Jour 8 (C18–C21)

- Restitution :
  - démo live de 15 minutes ;
  - présentation des choix techniques ;
  - auto-évaluation ;
  - rapport de 5 pages.

## Proposition d'installation de MinIO via Docker

### Avec volume local

```bash
docker run -d \
  --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -v ./minio-data:/data \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  quay.io/minio/minio server /data --console-address ":9001"
```

### Sans volume

```bash
docker run -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  quay.io/minio/minio server /data --console-address ":9001"
```

## Architecture cible proposée

```text
raw/
  └── production_lines/
      ├── lineA/ year=2025/month=05/ …
      ├── lineB/ year=2025/month=04/ …
      ├── lineC/ · lineD/ · lineE/

staging/ ← données nettoyées, schéma harmonisé
curated/ ← données prêtes à l'analyse
archive/ ← données expirées (règle ILM MinIO)
```

## Modalités d'évaluation

La restitution du dernier jour s'inscrit dans le cadre de l'épreuve E7 du titre professionnel.

Il est proposé de produire un petit rapport professionnel de minimum 5 pages décrivant les activités mises en œuvre ainsi que les notions abordées.

## Livrables

L'ensemble est déposé dans un dépôt Git organisé, remis au plus tard à la fin du Jour 8, également organisé autour d'un rapport de 5 pages minimum.

| Livrable | Compétence | Format attendu |
|---|---|---|
| Dossier d'architecture + schéma technique annoté | C18 | PDF / draw.io |
| `docker-compose.yml` + script Python d'upload et vérification d'intégrité | C19 | Git |
| 2 DAGs Airflow : ingestion brute + harmonisation + README | C19 | Git |
| Catalogue OpenMetadata peuplé : 5 fiches + politique ILM documentée | C20 | Captures + documentation |
| Matrice des droits d'accès + politique de gouvernance | C21 | Markdown ou PDF |
| Dépôt Git complet et organisé avec README, structure claire | C18–C21 | Git |

## Critères de performance

### C18 — Architecture

- Les 5 lignes sont analysées avant toute décision technique.
- Les hétérogénéités de schéma sont explicitement identifiées dans le dossier.
- L'architecture en couches est justifiée au regard de la volumétrie et de la fréquence.
- Le schéma est lisible, annoté et exploitable par un tiers.

### C19 — Intégration

- MinIO est opérationnel.
- Les 4 buckets sont créés et configurés.
- Les 5 CSV sont ingérés automatiquement avec partitionnement `year=/month=/line=/`.
- Les colonnes sont harmonisées en `staging`.
- Line A est traitée en chunks.
- Le README permet à un autre apprenant de reproduire l'environnement.

### C20 — Catalogue

- Les 5 lignes ont chacune une fiche OpenMetadata complète :
  - description ;
  - propriétaire ;
  - source ;
  - fréquence ;
  - colonnes ;
  - signification de `label`.
- Les règles ILM sont configurées et documentées.

### C21 — Gouvernance

- 3 comptes de service avec policies strictement différenciées par rôle.
- Chiffrement SSE-S3 actif.
- Logs d'audit activés et analysés.
- Politique de gouvernance rédigée :
  - qui accède à quoi ;
  - sous quelles conditions ;
  - avec quelles responsabilités.
