# Conception et déploiement d'un data lake industriel

**Rapport professionnel, épreuve E7 (compétences C18 à C21)**

| | |
|---|---|
| Auteur | Valentin Fèvre |
| Contexte | Brief « Data Lake industriel », 8 jours |
| Technologies | MinIO Community, Apache Airflow 2.9.3, OpenMetadata 1.13.5, Python / boto3, Docker Compose, Git |
| Jeu de données | *Synthetic Data from Industrial Sensor Monitoring*, Polytechnic Institute of Porto / INESC TEC, [Zenodo, avril 2025](https://zenodo.org/records/15277168) |
| Dépôt | `brief_minIO` |

---

## 1. Contexte et objectifs

Le client exploite cinq lignes de production instrumentées. Chacune remonte en continu des mesures de température, de pression et parfois de durée de cycle, avec un indicateur d'anomalie. Les données existent déjà mais elles sont éparpillées, elles n'ont pas le même format d'une ligne à l'autre et rien n'indique qui en est responsable ni qui peut y accéder.

La DSI vise à terme un projet de maintenance prédictive, c'est-à-dire l'entraînement de modèles capables de repérer les signes avant-coureurs d'une panne pour limiter les arrêts non planifiés. Ce type de projet suppose trois choses que la situation actuelle ne permet pas : un historique long, des mesures comparables entre équipements, et la possibilité de remonter d'un résultat au fichier qui l'a produit.

Ma mission sur ces huit jours portait donc sur la fondation technique : concevoir l'architecture, déployer le stockage, construire les pipelines, cataloguer les jeux de données et mettre en place la gouvernance des accès. Cela correspond aux quatre compétences visées par le brief, C18 pour l'architecture, C19 pour l'intégration, C20 pour le catalogue et C21 pour la gouvernance.

Ce rapport décrit ce que j'ai fait, pourquoi j'ai tranché dans un sens plutôt qu'un autre, les problèmes que j'ai rencontrés en cours de route, et ce que la plateforme ne fait pas encore.

---

## 2. Analyse des données sources (C18)

J'ai commencé par regarder les fichiers avant de dessiner quoi que ce soit. Les chiffres ci-dessous ont tous été recalculés depuis `data/`.

### 2.1 Volumétrie

| Ligne | Fichier | Lignes | Colonnes | Taille | Période |
|---|---|---:|---:|---:|---|
| A | `LineA_Stable_10K.csv` | 10 000 | 5 | 775 680 o | 2025-05-01 → 2025-05-07 |
| B | `LineB_Flux.csv` | 5 000 | 5 | 390 706 o | 2025-04-01 → 2025-04-04 |
| C | `LineC_Turbulent.csv` | 5 000 | 4 | 295 024 o | 2025-03-01 → 2025-03-04 |
| D | `LineD_SpikeControl.csv` | 5 000 | 4 | 295 098 o | 2025-02-01 → 2025-02-04 |
| E | `LineE_SmoothRun.csv` | 5 000 | 4 | 295 081 o | 2025-01-01 → 2025-01-04 |
| Total | 5 CSV | 30 000 | | 2,05 Mo | janvier à mai 2025 |

Deux mégaoctets, c'est très peu. Le piège aurait été de dimensionner la plateforme pour ce volume. Le pas de mesure observé est d'une minute, ce qui donne 1 440 points par jour et par ligne. Sur cinq lignes en collecte continue, les trois ans d'historique nécessaires à un entraînement supervisé représentent environ 7,9 millions d'enregistrements. J'ai donc raisonné sur cette projection et pas sur le fichier d'aujourd'hui : stockage objet extensible, partitionnement mis en place dès le premier dépôt, format colonne compressé en aval.

### 2.2 Les écarts de schéma

Les cinq fichiers n'ont pas le même schéma. C'est voulu dans le jeu de données et c'est le vrai problème du brief.

| Ligne | Colonnes | Écart |
|---|---|---|
| A | `timestamp`, `Temperature`, `pressure`, `elapsed_time`, `label` | casse sur `Temperature` |
| B | `timestamp`, `temperature`, `pressure`, `Elapsed_time`, `label` | casse sur `Elapsed_time` |
| C | `timestamp`, `Temperature`, `pressure`, `label` | `elapsed_time` absent |
| D | `timestamp`, `temperature`, `Pressure`, `label` | casse, et `elapsed_time` absent |
| E | `timestamp`, `Temperature`, `pressure`, `label` | `elapsed_time` absent |

Il y a en fait deux problèmes différents derrière ce tableau, et ils ne se traitent pas pareil.

La casse est un détail d'écriture. `Temperature` et `temperature` désignent la même grandeur physique, donc passer tout en minuscules ne perd aucune information.

L'absence d'`elapsed_time` sur C, D et E est autre chose. Ces trois lignes n'instrumentent pas la durée de cycle, donc l'information n'existe pas. J'ai hésité à remplir la colonne avec une moyenne pour obtenir cinq fichiers parfaitement identiques, puis j'y ai renoncé : une valeur inventée devient impossible à distinguer d'une mesure réelle une fois le fichier écrit, et le jour où un modèle s'entraîne dessus personne ne saura faire la différence. J'ai donc gardé `elapsed_time` dans le schéma cible en la déclarant nullable. Elle vaut `NULL` pour C, D et E, ce qui se voit.

### 2.3 Qualité et anomalies

| Ligne | Température min/max | Pression min/max | `elapsed_time` | Anomalies |
|---|---|---|---|---:|
| A | 179,49 / 185,19 | 156,80 / 160,63 | 34,27 / 37,23 | 18 sur 10 000 (0,18 %) |
| B | 186,76 / 202,72 | 108,95 / 123,53 | 18,65 / 22,98 | 50 sur 5 000 (1,00 %) |
| C | 194,65 / 216,27 | 88,32 / 104,73 | absent | 200 sur 5 000 (4,00 %) |
| D | 194,81 / 213,11 | 90,43 / 104,23 | absent | 15 sur 5 000 (0,30 %) |
| E | 198,38 / 209,60 | 96,75 / 100,88 | absent | 25 sur 5 000 (0,50 %) |

Les contrôles ne remontent aucun horodatage invalide, aucun doublon complet, aucune valeur numérique manquante sur les colonnes présentes, et `label` ne prend que les valeurs 0 et 1.

Deux observations ont pesé sur la suite. D'abord les plages nominales n'ont rien à voir d'une ligne à l'autre : la pression tourne autour de 158 hPa sur la ligne A et autour de 96 hPa sur la ligne C. Un seuil d'alerte commun aux cinq lignes n'aurait donc aucun sens, et le futur modèle devra être contextualisé par ligne. C'est ce qui m'a décidé à garder `line_id` comme dimension de premier plan et à partitionner par ligne. Ensuite le taux d'anomalie varie de 0,18 % à 4 %, ce qui est très déséquilibré. C'est un problème pour le ML plus tard, mais c'est surtout un argument pour conserver l'historique longtemps, puisque c'est la seule façon de peupler la classe rare. La règle de rétention à deux ans vient de là.

---

## 3. Architecture retenue (C18)

### 3.1 Les quatre couches

```
Sources CSV (Zenodo)
      ↓  Airflow + boto3
raw/      données brutes, inchangées, partitionnées year=/month=/
      ↓  DAG de transformation
staging/  schéma harmonisé, typé, Parquet snappy
      ↓
curated/  données prêtes à l'analyse métier et au ML
      ↓  180 jours
archive/  rétention longue, suppression à 2 ans
```

J'ai repris le découpage en couches de type medallion, avec une règle que je me suis fixée : chaque couche a une seule raison de changer.

`raw` reçoit les fichiers tels quels, sans aucune modification. C'est ce qui permet de rejouer les traitements. Si je découvre dans six mois une erreur dans la transformation, je relance le DAG, je n'ai pas besoin de retourner chercher les sources. Le partitionnement `production_lines/<line>/year=/month=/` est posé dès l'ingestion, parce que repartitionner après coup un historique de plusieurs millions d'objets coûte cher et que je préfère ne jamais avoir à le faire.

`staging` est la seule couche où l'hétérogénéité du paragraphe 2.2 est traitée. J'y écris du Parquet compressé en snappy pour trois raisons : les types sont portés par le fichier donc il n'y a plus de réinférence à chaque lecture, la compression colonne réduit nettement l'empreinte sur un historique long, et une lecture analytique ne charge que les colonnes utiles.

`curated` est prévue pour les agrégats et les indicateurs métier. Le bucket est créé et configuré mais il n'est pas encore alimenté, j'y reviens au paragraphe 8.4.

`archive` isole les données âgées. Mettre l'archive dans un bucket séparé plutôt que dans un préfixe simplifie beaucoup la gouvernance, puisque les droits et les règles de cycle de vie s'expriment alors au niveau du bucket, sans condition de date ni de chemin.

### 3.2 Le schéma cible

| Colonne | Type | Origine |
|---|---|---|
| `timestamp` | datetime | parsing de la colonne source |
| `line_id` | string | ajouté à la transformation (`lineA` à `lineE`) |
| `temperature` | float | depuis `Temperature` ou `temperature` |
| `pressure` | float | depuis `Pressure` ou `pressure` |
| `elapsed_time` | float nullable | si présent, `NULL` sinon, jamais imputé |
| `label` | int | validé dans {0, 1} |
| `ingestion_ts` | datetime | horodatage technique d'ingestion |
| `source_file_hash` | string | MD5 du fichier `raw/` d'origine |

Les deux dernières colonnes ne viennent pas du jeu de données, je les ai ajoutées pour la traçabilité. Depuis n'importe quelle ligne de `staging`, on retrouve le fichier source exact et le moment où il a été traité. C'est ce qui permettra de remonter d'une prédiction bizarre au lot de données qui l'a produite.

### 3.3 Choix techniques

| Besoin | Choix | Pourquoi |
|---|---|---|
| Stockage | MinIO Community | API S3 standard, donc le code d'ingestion est portable vers S3 ou un autre fournisseur sans réécriture ; déploiement Docker léger ; les buckets servent de frontière de gouvernance |
| Ingestion | Python / boto3 | SDK S3 de référence, et l'ETag permet de vérifier l'intégrité sans outil supplémentaire |
| Orchestration | Airflow 2.9.3 | dépendances explicites entre couches, rejeu par tâche, historique des exécutions, planification du cycle de vie |
| Catalogue | OpenMetadata 1.13.5 | fiches de jeux de données, propriétaires, documentation des colonnes, glossaire métier, découverte automatique des schémas |
| Format aval | Parquet snappy | typage porté par le fichier, compression, lecture colonne |
| Sécurité | policies MinIO, SSE-S3, audit par webhook | couvre les trois questions : qui peut faire quoi, qu'est-ce qui est protégé, qui a fait quoi |

J'ai écarté une base relationnelle : le besoin porte sur un historique volumineux, des schémas qui bougent et des fichiers hétérogènes, pas sur du transactionnel. J'ai aussi écarté un simple partage de fichiers, qui ne donne ni granularité d'accès, ni chiffrement par défaut, ni journal d'audit, donc aucun des prérequis de C21.

Le schéma technique annoté est dans `architecture_datalake.drawio`, avec un export SVG pour le consulter sans installer draw.io.

---

## 4. Déploiement de l'infrastructure (C19)

Toute la plateforme est décrite en Docker Compose, en deux fichiers. `docker-compose.yml` contient le data lake, c'est-à-dire MinIO, PostgreSQL, le webserver et le scheduler Airflow, plus le collecteur d'audit ajouté au jour 6. `docker-compose.openmetadata.yml` est un overlay qui ajoute MySQL, Elasticsearch, le serveur OpenMetadata et son Airflow d'ingestion.

J'ai fait d'OpenMetadata un overlay et non un fichier autonome pour que ses conteneurs rejoignent le réseau du projet. Ils atteignent ainsi MinIO par son nom de service, `http://minio:9000`, sans que j'aie à exposer quoi que ce soit ni à jongler avec des adresses IP.

```bash
docker-compose up -d                                                           # data lake
docker-compose -f docker-compose.yml -f docker-compose.openmetadata.yml up -d  # avec le catalogue
```

Les quatre buckets sont créés par `upload_to_minio.py`, qui est idempotent et applique au passage une policy initiale différente selon le bucket. Les cinq CSV sont ensuite déposés dans `raw/production_lines/<line>/`.

Pour l'intégrité, je calcule le MD5 en local avant l'envoi, par blocs d'un mégaoctet, et je le compare à l'ETag renvoyé par MinIO. Sur un dépôt simple l'ETag est le MD5 de l'objet, donc la comparaison est directe. En cas d'écart le fichier est marqué KO et le script sort en code non nul, il ne se contente pas d'un avertissement. Le détail fichier par fichier est écrit dans `JOUR2_UPLOAD_MANIFEST.md`.

Un point d'exploitation à connaître : deux Airflow tournent en parallèle, sur les ports 8080 et 8081. Celui du data lake orchestre les pipelines de données, celui d'OpenMetadata n'exécute que les workflows de catalogage déployés par le serveur. Je les ai confondus au moins une fois pendant le brief, donc la distinction est écrite noir sur blanc dans le README.

---

## 5. Pipelines d'ingestion et d'harmonisation (C19)

J'ai écrit trois DAGs.

### 5.1 `dag_raw_ingestion`

Cinq tâches indépendantes, une par ligne, qui s'exécutent en parallèle. Chacune dépose son fichier au bon endroit :

```
raw/production_lines/lineB/year=2025/month=04/LineB_Flux.csv
```

La ligne A est traitée en chunks de 1 000, comme demandé, avec `pd.read_csv(chunksize=1000)`. Les 10 000 enregistrements donnent dix objets numérotés de `_chunk_0001` à `_chunk_0010`. Au-delà de la consigne, cette découpe correspond à ce qu'on ferait sur un vrai flux : chaque lot arrive indépendamment, l'échec d'un lot ne casse ni les précédents ni les suivants, et la mémoire consommée reste celle d'un chunk quelle que soit la taille du fichier.

### 5.2 `dag_staging_transform`

Le DAG enchaîne la normalisation de la casse, le parsing des horodatages en `datetime64[ns]`, l'ajout d'`elapsed_time` en `Float64` nullable quand la colonne manque, la vérification que `label` ne contient que des 0 et des 1, puis l'ajout de `line_id`, `ingestion_ts` et `source_file_hash`. La sortie part en Parquet snappy avec le même partitionnement que la source.

Sur la validation de `label`, j'ai choisi de lever une erreur et de faire échouer la tâche plutôt que de filtrer la ligne fautive en silence. Une tâche en rouge se voit dans Airflow, une ligne supprimée discrètement ne se voit nulle part.

C'est ce DAG qui règle le problème d'hétérogénéité. Tout ce qui est en aval, catalogue compris, voit cinq jeux de données au schéma identique. Je n'ai pas eu à le prouver moi-même : l'ingestion automatique d'OpenMetadata a lu les Parquet, en a déduit les colonnes, et a trouvé le même schéma sur les cinq lignes.

### 5.3 `dag_ilm_archive`

Balayage quotidien de `raw` et `staging`, déplacement vers `archive` de tout objet de plus de 180 jours. La copie se fait avant la suppression, volontairement, pour qu'un échec de copie laisse la source intacte et la tâche rejouable. Les objets gardent leur chemin d'origine préfixé par le bucket de provenance, et reçoivent trois métadonnées de traçabilité, `archived_from`, `archived_at` et `original_last_modified`.

La raison pour laquelle ce déplacement passe par un DAG et non par une règle ILM est expliquée au paragraphe 8.1.

---

## 6. Catalogue et cycle de vie (C20)

### 6.1 Les cinq fiches

`openmetadata_catalog.py` est idempotent et fait, dans l'ordre : création du service Datalake `minio_datalake` pointant sur le bucket `staging`, déploiement d'un pipeline d'ingestion planifié à 3 h sur l'Airflow d'OpenMetadata, création de l'équipe « Maintenance industrielle » et du propriétaire « Responsable maintenance », création du glossaire, puis enrichissement des cinq fiches.

| Ligne | Fiche | Enregistrements | Anomalies | Colonnes documentées |
|---|---|---:|---:|---:|
| lineA | Ligne A, régime stable | 10 000 | 0,18 % | 8 sur 8 |
| lineB | Ligne B, flux moyen | 5 000 | 1,00 % | 8 sur 8 |
| lineC | Ligne C, régime turbulent | 5 000 | 4,00 % | 8 sur 8 |
| lineD | Ligne D, pics contrôlés | 5 000 | 0,30 % | 8 sur 8 |
| lineE | Ligne E, fonctionnement lissé | 5 000 | 0,50 % | 8 sur 8 |

Chaque fiche porte une description métier, le propriétaire, le lien vers la source Zenodo, la fréquence de collecte (une mesure par minute, soit 1 440 points par jour et par ligne) et la durée de rétention en `P730D`. Les huit colonnes sont documentées avec leur unité et leur plage nominale, et la colonne `label` indique explicitement que 0 vaut nominal et 1 anomalie. J'ai ajouté un glossaire « Maintenance prédictive » avec trois termes, État nominal, Anomalie et Durée de cycle, pour fixer le vocabulaire.

Un filtre `.*_chunk_.*` exclut du catalogue les dix chunks de la ligne A. L'idée est que le catalogue documente une ligne de production, pas un découpage technique d'ingestion. Dix entrées pour la ligne A auraient dégradé la recherche sans rien apprendre à personne.

Le rapport `JOUR5_CATALOG.md` est généré à partir des fiches relues sur le serveur, pas de ce que le script a envoyé. Cette précaution s'est révélée utile, voir le paragraphe 8.3.

### 6.2 Le cycle de vie

| Échéance | Événement | Mécanisme |
|---|---|---|
| J+0 | dépôt dans `raw/` puis `staging/` | DAGs d'ingestion et de transformation |
| J+180 | déplacement vers `archive/` | `dag_ilm_archive` |
| J+730 | suppression définitive | règle ILM MinIO |

Quatre règles sont enregistrées puis relues sur MinIO : expiration à 730 jours sur `raw`, `staging` et `curated`, et à 550 jours sur `archive`. L'écart n'est pas une coquille. Les objets arrivent dans `archive` en ayant déjà 180 jours, et 180 plus 550 font bien 730. L'horizon de deux ans est donc tenu quel que soit le chemin suivi par l'objet.

J'ai gardé l'expiration sur `raw` et `staging` même si le DAG est censé vider ces buckets à 180 jours. Elle sert de filet de sécurité : si Airflow tombe ou si le DAG reste en pause, les objets finissent quand même par être purgés à l'échéance. La règle de rétention ne dépend pas de la disponibilité de l'orchestrateur.

---

## 7. Gouvernance, sécurité et traçabilité (C21)

### 7.1 Les trois comptes de service

L = lister, G = lire, P = écrire, D = supprimer.

| Compte | `raw` | `staging` | `curated` | `archive` |
|---|:-:|:-:|:-:|:-:|
| `data-analyst` | aucun | aucun | L G | aucun |
| `data-engineer` | L G P D | L G P D | L G P D | aucun |
| `admin` | L G P D | L G P D | L G P D | L G P D |

Ce tableau n'est pas recopié des policies, il est mesuré. C'est le point sur lequel j'ai le plus insisté. Une policy acceptée par l'API sans message d'erreur ne prouve rien du tout ; ce qui compte, c'est qu'un client muni de ces identifiants se voie effectivement refuser ce qu'il doit l'être. `minio_governance_setup.py` crée donc un client boto3 par compte et tente les quatre opérations sur les quatre buckets, soit 48 essais réels, puis compare le résultat à la matrice attendue. Le script s'arrête en erreur dès qu'un seul droit ne correspond pas. Les objets témoins déposés pour ces tests sont préfixés `_verif_acl/` et supprimés à la fin, y compris quand la vérification échoue.

Le cas de `data-engineer` sur `archive` demande une explication, parce qu'il surprend : l'ingénieur de données n'a aucun droit sur l'archive alors que c'est bien son pipeline qui y envoie les données. C'est volontaire, l'archivage relève du cycle de vie et pas du traitement de données, donc le DAG tourne avec des identifiants privilégiés distincts. La conséquence est que restaurer une donnée archivée oblige à passer par l'administrateur. Je l'assume, c'est le prix de l'immuabilité de cette couche.

### 7.2 Chiffrement au repos

SSE-S3 est actif sur les quatre buckets. Le chiffrement est posé au niveau du bucket, donc aucun client n'a d'en-tête particulier à fournir, MinIO chiffre de lui-même. Un `head_object` sur un objet déposé renvoie `ServerSideEncryption: AES256`.

SSE-S3 a besoin d'un KMS, qui détient la clé maîtresse dont MinIO dérive les clés d'objet. J'utilise le KMS interne à clé statique de MinIO, configuré par `MINIO_KMS_SECRET_KEY` au format `nom-de-clé:clé-de-32-octets-en-base64`. Perdre cette clé rend les objets chiffrés définitivement illisibles. Celle qui se trouve dans le dépôt est une clé de démonstration, elle doit être remplacée dans `.env` et sauvegardée ailleurs que sur la plateforme pour un usage réel. L'avertissement figure aussi dans la politique de gouvernance.

### 7.3 Journal d'audit

MinIO n'écrit pas de fichier de log d'audit, il envoie un POST JSON sur un webhook HTTP à chaque appel S3. J'ai donc écrit un petit service `audit-collector` qui reçoit ces événements et les persiste en JSON Lines. MinIO déclare `depends_on: audit-collector`, ce qui fait qu'il refuse de démarrer si la cible du webhook est injoignable. Aucun appel ne passe donc hors journal.

L'exécution de la configuration de gouvernance a produit à elle seule une session exploitable : 97 appels S3, 79 autorisés, 18 refusés, 4 identités distinctes.

| Compte | Appels | Refus | Écritures |
|---|---:|---:|---:|
| `admin` | 16 | 0 | 8 |
| `data-analyst` | 16 | 14 | 0 |
| `data-engineer` | 16 | 4 | 6 |

Le décompte seul n'apprend rien, il faut le lire. `data-analyst` est refusé 14 fois sur 16, ce qui est exactement ce qu'on attend d'un compte limité à la lecture de `curated` : il se fait jeter sur `raw`, `staging` et `archive` parce qu'ils sont hors de son périmètre, et sur `PutObject` et `DeleteObject` dans `curated` parce qu'il y est en lecture seule. `data-engineer` est refusé quatre fois, toutes sur `archive`. Aucune écriture n'aboutit pour l'analyste.

La grille que j'applique est la suivante : un refus auquel je ne m'attendais pas signale une policy trop étroite qui bloque un traitement légitime, un accès auquel je ne m'attendais pas signale une policy trop large, donc une exposition de données. Sans cette lecture, le journal d'audit n'est qu'un fichier qui grossit.

J'ai documenté six signaux à surveiller avec l'action associée : rafale de 403 sur un compte, écriture réussie par un compte en lecture seule, `GetObject` massif sur `curated`, `remotehost` inhabituel, `DeleteObject` en dehors du DAG d'archivage, et usage du compte root.

### 7.4 La politique de gouvernance

`JOUR6_GOUVERNANCE.md` reprend cinq principes directeurs (moindre privilège, séparation lecture/écriture, traçabilité systématique, chiffrement par défaut, sensibilité industrielle des mesures), la matrice des droits, les conditions d'octroi et de révocation, les conditions techniques, les responsabilités des quatre rôles et une matrice RACI de neuf activités.

Sur les conditions d'octroi, j'ai retenu : demande auprès du responsable maintenance qui est propriétaire métier des données, justification de l'usage et de la durée, revue trimestrielle avec désactivation des comptes restés inactifs, et révocation le jour même en cas de départ ou de changement de poste.

J'ai tranché un point explicitement, le cloisonnement par ligne de production. Techniquement, le partitionnement `production_lines/<line>/` permettrait de restreindre un compte à une seule ligne en remplaçant l'ARN `arn:aws:s3:::curated/*` par `arn:aws:s3:::curated/production_lines/lineC/*`. Je ne l'ai pas fait, parce que les cinq lignes relèvent du même périmètre métier et du même service, et qu'un cloisonnement sans besoin réel complique l'exploitation pour rien. En revanche je l'ai documenté avec ses conditions de remise en cause, à savoir des lignes exploitées par des prestataires différents ou une ligne couverte par un contrat de confidentialité particulier. Comme le partitionnement est déjà en place, la restriction s'activerait sans migration de données.

---

## 8. Problèmes rencontrés et arbitrages

Quatre obstacles ont vraiment orienté l'implémentation. Je les détaille parce que c'est là que j'ai le plus appris.

### 8.1 L'API S3 ne déplace pas un objet d'un bucket à un autre

L'architecture prévoyait un archivage automatique à 180 jours vers un bucket dédié. Or le cycle de vie S3 ne propose que deux actions, expirer un objet ou le transitionner vers une classe de stockage. MinIO propose bien une transition vers un remote tier, mais ce tier doit être un déploiement MinIO ou S3 séparé. Le faire pointer vers le bucket `archive` du même serveur n'est pas supporté, et la commande `mc ilm tier add` reste bloquée sans jamais rendre la main. J'ai perdu un bon moment avant de comprendre que ce n'était pas une erreur de ma part.

La solution retenue répartit la politique sur deux mécanismes : un DAG Airflow pour le déplacement à 180 jours, l'ILM native pour la suppression à 2 ans, avec l'expiration d'`archive` calée à 550 jours pour que le total reste de 730. La contrainte technique n'a donc pas dégradé la règle métier.

### 8.2 Les comptes MinIO ne se gèrent pas en boto3

Créer un utilisateur ou une policy relève de l'API d'administration de MinIO, que boto3 ne couvre pas puisqu'il ne parle que S3. Le script pilote donc le client `mc` embarqué dans l'image MinIO via `docker exec`, en passant le document de policy par l'entrée standard pour qu'il n'atterrisse jamais sur le disque de l'hôte.

La vérification des droits, elle, reste en boto3, et c'est délibéré : c'est le protocole que les vrais clients utiliseront. Vérifier avec l'outil d'administration reviendrait à me demander à moi-même si je me suis bien compris.

Deux détails m'ont coûté du temps au passage. `s3:ListBucket` porte sur l'ARN du bucket, sans `/*`, alors que `s3:GetObject` porte sur les objets, avec `/*`. Confondre les deux donne une policy qui autorise la lecture d'un objet dont on connaît la clé mais qui interdit de lister le bucket, et le symptôme n'est pas évident à diagnostiquer. Par ailleurs, un `GetObject` sur une clé qui n'existe pas renvoie `NoSuchKey` et pas `AccessDenied`, parce que MinIO évalue l'autorisation avant l'existence de l'objet. Si je l'avais compté comme un refus, toute ma matrice aurait sous-estimé les droits en lecture.

### 8.3 OpenMetadata masque ses secrets et réécrit les descriptions

Deux comportements non documentés ont déterminé la façon dont le script de catalogage est écrit.

Le premier : seul l'`ingestion-bot` récupère les secrets de connexion en clair. À tout autre porteur de token, y compris le compte admin, le serveur les renvoie masqués. Une ingestion lancée à la main depuis le conteneur échoue donc côté MinIO avec un `SignatureDoesNotMatch`, ce qui est assez déroutant tant qu'on n'a pas compris d'où ça vient. C'est pour cette raison que le script déploie un pipeline via l'API au lieu d'appeler la CLI : le serveur génère lui-même le DAG Airflow et y injecte le JWT du bot, et aucun secret ne transite par mon code.

Le second : OpenMetadata filtre les descriptions de table contre les injections HTML, en convertissant apostrophes, guillemets, esperluettes et signes égal en entités et en supprimant tout ce qui ressemble à une balise, mais il ne filtre pas les descriptions de colonnes. Les fiches de table n'utilisent donc qu'apostrophes typographiques et gras, tandis que les colonnes gardent du Markdown complet. Surtout, le script relit chaque description après écriture et signale toute réécriture par le serveur. Je préfère un rapport qui décrit l'état réel du catalogue plutôt qu'un rapport qui décrit mes intentions.

### 8.4 La couche `curated` n'est pas alimentée

C'est l'écart le plus visible entre l'architecture du paragraphe 3 et ce que je livre. Le bucket existe, il est chiffré, couvert par les policies et par l'ILM, et `data-analyst` y a bien ses droits de lecture, mais aucun pipeline n'écrit dedans. La chaîne s'arrête à `staging`.

Le brief demandait deux DAGs, ingestion et harmonisation, et ils sont livrés, avec un troisième pour le cycle de vie. Alimenter `curated` relève du cas d'usage analytique, qui n'était pas dans le périmètre des huit jours. J'ai préféré consolider la gouvernance et la vérification des droits plutôt que d'ajouter un quatrième DAG produisant des agrégats dont les règles métier n'ont été validées par personne. Une couche vide et signalée comme telle me paraît plus honnête qu'une couche remplie d'indicateurs inventés. Les jeux de données attendus sont décrits au paragraphe 3.1, et leur spécification avec le propriétaire métier est le premier chantier à ouvrir.

---

## 9. Limites et suites

Trois limites de l'installation actuelle sont à lever avant un usage réel, et elles sont documentées comme telles dans la politique de gouvernance.

Le journal d'audit n'est pas répliqué. Il vit sur le même hôte que MinIO, donc un incident sur cet hôte emporte à la fois les données et leur trace. Un journal d'audit devrait être expédié hors de la plateforme qu'il surveille.

Le journal n'a aucune rétention. `audit.jsonl` grossit indéfiniment et n'est jamais purgé. Il lui faut une rotation et une durée de conservation alignée sur l'obligation applicable.

Rien n'est automatisé côté alerte. Les six signaux du paragraphe 7.3 sont documentés et l'analyse est outillée, mais elle reste manuelle et à la demande.

S'y ajoutent deux points d'infrastructure. Le transport est en HTTP, ce qui va pour un déploiement local mais pas en production, où identifiants et données circuleraient en clair : TLS sur MinIO est un prérequis. Et les comptes humains nominatifs restent à créer, les trois comptes livrés étant des comptes de service destinés aux traitements automatisés.

Pour la suite, dans l'ordre où je m'y prendrais : spécifier puis alimenter `curated` avec le propriétaire métier, brancher le lignage OpenMetadata sur les DAGs pour rendre le parcours `raw` vers `staging` vers `curated` navigable depuis le catalogue, exporter les logs d'audit vers un collecteur externe avec des alertes sur les signaux critiques, puis attaquer le cas d'usage de maintenance prédictive proprement dit.

---

## 10. Conclusion

Au départ il y avait cinq fichiers CSV qui ne se ressemblaient pas. À l'arrivée il y a une plateforme avec quatre couches justifiées par l'analyse des données, une infrastructure entièrement décrite en Docker Compose et reproductible par quelqu'un d'autre, trois DAGs qui ingèrent, harmonisent et archivent, un catalogue de cinq fiches documentées jusqu'à la signification de la variable cible, une politique de rétention à deux mécanismes, et une matrice de droits vérifiée par 48 tests réels.

S'il y a une chose que je retiens de ces huit jours, c'est qu'une configuration qu'on n'a pas vérifiée n'est pas vraiment une configuration. Ça s'est confirmé trois fois : une policy acceptée par l'API mais fausse sur l'ARN, une description réécrite sans prévenir par OpenMetadata, une action ILM acceptée par MinIO puis ignorée. C'est pour ça que les trois rapports générés du dépôt sont tous écrits à partir de l'état relu sur le serveur et jamais de ce que le script pensait avoir fait.

J'ai aussi constaté que les obstacles techniques finissent par améliorer la solution. L'impossibilité de déplacer un objet entre buckets m'a conduit à poser une double sécurité sur la rétention, DAG plus ILM native, qui est plus robuste que ce que j'avais prévu au départ.

Enfin, la gouvernance se démontre surtout par ce qu'elle refuse. Les 18 refus de la session analysée, tous attendus et tous expliqués, m'en disent plus long que la lecture de la policy qui les a produits.

La plateforme n'est pas finie, la couche `curated` est vide et il reste du travail sur l'audit et le chiffrement en transit. Mais elle tient debout, elle se redéploie de zéro, et elle sait déjà accumuler, documenter et protéger l'historique dont le projet de maintenance prédictive aura besoin.

---

## Annexes

### A. Livrables du dépôt

| Compétence | Livrable | Fichier |
|---|---|---|
| C18 | Dossier d'architecture | `JOUR1_ARCHITECTURE.md` |
| C18 | Schéma technique annoté | `architecture_datalake.drawio` et son export SVG |
| C19 | Infrastructure | `docker-compose.yml`, `docker-compose.openmetadata.yml` |
| C19 | Upload et intégrité MD5 | `upload_to_minio.py`, `JOUR2_README.md` |
| C19 | DAGs d'ingestion et d'harmonisation | `dags/dag_raw_ingestion.py`, `dags/dag_staging_transform.py`, `JOUR3_README.md` |
| C20 | Catalogue OpenMetadata | `openmetadata_catalog.py`, `JOUR5_CATALOG.md`, `JOUR5_README.md` |
| C20 | Politique ILM | `minio_ilm_setup.py`, `dags/dag_ilm_archive.py`, `JOUR5_ILM_POLICY.md` |
| C21 | Comptes, policies, chiffrement | `minio_governance_setup.py`, `JOUR6_ACCESS_MATRIX.md` |
| C21 | Audit | `audit_collector.py`, `analyze_audit_logs.py`, `JOUR7_AUDIT.md` |
| C21 | Politique de gouvernance | `JOUR6_GOUVERNANCE.md` |
| C18 à C21 | Rapport et auto-évaluation | `RAPPORT.md`, `JOUR8_AUTO_EVALUATION.md` |

### B. Reproduire la plateforme de bout en bout

```bash
cp .env.example .env && echo "AIRFLOW_UID=$(id -u)" >> .env

docker-compose up -d                                 # MinIO + Airflow + PostgreSQL
python upload_to_minio.py                            # buckets, policies, upload, MD5

docker exec airflow-scheduler airflow dags trigger dag_raw_ingestion
docker exec airflow-scheduler airflow dags trigger dag_staging_transform

docker-compose -f docker-compose.yml -f docker-compose.openmetadata.yml up -d
python openmetadata_catalog.py                       # 5 fiches + glossaire
python minio_ilm_setup.py                            # règles de cycle de vie
docker exec airflow-scheduler airflow dags unpause dag_ilm_archive

python minio_governance_setup.py                     # comptes, policies, SSE-S3, 48 tests
python analyze_audit_logs.py --report JOUR7_AUDIT.md
```

Contrôles en lecture seule, lançables à tout moment :

```bash
python minio_governance_setup.py --check
python minio_ilm_setup.py --check
python analyze_audit_logs.py
```

### C. Références

- Jeu de données, *Synthetic Data from Industrial Sensor Monitoring*, Polytechnic Institute of Porto / INESC TEC, Zenodo, avril 2025 : <https://zenodo.org/records/15277168>
- Documentation MinIO : <https://min.io/docs/minio/container/index.html>
- Documentation OpenMetadata : <https://docs.open-metadata.org/>
- Documentation Apache Airflow : <https://airflow.apache.org/docs/>
- IBM, *Qu'est-ce que la maintenance prédictive ?* : <https://www.ibm.com/fr-fr/think/topics/predictive-maintenance>
- AWS, *Maintenance prédictive* : <https://aws.amazon.com/fr/what-is/predictive-maintenance/>
