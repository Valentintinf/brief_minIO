# Auto-évaluation, jour 8

**Brief** : conception et déploiement d'un data lake industriel
**Compétences** : C18, C19, C20, C21
**Auteur** : Valentin Fèvre
**Rapport associé** : [`RAPPORT.md`](RAPPORT.md)

J'ai repris un par un les critères de performance de la consigne. Pour chacun je me positionne, j'indique où se trouve la preuve dans le dépôt, et j'ajoute ce que j'aurais pu faire mieux. Les critères que je n'ai pas tenus sont signalés comme tels : une auto-évaluation qui ne trouve rien à redire ne sert à rien.

Échelle : ✅ atteint, 🟡 partiellement atteint, ❌ non atteint.

---

## C18, architecture

| Critère | Niveau | Où le vérifier |
|---|:-:|---|
| Les 5 lignes sont analysées avant toute décision technique | ✅ | `JOUR1_ARCHITECTURE.md` §2 : volumétrie, périodes, plages min/max et taux d'anomalie recalculés depuis `data/`. L'architecture n'arrive qu'au §3 |
| Les hétérogénéités de schéma sont explicitement identifiées | ✅ | Tableau ligne par ligne des colonnes observées, avec la casse sur `Temperature`, `Pressure` et `Elapsed_time`, et l'absence d'`elapsed_time` sur C, D et E |
| L'architecture en couches est justifiée au regard de la volumétrie et de la fréquence | ✅ | Pas de mesure de 60 s, 1 440 points par jour et par ligne, environ 7,9 M d'enregistrements sur 3 ans. Dimensionnement sur cette projection et non sur les 2 Mo initiaux (`RAPPORT.md` §2.1 et §3) |
| Le schéma est lisible, annoté et exploitable par un tiers | ✅ | `architecture_datalake.drawio` et son export SVG : 5 sources avec volumétrie, 2 flux, 4 couches, partitionnement, catalogue, rôles, annotations |

Ce dont je suis plutôt content : l'analyse a vraiment précédé la conception, et elle a débouché sur deux décisions que je n'aurais pas prises sans elle. Partitionner par ligne, parce que les plages nominales n'ont rien à voir d'une ligne à l'autre et qu'un seuil commun serait absurde. Et garder `elapsed_time` en colonne nullable au lieu de l'imputer, pour ne pas fabriquer une mesure qui deviendrait indiscernable d'une vraie.

Ce que j'aurais dû ajouter : une estimation chiffrée de l'empreinte de stockage par couche sur trois ans, en comparant CSV brut et Parquet snappy. Ça aurait objectivé le choix du format colonne au lieu de le justifier avec des arguments qualitatifs.

---

## C19, intégration

| Critère | Niveau | Où le vérifier |
|---|:-:|---|
| MinIO est opérationnel | ✅ | `docker-compose.yml`, service `minio` avec healthcheck, console sur le port 9001, volume persistant `./minio-data` |
| Les 4 buckets sont créés et configurés | ✅ | `upload_to_minio.py` crée `raw`, `staging`, `curated` et `archive` de façon idempotente, avec une policy initiale différente par bucket |
| Les 5 CSV sont ingérés automatiquement avec partitionnement `year=/month=/line=/` | ✅ | `dags/dag_raw_ingestion.py`, 5 tâches parallèles, dépôt en `raw/production_lines/<line>/year=YYYY/month=MM/` |
| Les colonnes sont harmonisées en `staging` | ✅ | `dags/dag_staging_transform.py` : casse normalisée, timestamps parsés, `elapsed_time` nullable, `label` validé, ajout de `line_id`, `ingestion_ts` et `source_file_hash`, sortie en Parquet snappy |
| Line A est traitée en chunks | ✅ | `pd.read_csv(chunksize=1000)`, 10 objets de `_chunk_0001` à `_chunk_0010`, mémoire constante quelle que soit la taille du fichier |
| Le README permet à un autre apprenant de reproduire l'environnement | ✅ | `README.md`, `JOUR2_README.md` et `JOUR3_README.md` : prérequis, `.env`, démarrage, sortie attendue, vérification, arrêt. Séquence complète en annexe B du rapport |

Ce dont je suis plutôt content : la vérification d'intégrité ne se contente pas de calculer un hash, elle compare le MD5 local à l'ETag renvoyé par MinIO et fait sortir le script en code non nul si ça ne colle pas. Pour le chunking, j'avais d'abord vu ça comme une contrainte de l'énoncé, puis je me suis rendu compte que c'est exactement le schéma d'un flux réel, où chaque lot arrive seul et où l'échec d'un lot ne casse pas les autres.

Ce que j'aurais dû faire autrement, sur deux points. Je n'ai écrit aucun test automatisé sur les fonctions de transformation, je me suis contenté de lancer les DAGs et de regarder le résultat dans MinIO. Trois ou quatre tests unitaires sur la normalisation de casse et la gestion du nullable sécuriseraient toute évolution future, et ça m'aurait pris une heure. Par ailleurs les DAGs sont en `schedule="@once"`, ce qui convient pour un rejeu manuel mais pas pour une vraie collecte continue, qui demanderait une planification et une détection des nouveaux fichiers.

---

## C20, catalogue

| Critère | Niveau | Où le vérifier |
|---|:-:|---|
| Les 5 lignes ont une fiche complète : description, propriétaire, source, fréquence, colonnes, sens de `label` | ✅ | `JOUR5_CATALOG.md` : 5 fiches, 8 colonnes documentées sur 8 pour chacune, propriétaire « Responsable maintenance », source Zenodo, fréquence d'une mesure par minute, `label` documenté comme 0 nominal et 1 anomalie |
| Les règles ILM sont configurées et documentées | ✅ | `minio_ilm_setup.py` et `JOUR5_ILM_POLICY.md` : 4 règles relues sur le serveur, archivage à 180 jours par `dag_ilm_archive`, suppression à 730 jours par l'ILM native |
| Livrable attendu au format « captures et documentation » | ❌ | Il n'y a aucune capture d'écran dans le dépôt |

Ce dont je suis plutôt content : deux choix qui ne sautent pas aux yeux mais qui comptent. Le rapport de catalogue est écrit à partir des fiches relues sur le serveur et pas de ce que le script a envoyé, et c'est comme ça que je me suis aperçu qu'OpenMetadata réécrit certaines descriptions sans prévenir. Et le filtre qui exclut les dix chunks de la ligne A, parce que le catalogue doit documenter une ligne de production et pas un découpage technique.

La politique ILM m'a demandé de composer avec une limite de l'API S3, incapable de déplacer un objet d'un bucket à un autre. J'ai résolu ça avec deux mécanismes, un DAG pour le déplacement et l'ILM native pour la suppression, en calant l'expiration d'`archive` à 550 jours pour que le total fasse bien 730. L'ILM reste aussi posée sur `raw` et `staging` comme filet de sécurité si Airflow est en panne.

Ce que je n'ai pas fait : les captures d'écran. C'est le seul livrable explicitement demandé qui manque au dépôt. Je les prendrai pendant la démo, sur Explore puis Tables, l'onglet Schema d'une fiche, Govern puis Glossaries, et Buckets puis Lifecycle côté MinIO, et je les verserai au dépôt.

Ce que j'aurais pu pousser plus loin : le lignage. OpenMetadata connaît les cinq tables de `staging` mais ne sait pas quel DAG les fabrique depuis `raw`. C'est l'apport le plus évident du catalogue que je n'ai pas exploité.

---

## C21, gouvernance

| Critère | Niveau | Où le vérifier |
|---|:-:|---|
| 3 comptes de service avec policies strictement différenciées | ✅ | `minio_governance_setup.py` et `JOUR6_ACCESS_MATRIX.md` : matrice mesurée par 48 essais boto3 réels (3 comptes × 4 buckets × 4 opérations), le script échoue dès qu'un droit ne correspond pas |
| Chiffrement SSE-S3 actif | ✅ | KMS interne MinIO via `MINIO_KMS_SECRET_KEY`, SSE-S3 par défaut sur les 4 buckets, `head_object` renvoie `AES256` |
| Logs d'audit activés et analysés | ✅ | Webhook MinIO vers `audit_collector.py` puis `audit.jsonl`, analyse par `analyze_audit_logs.py` vers `JOUR7_AUDIT.md` : 97 appels, 79 autorisés, 18 refusés, 4 identités, lectures par identité, opération, bucket et refus |
| Politique de gouvernance rédigée : qui accède à quoi, sous quelles conditions, avec quelles responsabilités | ✅ | `JOUR6_GOUVERNANCE.md` : 5 principes, matrice des droits, conditions d'octroi et de révocation, conditions techniques, responsabilités des 4 rôles, matrice RACI de 9 activités, 6 signaux de détection, limites assumées |

C'est la partie que je considère comme la plus réussie du brief, et ce qui fait la différence tient en une idée : vérifier les droits au lieu de les déclarer. Une policy que l'API a acceptée sans erreur ne prouve rien, ce qui compte c'est qu'un client muni des identifiants se fasse effectivement refuser ce qu'il doit l'être. Le script tente donc les 48 opérations avec de vrais identifiants, en boto3, qui est le protocole des vrais clients, et s'arrête au premier écart avec la matrice attendue.

Cette approche m'a fait tomber sur deux pièges qui auraient produit une gouvernance fausse mais d'apparence correcte. `s3:ListBucket` porte sur l'ARN du bucket sans `/*` alors que `s3:GetObject` porte sur les objets avec `/*`, et les confondre donne une policy incohérente dont le symptôme est difficile à lire. Et un `GetObject` sur une clé absente renvoie `NoSuchKey` et pas `AccessDenied`, parce que MinIO évalue l'autorisation avant l'existence de l'objet : si je l'avais compté comme un refus, toute ma matrice aurait sous-estimé les droits en lecture.

Côté audit, j'ai veillé à ne pas m'arrêter au décompte. Les 18 refus sont tous expliqués et tous attendus, et j'ai posé la grille qui va avec : un refus inattendu veut dire policy trop étroite qui bloque un traitement légitime, un accès inattendu veut dire policy trop large donc exposition de données.

Ce qui reste faible, et qui est documenté comme tel au §5.3 de la politique de gouvernance :

- le journal d'audit vit sur le même hôte que MinIO, donc un incident emporte à la fois la donnée et sa trace ;
- `audit.jsonl` grossit sans rotation ni durée de conservation ;
- aucun des six signaux de détection ne déclenche d'alerte automatique ;
- le transport est en HTTP, TLS est un prérequis avant un usage réel ;
- les comptes humains nominatifs restent à créer, les trois comptes livrés sont des comptes de service.

---

## Synthèse

| Compétence | Critères tenus | Niveau |
|---|:-:|:-:|
| C18, architecture | 4 sur 4 | ✅ |
| C19, intégration | 6 sur 6 | ✅ |
| C20, catalogue | 2 sur 2, mais le livrable « captures » manque | 🟡 |
| C21, gouvernance | 4 sur 4 | ✅ |

Les 16 critères de performance sont tenus. Il manque un livrable de forme, les captures d'écran du catalogue.

### L'écart entre l'architecture décrite et ce que je livre

Il n'y en a qu'un, et je l'assume : la couche `curated` n'est pas alimentée. Le bucket existe, il est chiffré, couvert par les policies et par l'ILM, et `data-analyst` y a bien ses droits de lecture, mais aucun pipeline n'écrit dedans et la chaîne s'arrête à `staging`.

Le brief demandait deux DAGs, ingestion et harmonisation, et je les ai livrés, avec un troisième pour le cycle de vie. Remplir `curated` relève du cas d'usage analytique, qui n'était pas dans le périmètre des huit jours. J'ai préféré consolider la gouvernance plutôt que de produire des agrégats dont les règles métier n'ont été validées par personne. Une couche vide et signalée comme telle me semble plus honnête qu'une couche remplie d'indicateurs que je serais seul à connaître.

### Ce que ce brief m'a appris

Surtout une chose : une configuration qu'on n'a pas vérifiée n'est pas vraiment configurée. Ça s'est vérifié trois fois pendant le brief, avec une policy acceptée par l'API mais fausse sur l'ARN, une description réécrite en silence par OpenMetadata, et une action ILM acceptée par MinIO puis ignorée. C'est pour ça que les trois rapports générés du dépôt sont tous écrits à partir de l'état relu sur le serveur.

J'ai aussi constaté que les obstacles finissent par servir. L'impossibilité de déplacer un objet entre buckets m'a amené à poser une double sécurité sur la rétention, DAG plus ILM native, qui est plus robuste que ce que j'avais prévu au départ.

Et sur la gouvernance, j'ai compris que ce sont les refus qui prouvent quelque chose. Une policy qui n'a jamais rien bloqué n'a jamais été testée.

### Si c'était à refaire

Trois choses. Écrire les tests unitaires sur les transformations dès le jour 3 au lieu de valider en regardant le résultat. Prendre les captures d'écran au fur et à mesure de chaque étape de configuration, plutôt que de les repousser à la restitution et de me retrouver à les faire après coup. Et brancher le lignage OpenMetadata au moment où je crée le service, pendant que j'ai encore les DAGs en tête.

### Prochaines étapes

1. Spécifier puis alimenter la couche `curated` avec le propriétaire métier.
2. Brancher le lignage `raw` vers `staging` vers `curated` dans OpenMetadata.
3. Exporter les logs d'audit hors de la plateforme, avec rotation et alertes.
4. Activer TLS sur MinIO et créer les comptes humains nominatifs.
5. Lancer le cas d'usage de maintenance prédictive sur l'historique accumulé.
