# Analyse des logs d'audit MinIO — Jours 6-7 (C21)

Généré par `analyze_audit_logs.py` à partir de `audit-logs/audit.jsonl` (97 événements).

## Comment les logs sont produits

MinIO n'écrit pas de fichier de log d'audit : il **POSTe** un événement JSON
sur un webhook HTTP à chaque appel S3. Le service `audit-collector` reçoit ces
événements et les écrit en JSON Lines. La configuration tient en deux
variables d'environnement sur le conteneur MinIO :

```yaml
MINIO_AUDIT_WEBHOOK_ENABLE_datalake: "on"
MINIO_AUDIT_WEBHOOK_ENDPOINT_datalake: http://audit-collector:9999/audit
```

Chaque événement porte l'identité appelante (`accessKey`), l'opération
(`api.name`), la cible (`api.bucket`, `api.object`), le code de retour
(`api.statusCode`), l'IP source (`remotehost`) et le client (`userAgent`).

## Session analysée

| Indicateur | Valeur |
|---|---|
| Appels S3 | 97 |
| Période (UTC) | 2026-09-08 13:24:55 → 2026-09-08 13:26:56 |
| Autorisés | 79 |
| Refusés (403) | 18 |
| Identités distinctes | 4 |

## Répartition par identité

| Compte | Appels | Refus | Écritures | Buckets touchés |
|---|---:|---:|---:|---|
| `admin` | 16 | 0 | 8 | `archive`, `curated`, `raw`, `staging` |
| `data-analyst` | 16 | 14 | 0 | `archive`, `curated`, `raw`, `staging` |
| `data-engineer` | 16 | 4 | 6 | `archive`, `curated`, `raw`, `staging` |
| `minioadmin` | 49 | 0 | 28 | `archive`, `curated`, `raw`, `staging` |

## Répartition par opération

| Opération | Appels | dont refusés |
|---|---:|---:|
| `PutObject` | 17 | 5 |
| `DeleteObject` | 17 | 5 |
| `ListObjectsV2` | 16 | 4 |
| `GetObject` | 13 | 4 |
| `GetBucketLocation` | 10 | 0 |
| `PutBucketEncryption` | 5 | 0 |
| `GetBucketEncryption` | 5 | 0 |
| `AddCannedPolicy` | 3 | 0 |
| `ListUsers` | 3 | 0 |
| `AddUser` | 3 | 0 |
| `AttachDetachPolicyBuiltin` | 3 | 0 |
| `ServerInfo` | 1 | 0 |
| `HeadObject` | 1 | 0 |

## Tentatives refusées

| Compte | Opération | Bucket | Occurrences |
|---|---|---|---:|
| `data-analyst` | `DeleteObject` | `archive` | 1 |
| `data-analyst` | `DeleteObject` | `curated` | 1 |
| `data-analyst` | `DeleteObject` | `raw` | 1 |
| `data-analyst` | `DeleteObject` | `staging` | 1 |
| `data-analyst` | `GetObject` | `archive` | 1 |
| `data-analyst` | `GetObject` | `raw` | 1 |
| `data-analyst` | `GetObject` | `staging` | 1 |
| `data-analyst` | `ListObjectsV2` | `archive` | 1 |
| `data-analyst` | `ListObjectsV2` | `raw` | 1 |
| `data-analyst` | `ListObjectsV2` | `staging` | 1 |
| `data-analyst` | `PutObject` | `archive` | 1 |
| `data-analyst` | `PutObject` | `curated` | 1 |
| `data-analyst` | `PutObject` | `raw` | 1 |
| `data-analyst` | `PutObject` | `staging` | 1 |
| `data-engineer` | `DeleteObject` | `archive` | 1 |
| `data-engineer` | `GetObject` | `archive` | 1 |
| `data-engineer` | `ListObjectsV2` | `archive` | 1 |
| `data-engineer` | `PutObject` | `archive` | 1 |

### Lecture de gouvernance

Ces refus sont le fonctionnement **attendu** des policies : chaque compte
est arrêté dès qu'il sort de son périmètre, et la tentative reste tracée.

- `data-analyst` refusé sur `raw`, `staging` et `archive` : conforme, son
  périmètre est la seule couche `curated`.
- `data-analyst` refusé en `PutObject`/`DeleteObject` sur `curated` :
  conforme, son accès y est en lecture seule.
- `data-engineer` refusé sur `archive` : conforme, l'archivage est une
  opération de cycle de vie, pas de pipeline.

Un refus **inattendu** signalerait une policy trop étroite, bloquant un
traitement légitime. Un accès **inattendu** signalerait une policy trop
large, donc une exposition de données.

## Ce que l'audit permet de détecter

| Signal | Interprétation |
|---|---|
| Rafale de 403 sur un même compte | identifiants compromis, ou pipeline mal configuré |
| Écriture par un compte en lecture seule | policy trop large, à corriger |
| `GetObject` massif sur `curated` | extraction anormale de données |
| `remotehost` inhabituel | accès depuis un poste non prévu |
| `DeleteObject` hors DAG d'archivage | suppression non planifiée à investiguer |

## Rejouer l'analyse

```bash
python analyze_audit_logs.py                    # session complète
python analyze_audit_logs.py --denied-only      # uniquement les refus
python analyze_audit_logs.py --user data-analyst
```
