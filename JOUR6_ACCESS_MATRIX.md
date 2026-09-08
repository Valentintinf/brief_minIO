# Matrice des droits d'accès MinIO — Jours 6-7 (C21)

Généré par `minio_governance_setup.py`. Les droits ci-dessous ne sont pas
recopiés des policies : ils sont **observés**, en tentant réellement chaque
opération avec les identifiants de chaque compte.

Endpoint testé : `http://localhost:9000`

## Comptes de service

| Compte | Policy | Rôle |
|---|---|---|
| `data-analyst` | `data-analyst` | Lecture seule sur curated/ — consommation analytique |
| `data-engineer` | `data-engineer` | Lecture/écriture sur raw/, staging/ et curated/ — pipelines |
| `admin` | `admin` | Tous droits sur les 4 buckets et l'administration MinIO |

## Droits observés

`L` = ListBucket · `G` = GetObject · `P` = PutObject · `D` = DeleteObject

| Compte | Bucket | L | G | P | D |
|---|---|:-:|:-:|:-:|:-:|
| `data-analyst` | `raw` | — | — | — | — |
| `data-analyst` | `staging` | — | — | — | — |
| `data-analyst` | `curated` | ✓ | ✓ | — | — |
| `data-analyst` | `archive` | — | — | — | — |
| `data-engineer` | `raw` | ✓ | ✓ | ✓ | ✓ |
| `data-engineer` | `staging` | ✓ | ✓ | ✓ | ✓ |
| `data-engineer` | `curated` | ✓ | ✓ | ✓ | ✓ |
| `data-engineer` | `archive` | — | — | — | — |
| `admin` | `raw` | ✓ | ✓ | ✓ | ✓ |
| `admin` | `staging` | ✓ | ✓ | ✓ | ✓ |
| `admin` | `curated` | ✓ | ✓ | ✓ | ✓ |
| `admin` | `archive` | ✓ | ✓ | ✓ | ✓ |

## Chiffrement au repos

| Bucket | Chiffrement par défaut |
|---|---|
| `raw` | SSE-S3 |
| `staging` | SSE-S3 |
| `curated` | SSE-S3 |
| `archive` | SSE-S3 |

Tout objet déposé dans ces buckets est chiffré par MinIO sans que le client
ait à le demander. Vérification :

```bash
python minio_governance_setup.py --check
docker exec minio mc encrypt info local/raw
```

## Reproduire

```bash
python minio_governance_setup.py
```

Le script échoue si un seul droit observé s'écarte de la matrice attendue.
