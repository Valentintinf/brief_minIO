# Jours 6-7 — Comptes de service, chiffrement et audit (C21)

## Objectifs du jour

| Attendu du brief | Réalisation |
|---|---|
| 3 comptes de service avec policies MinIO différenciées | `minio_governance_setup.py` → `JOUR6_ACCESS_MATRIX.md` |
| Chiffrement SSE-S3 sur les buckets de production | KMS interne MinIO + `mc encrypt set sse-s3` sur les 4 buckets |
| Logs d'audit activés et analysés | `audit_collector.py` + `analyze_audit_logs.py` → `JOUR7_AUDIT.md` |
| Politique de gouvernance rédigée | [`JOUR6_GOUVERNANCE.md`](JOUR6_GOUVERNANCE.md) |

---

## Étape 1 — Redémarrer MinIO avec KMS et audit

Deux blocs de variables d'environnement ont été ajoutés au service `minio` dans
`docker-compose.yml`, et un service `audit-collector` créé.

```bash
docker-compose up -d audit-collector minio
```

> **`docker-compose` v1.29 + Docker 29** : la recréation d'un conteneur ayant des
> volumes échoue sur `KeyError: 'ContainerConfig'`, une incompatibilité connue
> entre ces deux versions. Contournement :
> ```bash
> docker-compose rm -sf minio && docker-compose up -d minio
> ```
> Les données de MinIO sont dans le bind mount `./minio-data`, la suppression du
> conteneur ne les touche pas.

### Ce que le KMS apporte

SSE-S3 exige un KMS : c'est lui qui détient la clé maîtresse dont MinIO dérive
les clés d'objet. MinIO embarque un KMS à clé statique, configuré par une seule
variable :

```yaml
MINIO_KMS_SECRET_KEY: ${MINIO_KMS_SECRET_KEY:-datalake-key:oMvLtN…MJZc=}
```

Format `nom-de-clé:clé-de-32-octets-en-base64`. Générer une clé :

```bash
echo "datalake-key:$(openssl rand -base64 32)"
```

> **Perdre cette clé rend les objets chiffrés définitivement illisibles.** La clé
> du dépôt est une clé de démonstration : elle doit être surchargée dans `.env` et
> sauvegardée hors de la plateforme pour tout usage réel.

### Ce que l'audit apporte

MinIO n'écrit **pas** de fichier de log d'audit : il POSTe un événement JSON sur
un webhook HTTP à chaque appel S3.

```yaml
MINIO_AUDIT_WEBHOOK_ENABLE_datalake: "on"
MINIO_AUDIT_WEBHOOK_ENDPOINT_datalake: http://audit-collector:9999/audit
```

Le suffixe `_datalake` nomme la cible : MinIO accepte plusieurs webhooks, un par
suffixe. Le service `audit-collector` reçoit ces événements et les écrit en JSON
Lines dans `audit-logs/audit.jsonl`.

`minio` déclare `depends_on: audit-collector` : MinIO refuse de démarrer si la
cible du webhook est injoignable.

---

## Étape 2 — Créer les comptes et les policies

```bash
python minio_governance_setup.py
```

Le script est idempotent. Il enchaîne :

1. **3 policies MinIO**, une par rôle, générées depuis le dictionnaire `ROLES` ;
2. **3 comptes de service** et rattachement de leur policy ;
3. **chiffrement SSE-S3** sur les 4 buckets ;
4. **vérification empirique** des droits ;
5. **rapport** `JOUR6_ACCESS_MATRIX.md`.

### Les comptes créés

| Compte | Secret par défaut | Périmètre |
|---|---|---|
| `data-analyst` | `Analyst-2025-datalake` | Lecture seule sur `curated` |
| `data-engineer` | `Engineer-2025-datalake` | Lecture/écriture sur `raw`, `staging`, `curated` |
| `admin` | `Admin-2025-datalake` | Tous droits + administration MinIO |

Secrets surchargeables par `MINIO_ANALYST_SECRET`, `MINIO_ENGINEER_SECRET` et
`MINIO_ADMIN_SECRET`. Une relance réaligne le secret du compte sur la valeur
fournie — c'est ainsi que se fait la rotation, sans supprimer ni recréer le
compte :

```bash
MINIO_ANALYST_SECRET='nouveau-secret' python minio_governance_setup.py
```

### Pourquoi la vérification est le cœur du script

Une policy n'est pas « configurée » parce qu'elle a été envoyée sans erreur :
elle l'est quand un client muni de ces identifiants se voit effectivement
autoriser ou refuser chaque opération. Le script instancie donc un client boto3
par compte et tente les 4 opérations sur les 4 buckets — **48 tests** — puis
compare au résultat attendu. **Il échoue au moindre écart.**

Sortie attendue :

```
… vérification des droits (3 comptes × 4 buckets × 4 opérations)
    data-analyst   raw:————  staging:————  curated:✓✓——  archive:————
    data-engineer  raw:✓✓✓✓  staging:✓✓✓✓  curated:✓✓✓✓  archive:————
    admin          raw:✓✓✓✓  staging:✓✓✓✓  curated:✓✓✓✓  archive:✓✓✓✓

✓ Chiffrement : raw=SSE-S3, staging=SSE-S3, curated=SSE-S3, archive=SSE-S3
✓ Aucun écart : les 48 droits observés correspondent à la matrice attendue.
```

Les objets témoins déposés pour ces tests sont préfixés `_verif_acl/` et
supprimés en fin d'exécution, y compris si la vérification échoue.

### Vérifier sans rien modifier

```bash
python minio_governance_setup.py --check
```

### Contrôles manuels

```bash
docker exec minio mc admin user ls local
docker exec minio mc admin policy ls local
docker exec minio mc admin policy info local data-analyst
docker exec minio mc encrypt info local/raw
```

---

## Étape 3 — Vérifier le chiffrement au repos

Le chiffrement est posé **au niveau du bucket** : le client n'a aucun en-tête à
fournir, MinIO chiffre de lui-même.

```bash
python - <<'PY'
import boto3
from botocore.client import Config
s3 = boto3.client("s3", endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin",
    config=Config(signature_version="s3v4"), region_name="us-east-1")
s3.put_object(Bucket="raw", Key="_test/sse.csv", Body=b"a,b\n1,2\n")
print(s3.head_object(Bucket="raw", Key="_test/sse.csv").get("ServerSideEncryption"))
s3.delete_object(Bucket="raw", Key="_test/sse.csv")
PY
# → AES256
```

`AES256` confirme un chiffrement SSE-S3, la clé étant gérée par le serveur.

---

## Étape 4 — Analyser une session d'accès

L'exécution de l'étape 2 a elle-même produit une session d'audit riche : 3
comptes tentant 48 opérations, dont 18 refusées.

```bash
python analyze_audit_logs.py --report JOUR7_AUDIT.md
```

Le script produit quatre lectures : par identité, par opération, par bucket, et
le détail des tentatives refusées, suivi d'une chronologie.

### Options

```bash
python analyze_audit_logs.py --denied-only          # uniquement les refus
python analyze_audit_logs.py --user data-analyst    # une identité
python analyze_audit_logs.py --bucket curated       # un bucket
python analyze_audit_logs.py --last 0               # chronologie complète
```

### Ce que la session montre

| Compte | Appels | Refus | Écritures |
|---|---:|---:|---:|
| `admin` | 16 | 0 | 8 |
| `data-analyst` | 16 | 14 | 0 |
| `data-engineer` | 16 | 4 | 6 |

`data-analyst` est refusé 14 fois sur 16 : c'est le résultat **attendu** d'un
compte dont le périmètre se limite à la lecture de `curated`. `data-engineer` est
refusé 4 fois, toutes sur `archive`. Aucune écriture n'aboutit pour l'analyste.

Un refus inattendu signalerait une policy trop étroite, bloquant un traitement
légitime ; un accès inattendu, une policy trop large, donc une exposition de
données. C'est cette lecture — et non le simple décompte — qui fait de l'audit un
outil de gouvernance.

Analyse détaillée et signaux à surveiller : [`JOUR7_AUDIT.md`](JOUR7_AUDIT.md) et
le §5 de [`JOUR6_GOUVERNANCE.md`](JOUR6_GOUVERNANCE.md).

---

## Points d'attention rencontrés

### 1. Les comptes MinIO sont hors du périmètre de boto3

Créer un utilisateur ou une policy relève de l'**API d'administration** MinIO, que
boto3 ne couvre pas — il ne parle que S3. Le script pilote donc le client `mc`
embarqué dans l'image MinIO via `docker exec`. Le document de policy est transmis
par l'entrée standard de `tee`, sans jamais atterrir sur le disque de l'hôte.

En revanche, la **vérification** des droits se fait bien en boto3 : c'est le
protocole que les vrais clients utiliseront.

### 2. Le listing est un droit de bucket, pas d'objet

`s3:ListBucket` porte sur l'ARN du bucket (`arn:aws:s3:::curated`), sans `/*`,
alors que `s3:GetObject` porte sur les objets (`arn:aws:s3:::curated/*`).
Confondre les deux produit une policy qui autorise la lecture d'un objet dont on
connaît la clé, mais interdit d'énumérer le bucket — un symptôme déroutant à
diagnostiquer.

### 3. `NoSuchKey` n'est pas un refus

En testant les droits, un `GetObject` sur une clé absente renvoie `NoSuchKey`, et
non `AccessDenied` : MinIO évalue l'autorisation **avant** l'existence de l'objet.
La fonction `_attempt` traite donc `NoSuchKey` comme une **autorisation**, sans
quoi la matrice sous-estimerait les droits en lecture.

### 4. MinIO sonde sa propre signature

Le journal contient des appels sur des buckets `probe-bsign-<aléa>` : ce sont des
sondes internes de MinIO, pas des buckets du data lake. `analyze_audit_logs.py`
les écarte, sinon le décompte par bucket serait faussé.

---

## Fichiers produits

```
minio_governance_setup.py    ← policies, comptes, SSE-S3, vérification des 48 droits
audit_collector.py           ← webhook receveur des logs d'audit MinIO
analyze_audit_logs.py        ← analyse de session d'accès
JOUR6_GOUVERNANCE.md         ← politique de gouvernance (livrable C21)
JOUR6_ACCESS_MATRIX.md       ← matrice des droits observés (généré)
JOUR7_AUDIT.md               ← analyse du journal d'audit (généré)
audit-logs/audit.jsonl       ← journal brut (git-ignoré)
```

Modifications de `docker-compose.yml` : `MINIO_KMS_SECRET_KEY`, les deux
variables du webhook d'audit, le service `audit-collector` et le `depends_on` de
`minio`.
