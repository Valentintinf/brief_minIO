# Politique ILM MinIO — Jour 5 (C20)

Généré par `minio_ilm_setup.py`, à partir de la configuration relue sur MinIO.

## Horizon de rétention

| Échéance | Événement | Mécanisme |
|---|---|---|
| J+0 | dépôt dans `raw/` puis `staging/` | DAGs d'ingestion et de transformation |
| J+180 | déplacement vers `archive/` | DAG `dag_ilm_archive` |
| J+730 | suppression définitive | règle ILM MinIO |

## Règles enregistrées sur MinIO

| Bucket | ID de règle | Statut | Effet |
|---|---|---|---|
| `raw` | `expire-raw-2-years` | Enabled | suppression à 730 jours |
| `staging` | `expire-staging-2-years` | Enabled | suppression à 730 jours |
| `curated` | `expire-curated-2-years` | Enabled | suppression à 730 jours |
| `archive` | `expire-archive-end-of-retention` | Enabled | suppression à 550 jours |

## Vérification

```bash
# Via le client MinIO, pour chaque bucket
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec minio mc ilm rule ls local/raw

# Via boto3
python minio_ilm_setup.py --check
```

Dans la console MinIO : **Buckets → <bucket> → Lifecycle**.

## Pourquoi deux mécanismes

L'API S3 de cycle de vie n'expose que deux actions : expirer un objet, ou le
transitionner vers une classe de stockage. Elle ne sait pas le déplacer vers un
autre bucket. MinIO permet bien une transition vers un « remote tier », mais
celui-ci doit être un déploiement MinIO ou S3 **distinct** ; le pointer vers le
bucket `archive` du même serveur n'est pas supporté et fait échouer la commande
`mc ilm tier add`.

L'archivage à 180 jours est donc réalisé par le DAG `dag_ilm_archive`, et l'ILM
native assure la suppression définitive à 2 ans.

## Uploads multipart incomplets

MinIO refuse une règle ILM qui ne porterait qu'un `AbortIncompleteMultipartUpload`,
et ignore silencieusement cette action lorsqu'elle accompagne une expiration :
elle n'apparaît pas dans la configuration relue. Chez MinIO, la purge des
uploads interrompus est un réglage serveur, pas une règle de bucket :

```bash
docker exec minio mc admin config get local api | tr ' ' '\n' | grep stale
# stale_uploads_cleanup_interval=6h
# stale_uploads_expiry=24h
```
