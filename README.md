# Data Lake industriel — MinIO

Projet fil rouge : concevoir et déployer un data lake moderne pour centraliser, documenter et sécuriser des données issues de lignes de production instrumentées, en préparation d'un futur cas d'usage de maintenance prédictive.

## Données

Source : Synthetic Data from Industrial Sensor Monitoring — Polytechnic Institute of Porto / INESC TEC — Zenodo, avril 2025.

Les fichiers CSV sont disponibles dans `data/` :

- `LineA_Stable_10K.csv` — ligne A stable, 10 000 enregistrements ;
- `LineB_Flux.csv` — ligne B à flux moyen, 5 000 enregistrements ;
- `LineC_Turbulent.csv` — ligne C turbulente, 5 000 enregistrements ;
- `LineD_SpikeControl.csv` — ligne D avec pics, 5 000 enregistrements ;
- `LineE_SmoothRun.csv` — ligne E variable/lissée, 5 000 enregistrements.

Point d'attention : les schémas diffèrent volontairement selon les lignes, notamment sur la casse des colonnes (`Temperature` / `temperature`, `Pressure` / `pressure`) et la présence ou absence de `elapsed_time`.

## Livrables

- Jour 1 — Architecture : `JOUR1_ARCHITECTURE.md`
- Diagramme draw.io : `architecture_datalake.drawio`


## Architecture cible

```text
raw/      données brutes inchangées
staging/  données harmonisées et contrôlées
curated/  données prêtes à l'analyse
archive/  données expirées selon règles ILM
```

Le détail des choix techniques, du partitionnement et des règles de gouvernance prévues est documenté dans `JOUR1_ARCHITECTURE.md`.
