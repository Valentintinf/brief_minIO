# Contexte du projet
En tant que data engineer, concevoir l'architecture d'un data lake, déployer l'infrastructure de stockage objet, mettre en place les pipelines d'ingestion, cataloguer les données et implémenter les règles de gouvernance et de contrôle d'accès.

Leurs données sont aujourd'hui stockées en vrac, sans structure ni gouvernance. La DSI te confie la mission de concevoir et déployer un data lake moderne pour centraliser, documenter et sécuriser l'ensemble de ces flux, en vue d'un futur projet de maintenance prédictive.

Source de données : Synthetic Data from Industrial Sensor Monitoring — Polytechnic Institute of Porto / INESC TEC — Zenodo, avril 2025 (https://zenodo.org/records/15277168).

5 fichiers CSV représentant 5 lignes de production aux comportements distincts :

Line A stable (10 000 enregistrements)
Line B à flux moyen
Line C turbulente
Line D avec pics
Line E variable.
Point d'attention : les schémas diffèrent légèrement d'une ligne à l'autre (casse des colonnes, présence ou absence du champ elapsed_time). Cette hétérogénéité est volontaire et constitue une difficulté centrale du brief.

knkjn