# Mémo démo, 15 min

Un point par ligne. À garder sous les yeux pendant le passage.

---

## Avant d'entrer (30 min avant)

- [ ] `docker-compose -f docker-compose.yml -f docker-compose.openmetadata.yml up -d`
- [ ] Attendre OpenMetadata : `until curl -sf http://localhost:8585/api/v1/system/version; do sleep 10; done`
- [ ] Vérifier que `raw/` et `staging/` sont peuplés (sinon rejouer les 2 DAGs, ça prend du temps)
- [ ] Onglets ouverts dans l'ordre : drawio SVG, MinIO 9001, Airflow 8080, OpenMetadata 8585
- [ ] MinIO et Airflow déjà connectés (ne pas taper de mot de passe en live)
- [ ] 2 terminaux prêts, police grossie
- [ ] Terminal 1 positionné sur `/home/valentin/brief/brief_minIO`
- [ ] Captures d'écran de secours dans un dossier, au cas où un service tombe

---

## 0:00 à 2:00, le problème

- Cinq lignes de production, données éparpillées, aucune gouvernance
- Objectif à terme : maintenance prédictive, donc historique long + mesures comparables + traçabilité
- Montrer les 5 CSV et leurs écarts : `head -1 data/*.csv`
- Pointer `Temperature` vs `temperature`, et `elapsed_time` absent sur C, D, E
- Dire : « c'est le vrai problème du brief, tout le reste en découle »

---

## 2:00 à 4:00, l'architecture

- Afficher `architecture_datalake.drawio.svg`
- Les 4 couches, une phrase chacune : raw = rejouable, staging = harmonisé, curated = métier, archive = rétention
- Justifier le dimensionnement : 1 min de pas, 1 440 points/jour/ligne, 7,9 M d'enregistrements sur 3 ans
- Dire : « j'ai dimensionné pour la trajectoire, pas pour les 2 Mo d'aujourd'hui »
- Justifier le partitionnement par ligne : pression à 158 hPa sur A, 96 hPa sur C, pas de seuil commun possible

---

## 4:00 à 7:00, ingestion et harmonisation

- Airflow 8080, montrer les 3 DAGs
- Ouvrir le graphe de `dag_raw_ingestion`, 5 tâches parallèles
- MinIO, bucket `raw`, descendre jusqu'à `production_lines/lineA/year=2025/month=05/`
- Montrer les 10 chunks de LineA
- Dire : « découpe en chunks de 1 000, c'est le schéma d'un vrai flux, un lot qui échoue ne casse pas les autres »
- MinIO, bucket `staging`, montrer les Parquet
- Dire la décision importante : `elapsed_time` nullable et jamais imputé, pour ne pas inventer une mesure
- Dire : `label` hors de {0,1} fait échouer la tâche, une tâche rouge se voit, une ligne supprimée en silence non

---

## 7:00 à 9:30, le catalogue

- OpenMetadata 8585, Explore puis Tables, les 5 fiches
- Ouvrir « Ligne C, régime turbulent »
- Montrer description, propriétaire, source Zenodo, fréquence, rétention P730D
- Onglet Schema, les 8 colonnes avec unité et plage
- Insister sur `label` : 0 nominal, 1 anomalie, c'est la variable cible du futur modèle
- Dire : « ces colonnes, je ne les ai pas déclarées, c'est l'ingestion qui a lu les Parquet et trouvé le même schéma 5 fois, ça prouve que l'harmonisation marche »
- Govern puis Glossaries, les 3 termes
- Mentionner le filtre qui exclut les chunks : le catalogue documente une ligne, pas un fichier

---

## 9:30 à 10:30, le cycle de vie

- MinIO, Buckets, raw, Lifecycle, montrer la règle
- Les 3 échéances : J+0 dépôt, J+180 archivage, J+730 suppression
- Dire pourquoi 2 mécanismes : l'API S3 ne sait pas déplacer entre buckets, `mc ilm tier add` reste bloqué
- Dire pourquoi `archive` expire à 550 et pas 730 : les objets y arrivent avec déjà 180 jours
- Dire : l'ILM sur raw et staging sert de filet si Airflow tombe

---

## 10:30 à 13:30, la gouvernance (le gros morceau)

- Terminal 1 : `python minio_governance_setup.py --check`
- Laisser tourner, commenter pendant
- Dire : « 48 essais réels, 3 comptes × 4 buckets × 4 opérations, en boto3 »
- Dire la phrase clé : « une policy acceptée par l'API ne prouve rien, ce qui compte c'est qu'un client se fasse vraiment refuser »
- Montrer la sortie : analyst bloqué partout sauf lecture sur curated
- Dire : SSE-S3 sur les 4 buckets, AES256, le client n'a rien à demander
- Terminal 2 : `python analyze_audit_logs.py --denied-only`
- Dire : 97 appels, 79 OK, 18 refus, et les 18 sont tous attendus
- Dire : « un refus inattendu = policy trop étroite, un accès inattendu = policy trop large, c'est ça qui fait de l'audit un outil de gouvernance »
- Si le temps manque, couper ici et aller à la conclusion

---

## 13:30 à 15:00, limites et fin

- Annoncer les limites sans attendre la question
- `curated` est vide, le bucket est configuré mais aucun pipeline n'écrit dedans
- Dire : « le brief demandait 2 DAGs, j'en ai livré 3, remplir curated demande des règles métier que personne n'a validées »
- Journal d'audit sur le même hôte que MinIO, sans rotation, sans alerte
- HTTP et pas TLS, à corriger avant toute production
- Clore sur : « une configuration qu'on n'a pas vérifiée n'est pas configurée, ça m'est arrivé 3 fois pendant le brief »

---

## Si ça plante

- Service HS : passer aux captures de secours, ne pas débugger en live
- OpenMetadata lent : c'est normal, prévenir et enchaîner, ne pas attendre devant l'écran
- Retard à 10:00 : sauter le catalogue en détail, garder gouvernance + limites
- Retard à 13:00 : couper l'audit, aller direct aux limites

---

## Questions probables

- Pourquoi MinIO et pas S3 ? API identique, le code est portable, déployable en local pour le brief
- Pourquoi Parquet ? types portés par le fichier, compression, lecture colonne sur historique long
- Pourquoi curated est vide ? arbitrage assumé, je préfère vide et documenté que rempli d'indicateurs non validés
- Pourquoi data-engineer n'a rien sur archive ? l'archivage est du cycle de vie, pas du pipeline, la restauration passe par l'admin
- Et si une ligne devait être cloisonnée ? le partitionnement est déjà là, il suffit de restreindre l'ARN de la policy, pas de migration
- Comment on passe à l'échelle ? le partitionnement est posé dès l'ingestion, c'est ce qui évite d'avoir à le refaire
