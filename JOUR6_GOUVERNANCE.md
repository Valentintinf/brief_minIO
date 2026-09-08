# Politique de gouvernance du data lake industriel

**Périmètre** : data lake de données capteurs des 5 lignes de production instrumentées
(MinIO, buckets `raw`, `staging`, `curated`, `archive`).
**Compétence** : C21 — Gouvernance et contrôle d'accès.
**Version** : 1.0 — Jours 6-7.

---

## 1. Principes directeurs

**Moindre privilège.** Chaque compte reçoit exactement les droits nécessaires à sa
fonction, et rien de plus. Un besoin ponctuel ne justifie pas un élargissement
permanent de policy : il justifie une demande tracée.

**Séparation lecture / écriture.** Aucun compte d'analyse ne peut écrire. Aucun
compte d'écriture n'opère hors de son périmètre de couche.

**Traçabilité systématique.** Tout appel S3 est journalisé, autorisé ou refusé.
Les refus sont conservés au même titre que les succès : ils sont le signal
principal d'un incident d'accès.

**Chiffrement par défaut.** Le chiffrement au repos n'est pas une option laissée
au client : il est imposé au niveau du bucket.

**Sensibilité industrielle.** Ces mesures décrivent le comportement réel
d'équipements de production. Leur divulgation renseignerait un tiers sur les
cadences, les régimes de fonctionnement et les défaillances de l'usine. Elles
sont traitées comme des données industrielles confidentielles, non comme des
données ouvertes.

---

## 2. Qui accède à quoi

### 2.1 Comptes de service

| Compte | Fonction | Périmètre |
|---|---|---|
| `data-analyst` | Analyse métier, tableaux de bord | Lecture seule sur `curated` |
| `data-engineer` | Développement et exploitation des pipelines | Lecture/écriture sur `raw`, `staging`, `curated` |
| `admin` | Administration de la plateforme | Tous droits sur les 4 buckets, plus l'administration MinIO |
| `minioadmin` (root) | Compte racine | Bootstrap et secours uniquement — **pas d'usage courant** |

### 2.2 Matrice des droits

`L` = lister · `G` = lire un objet · `P` = écrire · `D` = supprimer

| Compte | `raw` | `staging` | `curated` | `archive` |
|---|:-:|:-:|:-:|:-:|
| `data-analyst` | — — — — | — — — — | **L G** — — | — — — — |
| `data-engineer` | **L G P D** | **L G P D** | **L G P D** | — — — — |
| `admin` | **L G P D** | **L G P D** | **L G P D** | **L G P D** |

Cette matrice n'est pas déclarative : elle est **vérifiée empiriquement** par
`minio_governance_setup.py`, qui tente les 4 opérations avec chacun des 3 comptes
sur chacun des 4 buckets et échoue au moindre écart. Résultat de la dernière
exécution dans [`JOUR6_ACCESS_MATRIX.md`](JOUR6_ACCESS_MATRIX.md).

### 2.3 Accès par ligne de production

Les policies MinIO portent sur des préfixes d'objets, non sur le contenu. Le
partitionnement `production_lines/<line>/` permettrait de restreindre un compte à
une ligne précise, en remplaçant l'ARN `arn:aws:s3:::curated/*` par
`arn:aws:s3:::curated/production_lines/lineC/*`.

**Choix retenu** : aucun cloisonnement par ligne à ce stade. Les cinq lignes
appartiennent au même périmètre métier, sous la responsabilité d'un même service.
Un cloisonnement deviendrait nécessaire si des lignes étaient exploitées par des
prestataires distincts, ou si une ligne relevait d'un contrat de
confidentialité spécifique. Le partitionnement est en place, la restriction est
donc activable sans migration de données.

### 2.4 Accès humains

Les comptes ci-dessus sont des **comptes de service**, destinés aux traitements
automatisés et aux outils. Les accès humains à la console MinIO passent par des
comptes nominatifs distincts, à créer selon le même principe de moindre
privilège. Aucun compte de service n'est partagé entre plusieurs personnes : un
identifiant partagé rend la traçabilité de l'audit inexploitable.

---

## 3. Sous quelles conditions

### 3.1 Conditions d'octroi

| Condition | Règle |
|---|---|
| Demande | Formulée auprès du responsable maintenance, propriétaire métier des données |
| Justification | Usage précis et durée ; un besoin ponctuel donne un accès temporaire |
| Attribution | Rattachement à une policy existante ; la création d'une policy nouvelle est un acte d'architecture, pas une opération courante |
| Revue | Trimestrielle — tout compte sans appel S3 sur la période est désactivé |
| Départ | Révocation le jour même du départ ou du changement de fonction |

### 3.2 Conditions techniques

**Chiffrement au repos.** SSE-S3 est actif sur les 4 buckets. Tout objet déposé
est chiffré par MinIO, quel que soit le client, sans en-tête particulier à
fournir. La clé maîtresse est portée par le KMS interne de MinIO
(`MINIO_KMS_SECRET_KEY`).

> **Avertissement** : la perte de cette clé rend les objets chiffrés
> définitivement illisibles. Elle doit être sauvegardée hors du dépôt Git et hors
> du serveur MinIO. La clé présente dans `docker-compose.yml` est une clé de
> démonstration, à remplacer pour tout usage réel.

**Chiffrement en transit.** L'installation actuelle est en HTTP, acceptable pour
un déploiement local. Une mise en production exige TLS sur MinIO, sans quoi les
identifiants et les données circulent en clair.

**Rotation des secrets.** Les secrets des comptes de service sont surchargeables
par variables d'environnement (`MINIO_ANALYST_SECRET`, `MINIO_ENGINEER_SECRET`,
`MINIO_ADMIN_SECRET`). Une relance de `minio_governance_setup.py` réaligne le
secret du compte sur la valeur fournie : la rotation ne demande ni suppression ni
recréation de compte.

**Cycle de vie.** Les données sont archivées à 180 jours et supprimées à 730
jours. Aucune donnée n'est conservée indéfiniment. Détail dans
[`JOUR5_ILM_POLICY.md`](JOUR5_ILM_POLICY.md).

### 3.3 Cas particulier — le DAG d'archivage

`dag_ilm_archive` écrit dans `archive`, bucket sur lequel `data-engineer` n'a
aucun droit. C'est délibéré : l'archivage est une opération de **cycle de vie**,
pas de pipeline de données. Le DAG s'exécute donc avec des identifiants
privilégiés, distincts de ceux du rôle `data-engineer`.

La conséquence est assumée : un ingénieur de données ne peut ni relire ni
restaurer une donnée archivée par lui-même, il doit passer par l'administrateur.
C'est le prix de l'immuabilité de la couche d'archive — et sa justification.

---

## 4. Responsabilités par rôle

### Responsable maintenance — propriétaire métier

- Valide les plages nominales et la signification des colonnes ;
- arbitre les demandes d'accès aux données de son périmètre ;
- qualifie les anomalies remontées par les modèles ;
- est identifié comme `owner` des 5 fiches dans OpenMetadata.

### Data engineer

- Exploite les pipelines d'ingestion et de transformation ;
- garantit l'intégrité des données déposées (contrôle MD5, validation du domaine
  de `label`) ;
- maintient à jour les fiches du catalogue quand un schéma évolue ;
- **n'accorde aucun droit d'accès** : ce n'est pas son rôle.

### Data analyst

- Consomme la couche `curated` en lecture seule ;
- ne duplique pas les données hors du data lake sans accord du propriétaire —
  une copie échappe à l'audit, au chiffrement et aux règles de cycle de vie ;
- signale toute incohérence au propriétaire métier.

### Administrateur de la plateforme

- Crée, modifie et révoque les comptes et les policies ;
- garantit l'activation du chiffrement et de l'audit ;
- conduit la revue trimestrielle des accès ;
- sauvegarde la clé KMS hors de la plateforme ;
- est seul habilité sur le bucket `archive`.

### Matrice RACI

`R` = réalise · `A` = approuve · `C` = consulté · `I` = informé

| Activité | Propriétaire métier | Data engineer | Data analyst | Administrateur |
|---|:-:|:-:|:-:|:-:|
| Octroi d'un accès | A | I | — | R |
| Révocation d'un accès | I | I | — | R |
| Création d'une policy | C | C | — | R |
| Évolution d'un schéma | A | R | C | I |
| Documentation d'un dataset | A | R | C | — |
| Revue trimestrielle des accès | C | I | I | R |
| Rotation des secrets | I | I | I | R |
| Restauration d'une donnée archivée | A | C | — | R |
| Analyse d'un incident d'accès | I | C | — | R |

---

## 5. Traçabilité et détection

### 5.1 Dispositif

MinIO POSTe chaque appel S3 sur un webhook HTTP ; le service `audit-collector`
écrit ces événements en JSON Lines. Chaque entrée porte l'identité appelante,
l'opération, la cible, le code de retour, l'IP source et le client utilisé.

Analyse : `python analyze_audit_logs.py`. Résultat de la dernière session dans
[`JOUR7_AUDIT.md`](JOUR7_AUDIT.md).

### 5.2 Signaux à surveiller

| Signal | Interprétation | Action |
|---|---|---|
| Rafale de 403 sur un compte | identifiants compromis, ou pipeline mal configuré | Vérifier l'origine, faire tourner le secret si l'IP est inconnue |
| Écriture réussie par `data-analyst` | policy trop large | Corriger la policy, rejouer la vérification |
| `GetObject` massif sur `curated` | extraction anormale de données | Confronter au besoin métier déclaré |
| `remotehost` inhabituel | accès depuis un poste non prévu | Identifier le poste, révoquer si non légitime |
| `DeleteObject` hors DAG d'archivage | suppression non planifiée | Investiguer, restaurer depuis `archive` si nécessaire |
| Usage du compte root `minioadmin` | contournement des comptes de service | Identifier la cause, corriger l'outil fautif |

### 5.3 Limites assumées du dispositif

Trois limites de l'installation actuelle, à lever avant tout usage réel.

1. **Journal non répliqué.** Les logs vivent dans `audit-logs/`, sur le même
   hôte que MinIO. Un incident sur cet hôte emporte la donnée et sa trace. Un
   journal d'audit doit être expédié hors de la plateforme qu'il surveille.
2. **Journal sans rétention.** `audit.jsonl` croît sans limite et n'est jamais
   purgé. Il faut une rotation, et une durée de conservation alignée sur
   l'obligation applicable.
3. **Aucune alerte.** L'analyse est manuelle, à la demande. Les signaux du §5.2
   ne déclenchent aujourd'hui aucune notification automatique.

---

## 6. Vérifier l'application de cette politique

```bash
# Droits réels des 3 comptes + état du chiffrement
python minio_governance_setup.py --check

# Analyse du journal d'audit
python analyze_audit_logs.py

# Règles de cycle de vie
python minio_ilm_setup.py --check
```

Les trois commandes sont en lecture seule et peuvent être lancées à tout moment.
