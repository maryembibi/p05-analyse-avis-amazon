# P05 — Analyse distribuée des avis clients et des tendances

Projet Big Data — Mastère 1 IA \& DS, ISIMa, 2026/2027
**Mariem Bibi \& Mohamed Lassoued**

Chaîne Big Data sur le corpus **Amazon Reviews'23, catégorie Appliances** (McAuley Lab, UC San Diego) :
préparation contrôlée (bronze → silver), jobs **Hadoop MapReduce en Java**, Lakehouse **Apache Iceberg**, restitution.

## État d'avancement (06/11/2026)

|Étape|État|
|-|-|
|0. Téléchargement et profilage du jeu complet|✅|
|1. Préparation bronze → silver (lot de développement 5 %)|✅|
|2. Cluster Hadoop pseudo-distribué (Docker Compose, HDFS + YARN)|✅|
|3. Ingestion HDFS (zones bronze et silver)|✅|
|4. Jobs MapReduce MR1, MR2, MR3|prochaine étape|
|5. Tables Iceberg (silver, gold) et requêtes SQL|à venir|
|6. Restitution (tableau de bord)|à venir|
|7. Tests et mesures sur lots 5 %, 20 %, 100 %|à venir|

## Contenu du dépôt

|Fichier|Rôle|
|-|-|
|`explorer\_dataset.py`|Profilage du fichier brut en streaming : schéma, rejets, doublons, période, notes, produits|
|`preparer\_silver.py`|Préparateur bronze → silver : validation, dédoublonnage, NLP v1, anonymisation, lot déterministe par hash, manifeste|
|`docker-compose.yml`, `hadoop.env`|Cluster Hadoop 3.3.6 pseudo-distribué (namenode, datanode, resourcemanager, nodemanager)|
|`preuves/`|Sorties brutes des exécutions (profilage, préparation, cluster, job de test, ingestion HDFS)|

Les données (fichier brut et sorties silver) ne sont pas versionnées : volume et conditions d'usage du jeu.

## Reproduire (Windows, PowerShell, Python 3.8+, Docker Desktop)

```
# 1. Données (zone bronze)
curl.exe -O https://mcauleylab.ucsd.edu/public\_datasets/data/amazon\_2023/raw/review\_categories/Appliances.jsonl.gz

# 2. Profilage
py explorer\_dataset.py Appliances.jsonl.gz

# 3. Préparation silver (lot de développement : 5 % des produits)
py preparer\_silver.py Appliances.jsonl.gz --pourcentage 5

# 4. Cluster Hadoop
docker compose up -d
docker exec namenode hdfs dfsadmin -report
docker exec resourcemanager yarn node -list

# 5. Ingestion HDFS
docker exec namenode hdfs dfs -mkdir -p /p05/bronze /p05/silver
docker exec namenode hdfs dfs -put /projet/Appliances.jsonl.gz /projet/profil\_dataset.json /p05/bronze/
docker exec namenode hdfs dfs -put /projet/silver/<run\_id> /p05/silver/
```

Interfaces : HDFS http://localhost:9870 — YARN http://localhost:8088

## Résultats obtenus

**Profil du fichier complet** (2 128 605 lignes) : 0 ligne invalide, 22 657 doublons (1,1 %), 2 105 948 avis uniques,
94 319 produits, période 2000–2023, timestamp en millisecondes, 79,6 % de notes à 4–5 étoiles,
14 526 produits avec au moins 20 avis (85,6 % des avis).

**Lot de développement 5 %** (`lot5pct\_nlp-v1`) : 105 303 avis, 4 743 produits (N pour le TF-IDF),
1 923 280 tokens, vocabulaire de 31 605 termes, préparé en 54 s.

## Décisions principales

* Clé produit : `parent\_asin` ; document TF-IDF = ensemble des avis d'un produit
* Dédoublonnage : empreinte de `user\_id | parent\_asin | timestamp | texte` ; `user\_id` non conservé en silver
* Lots choisis par `md5(parent\_asin)` (5 %, 20 %, 100 %) : sélection stable, jamais les premières lignes
* NLP v1 sans deep learning : minuscules, HTML et URL retirés, stop words anglais, négations conservées
* Aucune statistique analytique dans la préparation : fréquences, TF-IDF et tendances sont calculés par MapReduce

## Source

Hou et al., *Bridging Language and Items for Retrieval and Recommendation*, 2024 — arXiv:2403.03952 —
https://amazon-reviews-2023.github.io/

