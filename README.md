# Projet Final — Pipeline ETL avec Apache Airflow, Elasticsearch et Kibana

## Contexte et Objectif
Ce projet s'inscrit dans le cadre d'un cas pratique de Data Engineering visant à automatiser un pipeline ETL complet pour collecter, transformer, valider, stocker, analyser et visualiser des données météorologiques[cite: 4, 5]. L'orchestration est réalisée avec **Apache Airflow**, le stockage des données avec **Elasticsearch**, et la restitution sous forme de tableaux de bord interactifs avec **Kibana**.

## Architecture

```
API / CSV / JSON → Airflow (extract → transform → validate) → Elasticsearch → Kibana
```

Services Docker :

| Service           | Rôle                               | Port |
| ----------------- | ----------------------------------- | ---- |
| elasticsearch     | Stockage et indexation des données | 9200 |
| kibana            | Visualisation / dashboard           | 5601 |
| postgres          | Base de métadonnées Airflow       | -    |
| airflow-webserver | Interface web Airflow               | 8080 |
| airflow-scheduler | Orchestrateur des DAGs              | -    |

## Prérequis et Environnement
Le projet a été développé et testé sous **Windows** en utilisant **Docker Desktop** avec le backend **WSL2**[cite: 1, 4, 15].

- **OS** : Windows (avec WSL2 activé)
- **Outils** : Docker Desktop & Docker Compose
- **Configuration système (requis pour Elasticsearch)** : `vm.max_map_count=262144` configuré sur l'hôte
- **RAM** : Au moins 8 Go alloués à Docker

## Installation et lancement

1. Cloner le dépôt :

   ```bash
   git clone <url-du-repo>
   cd ETL_Project
   ```
2. Configurer le système hôte (sous PowerShell / WSL) pour le bon fonctionnement d'Elasticsearch :
   ```bash
      wsl -d docker-desktop sysctl -w vm.max_map_count=262144
   ```
3. Copier le fichier d'environnement et ajuster les valeurs si besoin :

   ```bash
   cp .env.example .env
   ```
4. Construire les images et lancer les services :

   ```bash
   docker compose up -d --build
   ```
5. Vérifier que tout est démarré :

   ```bash
   docker compose ps
   ```
6. Accès aux interfaces :

   - Airflow : http://localhost:8080 (identifiants définis dans `.env`)
   - Kibana : http://localhost:5601
   - Elasticsearch : http://localhost:9200

## Structure du dépôt

```
projet-data-engineering/
├── dags/                 # DAG(s) Airflow
├── scripts/              # extract.py, transform.py, validate.py, load.py
├── data/                 # données brutes / échantillons
├── elasticsearch/        # mapping.json, queries.json
├── kibana/               # dashboard.ndjson (export du dashboard)
├── docker-compose.yml
├── Dockerfile             # image Airflow avec les dépendances du projet
├── requirements.txt
├── .env.example
└── README.md
```

## Pipeline ETL

Le pipeline est orchestré par un DAG Airflow planifié qui exécute séquentiellement quatre tâches principales :
1. **Extract (`extract.py`)** : Récupération des données météorologiques brutes depuis la source (format JSON/CSV).
2. **Transform (`transform.py`)** : Nettoyage et application des transformations (gestion des valeurs manquantes, conversion de types, normalisation des dates, calculs dérivés).
3. **Validate (`validate.py`)** : Contrôle rigoureux de la qualité des données (vérification des champs obligatoires, des types et du comptage des lignes).
4. **Load (`load.py`)** : Chargement des données transformées dans l'index Elasticsearch (`weather_data`).

## Transformations appliquées

- **Suppression des doublons** et filtrage des enregistrements invalides.
- **Normalisation des formats temporels** pour une compatibilité optimale avec Elasticsearch.
- **Création de colonnes calculées** (telles que les indices de ressenti thermique).

## Contrôle qualité

La tâche de validation s'assure que :
- Le nombre de lignes récupérées et transformées est conforme aux attentes.
- Les champs obligatoires (température, humidité, vent) ne contiennent pas de valeurs nulles aberrantes.
- Les identifiants uniques de localisation sont intègres.

## Mapping Elasticsearch

Un mapping explicite (`elasticsearch/mapping.json`) est appliqué pour définir rigoureusement le type des champs (`keyword`, `float`, `date`, etc.), garantissant une indexation performante et sans erreurs de type.

## Analyses et dashboard Kibana

Le dashboard regroupe 4 visualisations interactives créées sous Kibana (via l'outil *Lens*) pour répondre aux exigences du sujet :
1. **Évolution de la température par ville** : Graphique en barres représentant les variations thermiques (Antananarivo, Paris, New York).
2. **Vitesse médiane du vent** : Indicateur métrique (*Metric*) affichant la valeur moyenne du vent (`wind_speed_10m`).
3. **Précipitations par heure** : Suivi quantitatif des précipitations par zone géographique.
4. **Humidité relative** : Analyse comparative de l'humidité dans le temps.

### Capture d'écran du Dashboard Kibana :
![Dashboard Kibana](./assets/Dashboard-kibana.png)

## Démonstration

Le projet intègre un mécanisme de **retries** en cas d'échec sur les tâches, des logs détaillés accessibles depuis l'interface Airflow, et l'utilisation de **XComs** pour le passage de métadonnées entre les scripts[cite: 5]. L'ensemble est entièrement reproductible en suivant les instructions d'installation Docker ci-dessus
