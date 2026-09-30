# Projet Final — Pipeline ETL avec Apache Airflow, Elasticsearch et Kibana

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

## Prérequis

- Docker Desktop (avec le backend WSL2 sous Windows)
- Au moins 8 Go de RAM alloués à Docker

## Installation et lancement

1. Cloner le dépôt :

   ```bash
   git clone <url-du-repo>
   cd Airflow_ELK
   ```
2. Copier le fichier d'environnement et ajuster les valeurs si besoin :

   ```bash
   cp .env.example .env
   ```
3. Construire les images et lancer les services :

   ```bash
   docker compose up -d --build
   ```
4. Vérifier que tout est démarré :

   ```bash
   docker compose ps
   ```
5. Accès aux interfaces :

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

## Transformations appliquées

## Contrôle qualité

## Mapping Elasticsearch

## Analyses et dashboard Kibana

## Démonstration
