"""
load.py

Crée l'index Elasticsearch 'weather_data' (avec mapping explicite) s'il
n'existe pas, puis charge en bulk le CSV transformé/validé.

Utilise la connexion Airflow 'elasticsearch_default' (créée dans
airflow-init via `airflow connections add`) pour récupérer host/port,
au lieu de coder l'adresse en dur.

Utilisable en standalone (python scripts/load.py <fichier.csv>) ou depuis
un PythonOperator Airflow.
"""

import glob
import json
import os
import sys

import pandas as pd
from elasticsearch import Elasticsearch, helpers

DATA_PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")
MAPPING_PATH = os.path.join(os.path.dirname(__file__), "..", "elasticsearch", "mapping.json")

INDEX_NAME = "weather_data"


def latest_processed_file() -> str:
    files = glob.glob(os.path.join(DATA_PROCESSED_DIR, "weather_*.csv"))
    if not files:
        raise FileNotFoundError("Aucun fichier weather_*.csv trouvé dans data/processed/")
    return max(files, key=os.path.getmtime)


def get_es_client() -> Elasticsearch:
    """Construit le client ES à partir de la connexion Airflow si
    disponible, sinon retombe sur des valeurs par défaut (utile en
    exécution standalone hors Airflow)."""
    try:
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection("elasticsearch_default")
        host = conn.host or "elasticsearch"
        port = conn.port or 9200
        scheme = conn.schema or "http"
    except Exception:
        host, port, scheme = "elasticsearch", 9200, "http"

    url = f"{scheme}://{host}:{port}"
    print(f"[load] Connexion à Elasticsearch : {url}")
    return Elasticsearch(url)


def ensure_index(es: Elasticsearch) -> None:
    """Crée l'index avec son mapping s'il n'existe pas déjà."""
    if es.indices.exists(index=INDEX_NAME):
        print(f"[load] Index '{INDEX_NAME}' déjà existant, pas de recréation")
        return

    with open(MAPPING_PATH, "r", encoding="utf-8") as f:
        mapping = json.load(f)

    es.indices.create(index=INDEX_NAME, body=mapping)
    print(f"[load] Index '{INDEX_NAME}' créé avec le mapping de {MAPPING_PATH}")


def build_actions(df: pd.DataFrame):
    """Génère les actions bulk pour le client ES.

    L'_id est déterministe (city + time) pour que relancer le chargement
    sur les mêmes données mette à jour plutôt que dupliquer.
    """
    for _, row in df.iterrows():
        doc = row.to_dict()
        doc_id = f"{doc['city']}_{doc['time']}"
        yield {
            "_index": INDEX_NAME,
            "_id": doc_id,
            "_source": doc,
        }


def load_all(csv_path: str | None = None) -> dict:
    csv_path = csv_path or latest_processed_file()
    print(f"[load] Lecture de {csv_path}")

    df = pd.read_csv(csv_path, parse_dates=["time"])
    df["time"] = df["time"].dt.strftime("%Y-%m-%dT%H:%M:%S")

    es = get_es_client()
    ensure_index(es)

    success, errors = helpers.bulk(es, build_actions(df), raise_on_error=False)

    summary = {"file": csv_path, "indexed": success, "errors": len(errors)}
    print(f"[load] {summary}")

    if errors:
        raise RuntimeError(f"{len(errors)} document(s) en erreur lors du chargement : {errors[:3]}")

    return summary


if __name__ == "__main__":
    arg_path = sys.argv[1] if len(sys.argv) > 1 else None
    load_all(arg_path)
