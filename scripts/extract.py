"""
Récupère les prévisions météo horaires pour un ensemble de villes via
l'API publique Open-Meteo (https://open-meteo.com/), et sauvegarde le
résultat brut en JSON dans data/raw/.

Utilisable en standalone (python scripts/extract.py) ou depuis un
PythonOperator Airflow.
"""

import json
import os
from datetime import datetime, timezone

import requests

# Villes ciblées : nom -> (latitude, longitude)
CITIES = {
    "antananarivo": (-18.8792, 47.5079),
    "tokyo": (35.6762, 139.6503),
    "seoul": (37.5665, 126.9780),
    "paris": (48.8566, 2.3522),
    "new_york": (40.7128, -74.0060),
}

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
]

API_URL = "https://api.open-meteo.com/v1/forecast"

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")


def fetch_city_weather(city_name: str, lat: float, lon: float) -> dict:
    """Appelle l'API Open-Meteo pour une ville et renvoie le JSON brut."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": "auto",
        "forecast_days": 1,
    }
    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    data["city"] = city_name
    return data


def extract_all() -> str:
    """Récupère les données pour toutes les villes et les écrit sur disque.

    Retourne le chemin du fichier JSON écrit.
    """
    os.makedirs(DATA_DIR, exist_ok=True)

    results = []
    for city_name, (lat, lon) in CITIES.items():
        print(f"[extract] Récupération météo pour {city_name}...")
        results.append(fetch_city_weather(city_name, lat, lon))

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    output_path = os.path.join(DATA_DIR, f"weather_{timestamp}.json")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"[extract] {len(results)} villes écrites dans {output_path}")
    return output_path


if __name__ == "__main__":
    extract_all()
