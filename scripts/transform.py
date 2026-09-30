"""
Aplati les données météo horaires brutes (une ligne par ville/heure) et
applique les transformations :
  1. Suppression des doublons (ville, heure)
  2. Gestion des valeurs manquantes (précipitation NULL -> 0.0, lignes
     avec température ou humidité manquante -> supprimées)
  3. Conversion de types (heure en datetime, valeurs en float)
  4. Création d'une colonne dérivée : ressenti simplifié
     (temperature_2m - un facteur lié à l'humidité)

Utilisable en standalone (python scripts/transform.py <fichier.json>) ou
depuis un PythonOperator Airflow.
"""

import glob
import json
import os
import sys
from datetime import datetime, timezone

import pandas as pd

DATA_RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
DATA_PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")


def latest_raw_file() -> str:
    """Trouve le fichier brut le plus récent dans data/raw/."""
    files = glob.glob(os.path.join(DATA_RAW_DIR, "weather_*.json"))
    if not files:
        raise FileNotFoundError("Aucun fichier weather_*.json trouvé dans data/raw/")
    return max(files, key=os.path.getmtime)


def flatten(raw_records: list) -> pd.DataFrame:
    """Transforme la liste de réponses API (une par ville) en DataFrame
    avec une ligne par (ville, heure)."""
    rows = []
    for city_data in raw_records:
        city = city_data["city"]
        hourly = city_data["hourly"]
        times = hourly["time"]
        for i, ts in enumerate(times):
            rows.append({
                "city": city,
                "time": ts,
                "temperature_2m": hourly["temperature_2m"][i],
                "relative_humidity_2m": hourly["relative_humidity_2m"][i],
                "precipitation": hourly["precipitation"][i],
                "wind_speed_10m": hourly["wind_speed_10m"][i],
            })
    return pd.DataFrame(rows)


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Applique les 3 transformations principales."""
    # 1. Doublons : une seule ligne par (ville, heure)
    df = df.drop_duplicates(subset=["city", "time"])

    # 2. Valeurs manquantes
    df["precipitation"] = df["precipitation"].fillna(0.0)
    df = df.dropna(subset=["temperature_2m", "relative_humidity_2m"])

    # 3. Conversion de types
    df["time"] = pd.to_datetime(df["time"])
    for col in ["temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m"]:
        df[col] = df[col].astype(float)

    # 4. Colonne dérivée : indice de ressenti simplifié
    df["feels_like_approx"] = (
        df["temperature_2m"] - (df["relative_humidity_2m"] / 100) * 2
    ).round(1)

    return df.reset_index(drop=True)


def transform_all(raw_path: str | None = None) -> str:
    """Charge un fichier brut, le transforme, et écrit le résultat en CSV.

    Retourne le chemin du fichier CSV écrit.
    """
    raw_path = raw_path or latest_raw_file()
    print(f"[transform] Lecture de {raw_path}")

    with open(raw_path, "r", encoding="utf-8") as f:
        raw_records = json.load(f)

    df = flatten(raw_records)
    df = clean(df)

    os.makedirs(DATA_PROCESSED_DIR, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    output_path = os.path.join(DATA_PROCESSED_DIR, f"weather_{timestamp}.csv")
    df.to_csv(output_path, index=False)

    print(f"[transform] {len(df)} lignes écrites dans {output_path}")
    return output_path


if __name__ == "__main__":
    arg_path = sys.argv[1] if len(sys.argv) > 1 else None
    transform_all(arg_path)
