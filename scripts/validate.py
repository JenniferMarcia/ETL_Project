"""
Contrôle la qualité du CSV transformé avant chargement dans Elasticsearch :
  - nombre de lignes minimum attendu
  - champs obligatoires non nuls
  - types corrects
  - unicité de la clé (city, time)
  - plages de valeurs plausibles

Lève une exception si un contrôle critique échoue, ce qui fait échouer la
tâche Airflow correspondante (et donc bloque le chargement de données
invalides).

Utilisable en standalone (python scripts/validate.py <fichier.csv>) ou
depuis un PythonOperator Airflow.
"""

import glob
import os
import sys

import pandas as pd

DATA_PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")

REQUIRED_COLUMNS = [
    "city", "time", "temperature_2m", "relative_humidity_2m",
    "precipitation", "wind_speed_10m", "feels_like_approx",
]

MIN_ROWS = 1
PLAUSIBLE_RANGES = {
    "temperature_2m": (-60, 60),        # °C
    "relative_humidity_2m": (0, 100),   # %
    "precipitation": (0, 500),          # mm/h
    "wind_speed_10m": (0, 300),         # km/h
}


class ValidationError(Exception):
    pass


def latest_processed_file() -> str:
    files = glob.glob(os.path.join(DATA_PROCESSED_DIR, "weather_*.csv"))
    if not files:
        raise FileNotFoundError("Aucun fichier weather_*.csv trouvé dans data/processed/")
    return max(files, key=os.path.getmtime)


def validate(csv_path: str | None = None) -> dict:
    """Exécute tous les contrôles qualité sur le fichier donné.

    Retourne un résumé (dict) en cas de succès, lève ValidationError sinon.
    """
    csv_path = csv_path or latest_processed_file()
    print(f"[validate] Lecture de {csv_path}")

    df = pd.read_csv(csv_path, parse_dates=["time"])
    errors = []

    # 1. Colonnes obligatoires présentes
    missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_cols:
        errors.append(f"Colonnes manquantes : {missing_cols}")

    # 2. Nombre de lignes minimum
    if len(df) < MIN_ROWS:
        errors.append(f"Trop peu de lignes : {len(df)} (minimum {MIN_ROWS})")

    # 3. Champs obligatoires non nuls
    for col in REQUIRED_COLUMNS:
        if col in df.columns and df[col].isnull().any():
            n_null = df[col].isnull().sum()
            errors.append(f"{n_null} valeur(s) nulle(s) dans la colonne obligatoire '{col}'")

    # 4. Unicité de la clé (city, time)
    n_duplicates = df.duplicated(subset=["city", "time"]).sum()
    if n_duplicates > 0:
        errors.append(f"{n_duplicates} doublon(s) sur la clé (city, time)")

    # 5. Plages de valeurs plausibles
    for col, (low, high) in PLAUSIBLE_RANGES.items():
        if col not in df.columns:
            continue
        out_of_range = df[(df[col] < low) | (df[col] > high)]
        if not out_of_range.empty:
            errors.append(
                f"{len(out_of_range)} valeur(s) hors plage plausible pour '{col}' "
                f"(attendu [{low}, {high}])"
            )

    if errors:
        message = "Échec du contrôle qualité :\n- " + "\n- ".join(errors)
        raise ValidationError(message)

    summary = {
        "file": csv_path,
        "rows": len(df),
        "cities": sorted(df["city"].unique().tolist()),
        "time_range": [str(df["time"].min()), str(df["time"].max())],
    }
    print(f"[validate] OK — {summary}")
    return summary


if __name__ == "__main__":
    arg_path = sys.argv[1] if len(sys.argv) > 1 else None
    validate(arg_path)
