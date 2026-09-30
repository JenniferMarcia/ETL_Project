"""
etl_pipeline.py

DAG orchestrant le pipeline météo complet :
  extract -> transform -> validate -> load

Planifié pour tourner une fois par jour. Chaque tâche appelle directement
les fonctions des scripts dans scripts/ (import direct, pas de sous-
processus), ce qui permet d'utiliser XCom pour faire circuler les chemins
de fichiers et quelques métriques entre les tâches.
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# scripts/ est monté dans /opt/airflow/scripts et doit être importable
sys.path.insert(0, "/opt/airflow/scripts")

import extract as extract_module          # noqa: E402
import transform as transform_module      # noqa: E402
import validate as validate_module        # noqa: E402
import load as load_module                # noqa: E402


default_args = {
    "owner": "marcia",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


def task_extract(**context):
    raw_path = extract_module.extract_all()
    # Pousse le chemin du fichier brut aux tâches suivantes via XCom
    context["ti"].xcom_push(key="raw_path", value=raw_path)


def task_transform(**context):
    raw_path = context["ti"].xcom_pull(task_ids="extract", key="raw_path")
    processed_path = transform_module.transform_all(raw_path)
    context["ti"].xcom_push(key="processed_path", value=processed_path)


def task_validate(**context):
    processed_path = context["ti"].xcom_pull(task_ids="transform", key="processed_path")
    summary = validate_module.validate(processed_path)
    # Pousse un résumé consultable dans l'UI (onglet XCom de la tâche)
    context["ti"].xcom_push(key="validation_summary", value=summary)


def task_load(**context):
    processed_path = context["ti"].xcom_pull(task_ids="transform", key="processed_path")
    result = load_module.load_all(processed_path)
    context["ti"].xcom_push(key="load_result", value=result)


with DAG(
    dag_id="weather_etl_pipeline",
    description="Pipeline ETL météo : Open-Meteo -> Elasticsearch",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["etl", "weather", "elasticsearch"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=task_extract,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=task_transform,
    )

    validate = PythonOperator(
        task_id="validate",
        python_callable=task_validate,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=task_load,
    )

    extract >> transform >> validate >> load