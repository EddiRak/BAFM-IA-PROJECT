# -*- coding: utf-8 -*-
"""
DAG Airflow : exécute le pipeline 'src/etl/main_etl.py' chaque jour.
- Programmation : tous les jours à 03:00 (heure Antananarivo)
- Exécute la fonction run_all() du module main_etl
Prérequis :
  * Le scheduler/worker doit avoir accès au repo avec le dossier 'src'
  * Variable d'environnement PROJECT_SRC_DIR si besoin de forcer le chemin absolu vers 'src'
"""

import os
import sys
from datetime import timedelta, datetime
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

# --- Résolution du chemin vers 'src' pour les workers ---
# 1) Essayez d'utiliser une variable d'env si le projet est déployé hors du AIRFLOW_HOME
PROJECT_SRC_DIR = os.environ.get("PROJECT_SRC_DIR")  # ex: D:/Utilisateurs/Public/IA_BAFM/PROJECT/etl/src
if PROJECT_SRC_DIR and PROJECT_SRC_DIR not in sys.path:
    sys.path.insert(0, PROJECT_SRC_DIR)

# 2) Sinon, tentez un chemin relatif à AIRFLOW_HOME/dags/../src
HERE = os.path.abspath(os.path.dirname(__file__))
CANDIDATE_SRC = os.path.abspath(os.path.join(HERE, os.pardir, "src"))
if os.path.isdir(CANDIDATE_SRC) and (CANDIDATE_SRC not in sys.path):
    sys.path.insert(0, CANDIDATE_SRC)

# --- Import paresseux pour éviter les erreurs d'import au parsing DAG ---
def _run_main_etl():
    # On (re)insère éventuellement le src s'il vient d'être monté par le worker
    project_src = os.environ.get("PROJECT_SRC_DIR")
    if project_src and project_src not in sys.path:
        sys.path.insert(0, project_src)
    # Import tardif pour minimiser les soucis de parsing du DAG
    try:
        from etl.main_etl import run_all as _run_all
    except Exception:
        # Fallback si le package 'etl' n'est pas résolu dans ce contexte
        from src.etl.main_etl import run_all as _run_all  # si dags/ et src/ sont sœurs
    _run_all()


# --- Définition du DAG ---
local_tz = pendulum.timezone("Indian/Antananarivo")

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email": os.environ.get("ALERT_EMAIL", None),
    "email_on_failure": bool(os.environ.get("ALERT_EMAIL")),
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    dag_id="main_etl_daily",
    description="Orchestration quotidienne du pipeline ETL (SDP, AIR, MSC, CCN, OCC, GGSN)",
    default_args=default_args,
    start_date=datetime(2025, 9, 1, 3, 0, tzinfo=local_tz),
    schedule="0 3 * * *",  # tous les jours à 03:00 Antananarivo
    catchup=False,
    max_active_runs=1,
    tags=["etl", "batch", "cdr"],
) as dag:

    run_pipeline = PythonOperator(
        task_id="run_main_etl",
        python_callable=_run_main_etl,
        execution_timeout=timedelta(hours=6),  # à ajuster selon volumes
    )

    run_pipeline
