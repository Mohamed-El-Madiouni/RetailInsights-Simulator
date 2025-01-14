import os
import signal
import subprocess
import time

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from src.airflow.logger_airflow import airflow_logger

# Variables globales
API_PROCESS = None


def start_api():
    """
    Démarre l'api avec uvicorn en arrière-plan.

    Exceptions:
        RuntimeError: Si l'api ne peut pas être démarrée.
    """
    global API_PROCESS
    try:
        airflow_logger.info("Démarrage de l'API avec uvicorn.")
        API_PROCESS = subprocess.Popen(
            ["uvicorn", "src.api.main:app", "--reload"],
            cwd="/home/ubuntu/RetailInsights-Simulator",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid,  # Permet de tuer le processus facilement
        )
        time.sleep(5)  # Attendre que l'api soit prête
        airflow_logger.info("API démarrée avec succès.")
    except Exception as e:
        airflow_logger.error(f"Erreur lors du démarrage de l'API : {e}")
        raise RuntimeError("Échec du démarrage de l'API.")


def stop_api():
    """
    Arrête l'api en tuant le processus uvicorn.
    """
    global API_PROCESS
    try:
        if API_PROCESS:
            os.killpg(os.getpgid(API_PROCESS.pid), signal.SIGTERM)
            airflow_logger.info("API arrêtée avec succès.")
    except Exception as e:
        airflow_logger.error(f"Erreur lors de l'arrêt de l'API : {e}")


# Configuration du DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# Le DAG est planifié pour s'exécuter tous les jours à 11h50 (UTC).
with DAG(
    dag_id="full_pipeline_dag",
    default_args=default_args,
    description="DAG complet pour RetailInsights-Simulator",
    schedule_interval="50 11 * * *",
    start_date=days_ago(1),
) as dag:
    # Tâche 1 : Démarrer l'api
    start_api_task = PythonOperator(
        task_id="start_api",
        python_callable=start_api,
    )

    # Tâche 2 : Générer les données retail
    generate_data = BashOperator(
        task_id="generate_retail_data",
        bash_command="source ~/airflow_env/venv/bin/activate && "
        "cd ~/RetailInsights-Simulator && "
        "python src/api/retail_data_generator.py",
        on_success_callback=lambda context: airflow_logger.info(
            "Tâche generate_retail_data terminée avec succès."
        ),
        on_failure_callback=lambda context: airflow_logger.error(
            "Erreur lors de la tâche generate_retail_data."
        ),
    )

    # Tâche 3 : Extraire les ventes
    extract_sales = BashOperator(
        task_id="extract_sales",
        bash_command="source ~/airflow_env/venv/bin/activate && "
        "cd ~/RetailInsights-Simulator && "
        "python src/data_processing/extract/extract_sales.py",
        on_success_callback=lambda context: airflow_logger.info(
            "Tâche extract_sales terminée avec succès."
        ),
        on_failure_callback=lambda context: airflow_logger.error(
            "Erreur lors de la tâche extract_sales."
        ),
    )

    # Tâche 4 : Extraire les données retail
    extract_retail_data = BashOperator(
        task_id="extract_retail_data",
        bash_command="source ~/airflow_env/venv/bin/activate && "
        "cd ~/RetailInsights-Simulator && "
        "python src/data_processing/extract/extract_retail_data.py",
        on_success_callback=lambda context: airflow_logger.info(
            "Tâche extract_retail_data terminée avec succès."
        ),
        on_failure_callback=lambda context: airflow_logger.error(
            "Erreur lors de la tâche extract_retail_data."
        ),
    )

    # Tâche 5 : Supprimer les fichiers temporaires pour libérer de l'espace et éviter les conflits
    # lors de la prochaine exécution.
    cleanup_files = BashOperator(
        task_id="cleanup_files",
        bash_command="rm -rf ~/RetailInsights-Simulator/data_api/sales.json "
        "~/RetailInsights-Simulator/data_api/retail_data.json",
        on_success_callback=lambda context: airflow_logger.info(
            "Tâche cleanup_files terminée avec succès."
        ),
        on_failure_callback=lambda context: airflow_logger.error(
            "Erreur lors de la tâche cleanup_files."
        ),
    )

    # Tâche 6 : Agréger les métriques journalières
    aggregate_metrics = BashOperator(
        task_id="aggregate_metrics",
        bash_command="source ~/airflow_env/venv/bin/activate && "
        "cd ~/RetailInsights-Simulator && "
        "python src/data_processing/transform/aggregate_daily_metrics.py",
        on_success_callback=lambda context: airflow_logger.info(
            "Tâche aggregate_metrics terminée avec succès."
        ),
        on_failure_callback=lambda context: airflow_logger.error(
            "Erreur lors de la tâche aggregate_metrics."
        ),
    )

    # Tâche 7 : Arrêter l'api
    stop_api_task = PythonOperator(
        task_id="stop_api",
        python_callable=stop_api,
    )

    # Définir l'ordre des tâches
    (
        start_api_task
        >> generate_data
        >> [extract_sales, extract_retail_data]
        >> cleanup_files
        >> aggregate_metrics
        >> stop_api_task
    )
