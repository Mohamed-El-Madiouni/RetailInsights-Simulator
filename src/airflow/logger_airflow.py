import logging
import os

# Configurer le dossier et le fichier de logs pour Airflow
LOG_DIR = "src/logs"
LOG_FILE = "airflow_pipeline.log"

os.makedirs(LOG_DIR, exist_ok=True)  # Créer le dossier logs s'il n'existe pas
log_path = os.path.join(LOG_DIR, LOG_FILE)

# Configuration du logger spécifique pour Airflow
airflow_logger = logging.getLogger("airflow_logger")
handler = logging.FileHandler(log_path, encoding="utf-8")
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

airflow_logger.addHandler(handler)
airflow_logger.setLevel(logging.INFO)

# Empêcher la propagation des logs pour éviter les doublons
airflow_logger.propagate = False
