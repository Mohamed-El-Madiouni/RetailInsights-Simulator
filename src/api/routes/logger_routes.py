import logging
import os

# Configure le dossier et le fichier de logs
LOG_DIR = "src/logs"
LOG_FILE = "api_access.log"

os.makedirs(LOG_DIR, exist_ok=True)  # Crée le dossier s'il n'existe pas
log_path = os.path.join(LOG_DIR, LOG_FILE)

# Configuration du logger global
logger = logging.getLogger("api_logger")
handler = logging.FileHandler(log_path, encoding="utf-8")
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Empêche les doublons de logs
logger.propagate = False
