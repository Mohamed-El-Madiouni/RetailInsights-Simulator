import logging
import os

# Configurer le dossier et le fichier de logs pour la génération
LOG_DIR = "src/logs"
LOG_FILE = "generation.log"

os.makedirs(LOG_DIR, exist_ok=True)  # Créer le dossier logs s'il n'existe pas
log_path = os.path.join(LOG_DIR, LOG_FILE)

# Configuration du logger spécifique pour la génération
generation_logger = logging.getLogger("generation_logger")
handler = logging.FileHandler(log_path, encoding="utf-8")
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

generation_logger.addHandler(handler)
generation_logger.setLevel(logging.INFO)

# Empêche les doublons de logs
generation_logger.propagate = False
