import logging
import os

# Configurer le dossier et le fichier de logs pour les extractions
LOG_DIR = "src/logs"
LOG_FILE = "extract_data.log"

os.makedirs(LOG_DIR, exist_ok=True)  # Créer le dossier logs s'il n'existe pas
log_path = os.path.join(LOG_DIR, LOG_FILE)

# Configuration du logger spécifique pour les extractions
extraction_logger = logging.getLogger("extraction_logger")
handler = logging.FileHandler(log_path, encoding="utf-8")
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

extraction_logger.addHandler(handler)
extraction_logger.setLevel(logging.INFO)

# Empêcher la propagation des logs pour éviter les doublons
extraction_logger.propagate = False
