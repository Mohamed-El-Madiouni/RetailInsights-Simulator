import logging
import os

# Configurer le dossier et le fichier de logs pour les transformations
LOG_DIR = "src/logs"
LOG_FILE = "transform_data.log"

os.makedirs(LOG_DIR, exist_ok=True)  # Créer le dossier logs s'il n'existe pas
log_path = os.path.join(LOG_DIR, LOG_FILE)

# Configuration du logger spécifique pour les transformations
transformation_logger = logging.getLogger("transformation_logger")
handler = logging.FileHandler(log_path, encoding="utf-8")
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

transformation_logger.addHandler(handler)
transformation_logger.setLevel(logging.INFO)

# Empêcher la propagation des logs pour éviter les doublons
transformation_logger.propagate = False
