import logging
import os

# Configurer le dossier et le fichier de logs pour Streamlit
LOG_DIR = "src/logs"
LOG_FILE = "user_activity.log"

os.makedirs(LOG_DIR, exist_ok=True)  # Créer le dossier logs s'il n'existe pas
log_path = os.path.join(LOG_DIR, LOG_FILE)

# Configuration du logger spécifique à Streamlit
streamlit_logger = logging.getLogger("streamlit_logger")
handler = logging.FileHandler(log_path, encoding="utf-8")
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

streamlit_logger.addHandler(handler)
streamlit_logger.setLevel(logging.INFO)

# Empêcher la propagation des logs pour éviter les doublons
streamlit_logger.propagate = False
