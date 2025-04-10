import json
import os
import sys
from datetime import datetime

import pandas as pd

from src.data_processing.extract.utils import (fetch_from_api,
                                               read_parquet_from_s3,
                                               save_to_s3)
from src.data_processing.extract.logger_extraction import extraction_logger

# Paramètres S3
S3_FOLDER = "extracted_data/sales"


def fetch_stores():
    """
    Récupère la liste des magasins à partir du fichier 'stores.json'.

    Returns:
        list: Liste des identifiants des magasins.

    Raises:
        FileNotFoundError: Si le fichier 'stores.json' n'existe pas.
    """
    stores = []
    file_name = os.path.join("data_api", "stores.json")

    # Charger les magasins si le fichier existe
    if os.path.exists(file_name):
        with open(file_name, "r", encoding="utf-8") as f:
            for line in json.load(f):
                if line["id"] not in stores:
                    stores.append(line["id"])
        extraction_logger.info(f"Successfully fetched {len(stores)} stores from 'stores.json'.")
    else:
        extraction_logger.error("File 'stores.json' not found.")
        raise FileNotFoundError("Le fichier stores.json n'existe pas.")
    return stores


def fetch_and_save_sales(date):
    """
    Récupère les données de ventes pour une date donnée et les sauvegarde sur S3.

    Args:
        date (str): La date pour laquelle récupérer les données, au format 'YYYY-MM-DD'.

    Raises:
        Exception: Si une erreur inattendue survient lors de la lecture ou de l'écriture sur S3.
    """
    extraction_logger.info(f"Starting sales data extraction for date {date}.")
    stores = fetch_stores()
    all_sales = []

    # Récupérer les données pour chaque magasin
    for store in stores:
        url = f"http://127.0.0.1:8000/sales?sale_date={date}&store_id={store}"
        try:
            data = fetch_from_api(url)
            if data:
                all_sales.extend(data)
                extraction_logger.info(f"Fetched {len(data)} sales for store {store}.")
        except Exception as e:
            extraction_logger.error(f"Error fetching sales for store {store}: {e}")
            continue

    if all_sales:
        # Formater la date pour nommer le fichier
        day_str = pd.to_datetime(date).strftime("%Y-%m-%d")
        s3_key = f"{S3_FOLDER}/sale_date={day_str}/sales_{day_str}.parquet"

        # Charger les données existantes depuis S3, s'il y en a
        try:
            existing_data = read_parquet_from_s3(s3_key)
            if existing_data is not None:
                # Ajouter les nouvelles données aux anciennes
                updated_data = pd.concat(
                    [existing_data, pd.DataFrame(all_sales)], ignore_index=True
                )
                updated_data.drop_duplicates(inplace=True)
            else:
                updated_data = pd.DataFrame(all_sales)
            extraction_logger.info(f"Existing data merged for date {date}.")
        except Exception as e:
            if "NoSuchKey" in str(e):
                extraction_logger.warning(f"No existing data found for {s3_key}. Creating a new file.")
            else:
                extraction_logger.error(f"Unexpected error while reading S3 data: {e}")
            updated_data = pd.DataFrame(all_sales)

        try:
            # Sauvegarder les données mises à jour sur S3
            save_to_s3(updated_data, s3_key)
            extraction_logger.info(f"Sales data successfully saved to S3 at '{s3_key}'.")
        except Exception as e:
            extraction_logger.error(f"Error saving sales data to S3: {e}")
            raise
    else:
        extraction_logger.warning(f"No sales data retrieved for date {date}.")


# Point d'entrée pour exécuter la récupération et la sauvegarde des données de ventes.
# L'utilisateur doit fournir une date en argument (format : 'YYYY-MM-DD').
if __name__ == "__main__":
    if len(sys.argv) < 2:
        extraction_logger.error("Date parameter missing. Use format 'YYYY-MM-DD'.")
        print("Vous devez renseigner une date en argument (format : YYYY-MM-DD)")
        sys.exit(1)

    date_param = sys.argv[1]

    try:
        # Valider le format de la date
        date_obj = datetime.strptime(date_param, "%Y-%m-%d")
        extraction_logger.info(f"Received date parameter: {date_obj.strftime('%Y-%m-%d')}.")
        fetch_and_save_sales(date_param)
        extraction_logger.info(f"Sales data extraction process completed successfully for date {date_param}.")
    except ValueError:
        extraction_logger.error("Invalid date format. Use 'YYYY-MM-DD'.")
        print("Erreur : Le format de la date est incorrect. Utilisez 'YYYY-MM-DD'.")
        sys.exit(1)
    except Exception as e:
        extraction_logger.critical(f"Sales data extraction process failed: {e}")
        sys.exit(1)
