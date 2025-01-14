import sys
from datetime import datetime

import pandas as pd
from utils import fetch_from_api, read_parquet_from_s3, save_to_s3
from src.data_processing.extract.logger_extraction import extraction_logger

# Paramètres S3
S3_FOLDER = "extracted_data/retail_data"


def fetch_and_save_retail_data(date):
    """
    Récupère les données retail_data pour une date donnée depuis l'api,
    les combine avec les données existantes sur S3, et les sauvegarde.

    Args:
        date (str): La date pour laquelle récupérer les données, au format 'YYYY-MM-DD'.

    Raises:
        ValueError: Si aucune donnée n'est récupérée pour la date spécifiée.
    """
    url = f"http://127.0.0.1:8000/retail_data?date={date}"
    extraction_logger.info(f"Starting retail data extraction for date {date}.")

    try:
        new_data = fetch_from_api(url)  # Récupère les nouvelles données depuis l'api
        if new_data:
            extraction_logger.info(f"Successfully fetched retail data for date {date}, {len(new_data)} records.")

            # Formater la date pour nommer le fichier
            day_str = pd.to_datetime(date).strftime("%Y-%m-%d")
            month_str = pd.to_datetime(date).strftime("%Y-%m")
            s3_key = f"{S3_FOLDER}/retail_data_{month_str}/retail_data_{day_str}.parquet"

            # Charger les données existantes depuis S3, s'il y en a
            try:
                existing_data = read_parquet_from_s3(s3_key)
                if existing_data is not None:
                    # Ajouter les nouvelles données aux anciennes
                    updated_data = pd.concat(
                        [existing_data, pd.DataFrame(new_data)], ignore_index=True
                    )
                    updated_data.drop_duplicates(inplace=True)
                else:
                    updated_data = pd.DataFrame(new_data)
                extraction_logger.info(f"Existing data loaded and merged for date {date}.")
            except Exception as e:
                extraction_logger.warning(f"No existing data found for date {date}. Creating a new file: {e}")
                updated_data = pd.DataFrame(new_data)

            # Sauvegarder les données mises à jour sur S3
            save_to_s3(updated_data, s3_key)
            extraction_logger.info(f"Retail data successfully saved to S3 at '{s3_key}'.")
        else:
            extraction_logger.warning(f"No retail data retrieved for date {date}.")
            raise ValueError(f"Aucune donnée récupérée pour la date {date}.")
    except Exception as e:
        extraction_logger.error(f"Error during retail data extraction for date {date}: {e}")
        raise


# Point d'entrée pour exécuter la récupération et la sauvegarde des données retail_data.
# L'utilisateur doit fournir une date au format 'YYYY-MM-DD' en argument.
if __name__ == "__main__":
    if len(sys.argv) != 2:
        extraction_logger.error("Date parameter missing. Use format 'YYYY-MM-DD'.")
        print("Vous devez renseigner une date en argument (format : YYYY-MM-DD)")
        sys.exit(1)

    date_param = sys.argv[1]

    try:
        # Valider le format de la date
        date_obj = datetime.strptime(date_param, "%Y-%m-%d")
        extraction_logger.info(f"Received date parameter: {date_obj.strftime('%Y-%m-%d')}.")
        fetch_and_save_retail_data(date_param)
        extraction_logger.info(f"Retail data extraction process completed successfully for date {date_param}.")
    except ValueError:
        extraction_logger.error("Invalid date format. Use 'YYYY-MM-DD'.")
        print("Erreur : Le format de la date est incorrect. Utilisez 'YYYY-MM-DD'.")
        sys.exit(1)
    except Exception as e:
        extraction_logger.critical(f"Retail data extraction process failed: {e}")
        sys.exit(1)
