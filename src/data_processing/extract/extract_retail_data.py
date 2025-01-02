import pandas as pd
import sys
from datetime import datetime
from utils import fetch_from_api, save_to_s3, read_parquet_from_s3

# Paramètres S3
S3_FOLDER = "extracted_data/retail_data"


def fetch_and_save_retail_data(date):
    """
    Récupère et sauvegarde les données retail_data sur S3.
    """
    url = f"http://127.0.0.1:8000/retail_data?date={date}"
    new_data = fetch_from_api(url)  # Récupère les nouvelles données depuis l'API

    if new_data:
        # Formater la date pour nommer le fichier
        month_str = pd.to_datetime(date).strftime("%Y-%m")
        s3_key = f"{S3_FOLDER}/retail_data_{month_str}.parquet"

        # Charger les données existantes depuis S3, s'il y en a
        try:
            existing_data = read_parquet_from_s3(s3_key)
            if existing_data is not None:
                # Ajouter les nouvelles données aux anciennes
                updated_data = pd.concat([existing_data, pd.DataFrame(new_data)], ignore_index=True)
                updated_data.drop_duplicates(inplace=True)
            else:
                updated_data = pd.DataFrame(new_data)
        except Exception as e:
            print(f"Aucune donnée existante. Création d'un nouveau fichier : {e}")
            updated_data = pd.DataFrame(new_data)

        # Sauvegarder les données mises à jour sur S3
        save_to_s3(updated_data, s3_key)
    else:
        print(f"Aucune donnée récupérée pour la date {date}.")
        raise ValueError(f"Aucune donnée récupérée pour la date {date}.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Vous devez renseigner une date en argument (format : YYYY-MM-DD)")
        sys.exit(1)

    date_param = sys.argv[1]

    try:
        # Valider le format de la date
        date_obj = datetime.strptime(date_param, "%Y-%m-%d")
        print(f"Date passée en paramètre : {date_obj.strftime('%Y-%m-%d')}")
        fetch_and_save_retail_data(date_param)
    except ValueError:
        print("Erreur : Le format de la date est incorrect. Utilisez 'YYYY-MM-DD'.")
        sys.exit(1)
