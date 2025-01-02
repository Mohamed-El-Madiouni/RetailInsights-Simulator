import os
import pandas as pd
import json
import sys
from datetime import datetime
from src.data_processing.extract.utils import fetch_from_api, save_to_s3, read_parquet_from_s3

# Paramètres S3
S3_FOLDER = "extracted_data/sales"


def fetch_stores():
    """
    Récupère la liste des magasins depuis le fichier stores.json.
    """
    stores = []
    file_name = os.path.join('data_api', 'stores.json')

    # Charger les magasins si le fichier existe
    if os.path.exists(file_name):
        with open(file_name, 'r', encoding='utf-8') as f:
            for line in json.load(f):
                if line['id'] not in stores:
                    stores.append(line['id'])
    else:
        print("Le fichier stores.json n'existe pas.")
        raise FileNotFoundError("Le fichier stores.json n'existe pas.")
    return stores


def fetch_and_save_sales(date):
    """
    Récupère et sauvegarde les données de ventes sur S3.
    """
    stores = fetch_stores()
    all_sales = []

    # Récupérer les données pour chaque magasin
    for store in stores:
        url = f"http://127.0.0.1:8000/sales?sale_date={date}&store_id={store}"
        data = fetch_from_api(url)
        if data:
            all_sales.extend(data)

    if all_sales:
        # Formater la date pour nommer le fichier
        month_str = pd.to_datetime(date).strftime("%Y-%m")
        s3_key = f"{S3_FOLDER}/sales_{month_str}.parquet"

        # Charger les données existantes depuis S3, s'il y en a
        try:
            existing_data = read_parquet_from_s3(s3_key)
            if existing_data is not None:
                # Ajouter les nouvelles données aux anciennes
                updated_data = pd.concat([existing_data, pd.DataFrame(all_sales)], ignore_index=True)
                updated_data.drop_duplicates(inplace=True)
            else:
                updated_data = pd.DataFrame(all_sales)
        except Exception as e:
            if "NoSuchKey" in str(e):
                print(f"Aucune donnée existante pour {s3_key}. Création d'un nouveau fichier.")
            else:
                print(f"Erreur inattendue lors de la tentative de lecture : {e}")
            updated_data = pd.DataFrame(all_sales)

        # Sauvegarder les données mises à jour sur S3
        save_to_s3(updated_data, s3_key)
    else:
        print(f"Aucune donnée de ventes récupérée pour la date {date}.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Vous devez renseigner une date en argument (format : YYYY-MM-DD)")
        sys.exit(1)

    date_param = sys.argv[1]

    try:
        # Valider le format de la date
        date_obj = datetime.strptime(date_param, "%Y-%m-%d")
        print(f"Date passée en paramètre : {date_obj.strftime('%Y-%m-%d')}")
        fetch_and_save_sales(date_param)
    except ValueError:
        print("Erreur : Le format de la date est incorrect. Utilisez 'YYYY-MM-DD'.")
        sys.exit(1)
