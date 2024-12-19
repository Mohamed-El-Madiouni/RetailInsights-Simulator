from utils import create_output_folder, fetch_from_api
import pandas as pd
import os
import sys
from datetime import datetime
import json


# Fonction pour récupérer les magasins existants
def fetch_stores():
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


# Récupérer et sauvegarder les données de ventes
def fetch_and_save_sales(date):
    stores = fetch_stores()
    all_sales = []

    # Récupérer les données pour chaque magasin
    for store in stores:
        url = f"http://127.0.0.1:8000/sales?sale_date={date}&store_id={store}"
        data = fetch_from_api(url)
        if data:
            all_sales.extend(data)

    if all_sales:
        output_folder = create_output_folder()
        output_file = os.path.join(output_folder, "sales.parquet")

        # Charger les données existantes, s'il y en a
        if os.path.exists(output_file):
            print(f"Chargement des données existantes depuis {output_file}.")
            existing_data = pd.read_parquet(output_file)
            # Ajouter les nouvelles données aux anciennes
            updated_data = pd.concat([existing_data, pd.DataFrame(all_sales)], ignore_index=True)
        else:
            print("Aucune donnée existante. Création d'un nouveau fichier.")
            updated_data = pd.DataFrame(all_sales)

        # Supprimer les doublons éventuels
        updated_data.drop_duplicates(inplace=True)

        # Sauvegarder les données mises à jour
        updated_data.to_parquet(output_file, engine="pyarrow", compression="snappy")
        print(f"Données mises à jour et sauvegardées dans {output_file}.")
    else:
        print(f"Aucune donnée de ventes récupérée pour la date {date}.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
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
