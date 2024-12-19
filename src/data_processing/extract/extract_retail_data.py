from utils import create_output_folder, fetch_from_api
import pandas as pd
import os
import sys
from datetime import datetime


# Récupérer et sauvegarder les données retail_data
def fetch_and_save_retail_data(date):
    url = f"http://127.0.0.1:8000/retail_data?date={date}"
    new_data = fetch_from_api(url)  # Récupère les nouvelles données depuis l'API

    if new_data:
        output_folder = create_output_folder()
        output_file = os.path.join(output_folder, "retail_data.parquet")

        # Charger les données existantes, s'il y en a
        if os.path.exists(output_file):
            print(f"Chargement des données existantes depuis {output_file}.")
            existing_data = pd.read_parquet(output_file)
            # Ajouter les nouvelles données aux anciennes
            updated_data = pd.concat([existing_data, pd.DataFrame(new_data)], ignore_index=True)
        else:
            print("Aucune donnée existante. Création d'un nouveau fichier.")
            updated_data = pd.DataFrame(new_data)

        # Supprimer les doublons éventuels
        updated_data.drop_duplicates(inplace=True)

        # Sauvegarder les données mises à jour
        updated_data.to_parquet(output_file, engine="pyarrow", compression="snappy")
        print(f"Données mises à jour et sauvegardées dans {output_file}.")
    else:
        print(f"Aucune donnée récupérée pour la date {date}.")
        raise f"Aucune donnée récupérée pour la date {date}."


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
