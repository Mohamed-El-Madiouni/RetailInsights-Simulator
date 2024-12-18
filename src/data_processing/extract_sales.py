import sys
from datetime import datetime

import requests
import os
import json


# Fonction pour retrouver tous les magasins existants
def fetch_stores():
    stores = []
    file_name = os.path.join('../../data', 'stores.json')

    # Charger les magasins si le fichier existe
    if os.path.exists(file_name):
        with open(file_name, 'r', encoding='utf-8') as f:
            for line in json.load(f):
                if line['id'] not in stores:
                    stores.append(line['id'])
    else:
        print("File dont exists")
    return stores


# Fonction pour requêter l'API
def fetch_sales(date):
    stores = fetch_stores()
    for store in stores:
        url = f"http://127.0.0.1:8000/sales?sale_date={date}&store_id={store}"
        response = requests.get(url)
        if response.status_code == 200:
            print(f"Données de ventes récupérées de l'API pour la ville {store}.")
            print(response.json())
        else:
            print(f"Erreur lors de la récupération des données de ventes de l'API: {response.status_code}")
            return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Vous devez renseigner une date en argument")
        sys.exit(1)

    date_param = sys.argv[1]

    try:
        date_obj = datetime.strptime(date_param, "%Y-%m-%d")
        print(f"Date passée en paramètre : {date_obj.strftime('%Y-%m-%d')}")
        fetch_sales(date_param)
    except ValueError:
        print("Erreur: Le format de la date est incorrect. Utilisez 'YYYY-MM-DD'.")
        sys.exit(1)
