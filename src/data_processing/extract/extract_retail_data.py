import sys
import requests
from datetime import datetime


# Fonction pour requêter l'API
def fetch_retail_data(date):
    url = f"http://127.0.0.1:8000/retail_data?date={date}"
    response = requests.get(url)
    if response.status_code == 200:
        print(f"Données du nombre de ventes et visiteurs récupérées de l'API pour le jour {date}.")
        print(response.json())
    else:
        print(f"Erreur lors de la récupération des données de l'API: {response.status_code}")
        return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Vous devez renseigner une date en argument")
        sys.exit(1)

    date_param = sys.argv[1]

    try:
        date_obj = datetime.strptime(date_param, "%Y-%m-%d")
        print(f"Date passée en paramètre : {date_obj.strftime('%Y-%m-%d')}")
        fetch_retail_data(date_param)
    except ValueError:
        print("Erreur: Le format de la date est incorrect. Utilisez 'YYYY-MM-DD'.")
        sys.exit(1)
