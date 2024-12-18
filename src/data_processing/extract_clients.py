import requests
import os
import json


# Fonction pour retrouver toutes les villes des clients
def fetch_cities():
    cities = []
    file_name = os.path.join('../../data', 'clients.json')

    # Charger les ventes existantes si le fichier existe
    if os.path.exists(file_name):
        with open(file_name, 'r', encoding='utf-8') as f:
            for line in json.load(f):
                if line['city'] not in cities:
                    cities.append(line['city'])
    else:
        print("File dont exists")

    return cities


# Fonction pour requêter l'API
def fetch_data_clients():
    cities = fetch_cities()
    for city in cities:
        url = f"http://127.0.0.1:8000/clients?city={city}"
        response = requests.get(url)
        if response.status_code == 200:
            print(f"Données de clients récupérées de l'API pour la ville {city}.")
            print(response.json())
        else:
            print(f"Erreur lors de la récupération des données de clients de l'API: {response.status_code}")
            return 0


if __name__ == "__main__":
    fetch_data_clients()
