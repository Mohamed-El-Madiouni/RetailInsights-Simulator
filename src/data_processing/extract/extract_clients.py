from utils import create_output_folder, save_with_pandas, fetch_from_api
import os
import json


# Récupérer les villes depuis clients.json
def fetch_cities():
    cities = []
    file_name = os.path.join('data_api', 'clients.json')
    if os.path.exists(file_name):
        with open(file_name, 'r', encoding='utf-8') as f:
            for line in json.load(f):
                if line['city'] not in cities:
                    cities.append(line['city'])
    else:
        print("Le fichier clients.json n'existe pas.")
        raise "Le fichier clients.json n'existe pas..."
    return cities


# Récupérer et sauvegarder les données clients
def fetch_and_save_clients():
    cities = fetch_cities()
    all_clients = []
    for city in cities:
        url = f"http://127.0.0.1:8000/clients?city={city}"
        data = fetch_from_api(url)
        if data:
            all_clients.extend(data)

    output_folder = create_output_folder()
    output_file = os.path.join(output_folder, "clients.parquet")
    save_with_pandas(all_clients, output_file)


if __name__ == "__main__":
    fetch_and_save_clients()
