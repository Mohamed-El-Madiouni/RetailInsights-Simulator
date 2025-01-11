import json
import os

from src.data_processing.extract.utils import fetch_from_api, save_to_s3

S3_FOLDER = "extracted_data"


def fetch_cities():
    """
    Récupère les villes à partir du fichier clients.json.
    """
    cities = []
    file_name = os.path.join("data_api", "clients.json")
    if os.path.exists(file_name):
        with open(file_name, "r", encoding="utf-8") as f:
            for line in json.load(f):
                if line["city"] not in cities:
                    cities.append(line["city"])
    else:
        print("Le fichier clients.json n'existe pas.")
        raise FileNotFoundError("Le fichier clients.json n'existe pas.")
    return cities


def fetch_and_save_clients(is_test=False):
    """
    Récupère et sauvegarde les données clients sur S3.
    """
    base_url = "http://test" if is_test else "http://127.0.0.1:8000"
    cities = fetch_cities()
    all_clients = []
    for city in cities:
        url = f"{base_url}/clients?city={city}"
        try:
            data = fetch_from_api(url, is_test=is_test)
            if data:
                all_clients.extend(data)
        except Exception as e:
            print(f"Erreur lors de l'extraction pour la ville {city}: {e}")
            raise  # Relever l'exception pour interrompre l'exécution

    # Sauvegarder les données directement sur S3
    s3_key = f"{S3_FOLDER}/clients.parquet"  # Chemin dans S3
    save_to_s3(all_clients, s3_key)


if __name__ == "__main__":
    fetch_and_save_clients()
