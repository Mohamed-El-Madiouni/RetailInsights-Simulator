import json
import os

from src.data_processing.extract.utils import fetch_from_api, save_to_s3

S3_FOLDER = "extracted_data"


def fetch_cities():
    """
    Récupère la liste unique des villes à partir du fichier 'clients.json'.

    Returns:
        list: Liste des villes uniques extraites du fichier 'clients.json'.

    Raises:
        FileNotFoundError: Si le fichier 'clients.json' n'existe pas.
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
    Récupère les données clients pour chaque ville depuis l'api et les sauvegarde sur S3.

    Args:
        is_test (bool): Si True, utilise une URL de test pour les requêtes api.

    Raises:
        Exception: Si une erreur survient lors de l'extraction des données pour une ville.
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


# Point d'entrée pour exécuter la fonction de récupération et de sauvegarde des clients.
if __name__ == "__main__":
    fetch_and_save_clients()
