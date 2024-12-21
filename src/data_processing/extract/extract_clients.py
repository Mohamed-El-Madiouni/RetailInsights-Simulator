from dotenv import load_dotenv
import os
import boto3
import pandas as pd
import io
import json
from utils import fetch_from_api

# Charger les variables d'environnement
load_dotenv()

# Initialiser le client S3
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

# Paramètres S3
BUCKET_NAME = "retail-insights-bucket"
S3_FOLDER = "extracted_data"

def save_to_s3(data, s3_key):
    """
    Sauvegarde les données au format Parquet directement sur S3.

    :param data: Les données (liste ou DataFrame).
    :param s3_key: Chemin du fichier dans le bucket S3 (inclut le dossier et le nom du fichier).
    """
    # Convertir les données en DataFrame
    df = pd.DataFrame(data)

    # Sauvegarder dans un buffer en mémoire
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", compression="snappy", index=False)

    # Réinitialiser le curseur du buffer avant de le charger sur S3
    buffer.seek(0)

    # Charger le fichier sur S3
    s3.upload_fileobj(buffer, BUCKET_NAME, s3_key)
    print(f"Fichier sauvegardé sur S3 : s3://{BUCKET_NAME}/{s3_key}")


def fetch_cities():
    """
    Récupère les villes à partir du fichier clients.json.
    """
    cities = []
    file_name = os.path.join('data_api', 'clients.json')
    if os.path.exists(file_name):
        with open(file_name, 'r', encoding='utf-8') as f:
            for line in json.load(f):
                if line['city'] not in cities:
                    cities.append(line['city'])
    else:
        print("Le fichier clients.json n'existe pas.")
        raise FileNotFoundError("Le fichier clients.json n'existe pas...")
    return cities


def fetch_and_save_clients():
    """
    Récupère et sauvegarde les données clients sur S3.
    """
    cities = fetch_cities()
    all_clients = []
    for city in cities:
        url = f"http://127.0.0.1:8000/clients?city={city}"
        data = fetch_from_api(url)
        if data:
            all_clients.extend(data)

    # Sauvegarder les données directement sur S3
    s3_key = f"{S3_FOLDER}/clients.parquet"  # Chemin dans S3
    save_to_s3(all_clients, s3_key)


if __name__ == "__main__":
    fetch_and_save_clients()
