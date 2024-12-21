from dotenv import load_dotenv
import os
import boto3
import pandas as pd
import io
from src.data_processing.extract.utils import fetch_from_api

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

# Récupérer les clés d'accès AWS depuis les variables d'environnement
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
region_name = os.getenv("AWS_REGION")

# Initialiser le client S3
s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
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

def fetch_and_save_stores():
    """
    Récupère et sauvegarde les données des magasins sur S3.
    """
    url = "http://127.0.0.1:8000/stores"
    data = fetch_from_api(url)  # Récupère les données depuis l'API

    if data:
        s3_key = f"{S3_FOLDER}/stores.parquet"  # Chemin dans S3
        save_to_s3(data, s3_key)
    else:
        print("Aucune donnée récupérée depuis l'API magasins.")


if __name__ == "__main__":
    fetch_and_save_stores()
