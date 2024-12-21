from dotenv import load_dotenv
import os
import boto3
import pandas as pd
import io
import sys
from datetime import datetime
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
S3_FOLDER = "extracted_data/retail_data"

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


def fetch_and_save_retail_data(date):
    """
    Récupère et sauvegarde les données retail_data sur S3.
    """
    url = f"http://127.0.0.1:8000/retail_data?date={date}"
    new_data = fetch_from_api(url)  # Récupère les nouvelles données depuis l'API

    if new_data:
        # Formater la date pour nommer le fichier
        month_str = pd.to_datetime(date).strftime("%Y-%m")
        s3_key = f"{S3_FOLDER}/retail_data_{month_str}.parquet"

        # Charger les données existantes depuis S3, s'il y en a
        try:
            existing_data = read_parquet_from_s3(s3_key)
            if existing_data is not None:
                # Ajouter les nouvelles données aux anciennes
                updated_data = pd.concat([existing_data, pd.DataFrame(new_data)], ignore_index=True)
                updated_data.drop_duplicates(inplace=True)
            else:
                updated_data = pd.DataFrame(new_data)
        except Exception as e:
            print(f"Aucune donnée existante. Création d'un nouveau fichier : {e}")
            updated_data = pd.DataFrame(new_data)

        # Sauvegarder les données mises à jour sur S3
        save_to_s3(updated_data, s3_key)
    else:
        print(f"Aucune donnée récupérée pour la date {date}.")
        raise ValueError(f"Aucune donnée récupérée pour la date {date}.")


def read_parquet_from_s3(s3_key):
    """
    Lire un fichier Parquet depuis S3 et le charger dans un DataFrame Pandas.

    :param s3_key: Chemin du fichier dans le bucket S3.
    :return: DataFrame Pandas contenant les données du fichier.
    """
    response = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
    buffer = io.BytesIO(response["Body"].read())
    return pd.read_parquet(buffer)


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
