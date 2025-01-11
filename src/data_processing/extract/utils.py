import os
import tempfile
import io

import boto3
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import SparkSession

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
S3_FOLDER = "extracted_data/sales"


# Créer le dossier de sortie s'il n'existe pas
def create_output_folder(folder_name="data"):
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
        print(f"Dossier créé : {folder_name}")
    return folder_name


# Sauvegarder les données en Parquet avec Pandas
def save_with_pandas(data, output_file):
    df = pd.DataFrame(data)
    df.to_parquet(output_file, engine="pyarrow", compression="snappy")
    print(f"Fichier sauvegardé avec Pandas : {output_file}")


# Sauvegarder les données en Parquet avec Spark
def save_with_spark(data, output_file):
    spark = SparkSession.builder \
        .appName("API Data to Parquet") \
        .master("local[*]") \
        .getOrCreate()

    df = spark.read.json(spark.sparkContext.parallelize([data]))
    df.write.parquet(output_file, mode="overwrite", compression="snappy")
    print(f"Fichier sauvegardé avec Spark : {output_file}")

    spark.stop()


# Récupérer des données depuis une API
def fetch_from_api(url, is_test=False):
    if is_test:
        # Simuler une requête HTTP pendant les tests
        from unittest.mock import MagicMock
        mock_response = MagicMock()
        if "Paris" in url:
            mock_response.json.return_value = [{"id": "1", "name": "Client A", "city": "Paris"}]
        elif "Lyon" in url:
            mock_response.json.return_value = [{"id": "2", "name": "Client B", "city": "Lyon"}]
        elif "Nice" in url:
            mock_response.json.return_value = [{"id": "3", "name": "Client C", "city": "Nice"}]
        else:
            mock_response.json.return_value = []
        mock_response.status_code = 200
        return mock_response.json()
    else:
        import requests
        response = requests.get(url)
        if response.status_code == 200:
            print(f"Données récupérées depuis {url}")
            return response.json()
        else:
            print(f"Erreur lors de la récupération des données depuis {url}: {response.status_code}")
            raise Exception(f"Erreur lors de la récupération des données depuis {url}: {response.status_code}")


def save_to_s3(data, s3_key):
    """
    Sauvegarde les données au format Parquet directement sur S3.

    :param data: Les données (liste ou DataFrame).
    :param s3_key: Chemin du fichier dans le bucket S3 (inclut le dossier et le nom du fichier).
    """
    # Vérifier si les données sont déjà un DataFrame
    if not isinstance(data, pd.DataFrame):
        df = pd.DataFrame(data)
    else:
        df = data

    # Utiliser un fichier temporaire pour réduire l'empreinte mémoire
    with tempfile.NamedTemporaryFile(delete=True) as temp_file:
        df.to_parquet(temp_file.name, engine="pyarrow", compression="zstd", index=False)
        s3.upload_file(temp_file.name, BUCKET_NAME, s3_key)
    print(f"Fichier sauvegardé sur S3 : s3://{BUCKET_NAME}/{s3_key}")


def read_parquet_from_s3(s3_key):
    """
    Lire un fichier Parquet depuis S3 et le charger dans un DataFrame Pandas.

    :param s3_key: Chemin du fichier dans le bucket S3.
    :return: DataFrame Pandas contenant les données du fichier.
    """
    response = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
    buffer = io.BytesIO(response["Body"].read())
    return pd.read_parquet(buffer)
