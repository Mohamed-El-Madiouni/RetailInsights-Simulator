import io
import os
import tempfile

import boto3
import pandas as pd
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Initialiser le client S3
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)

# Paramètres S3
BUCKET_NAME = "retail-insights-bucket"
S3_FOLDER = "extracted_data/sales"


# Créer le dossier de sortie s'il n'existe pas
def create_output_folder(folder_name="data"):
    """
    Crée un dossier local pour sauvegarder les fichiers, s'il n'existe pas.

    Args:
        folder_name (str): Nom du dossier à créer. Par défaut, 'data'.

    Returns:
        str: Le nom du dossier créé.
    """
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
        print(f"Dossier créé : {folder_name}")
    return folder_name


# Sauvegarder les données en Parquet avec Pandas
def save_with_pandas(data, output_file):
    """
    Sauvegarde les données au format Parquet en utilisant Pandas.

    Args:
        data (list | dict): Données à sauvegarder.
        output_file (str): Chemin du fichier de sortie.
    """
    df = pd.DataFrame(data)
    df.to_parquet(output_file, engine="pyarrow", compression="snappy")
    print(f"Fichier sauvegardé avec Pandas : {output_file}")


# Récupérer des données depuis une API
def fetch_from_api(url, is_test=False):
    """
    Récupère les données depuis une API via une requête HTTP GET.

    Args:
        url (str): URL de l'API.
        is_test (bool): Si True, simule une réponse pour les tests. Par défaut, False.

    Returns:
        list | dict: Données récupérées depuis l'API.

    Raises:
        Exception: Si une erreur HTTP est rencontrée.
    """
    if is_test:
        # Simuler une requête HTTP pendant les tests
        from unittest.mock import MagicMock

        mock_response = MagicMock()
        if "Paris" in url:
            mock_response.json.return_value = [
                {"id": "1", "name": "Client A", "city": "Paris"}
            ]
        elif "Lyon" in url:
            mock_response.json.return_value = [
                {"id": "2", "name": "Client B", "city": "Lyon"}
            ]
        elif "Nice" in url:
            mock_response.json.return_value = [
                {"id": "3", "name": "Client C", "city": "Nice"}
            ]
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
            print(
                f"Erreur lors de la récupération des données depuis {url}: {response.status_code}"
            )
            raise Exception(
                f"Erreur lors de la récupération des données depuis {url}: {response.status_code}"
            )


def save_to_s3(data, s3_key):
    """
    Sauvegarde les données au format Parquet directement sur S3.

    Args:
        data (list | pd.DataFrame): Données à sauvegarder.
        s3_key (str): Chemin du fichier dans le bucket S3.
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
    Lit un fichier Parquet depuis S3 et le charge dans un DataFrame Pandas.

    Args:
        s3_key (str): Chemin du fichier dans le bucket S3.

    Returns:
        pd.DataFrame: Données du fichier chargé.
    """
    response = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
    buffer = io.BytesIO(response["Body"].read())
    return pd.read_parquet(buffer)
