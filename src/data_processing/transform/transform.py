import io
import os

import boto3
import pandas as pd
from dotenv import load_dotenv
from src.data_processing.transform.logger_transformation import transformation_logger

# Charger les variables d'environnement
load_dotenv()

# Initialiser le client S3
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)

BUCKET_NAME = "retail-insights-bucket"


def read_parquet_files_from_s3(folder_prefix):
    """
    Lit et concatène les fichiers Parquet depuis un dossier S3.

    Identifie les fichiers dans la racine du dossier et concatène les fichiers
    dans les sous-dossiers spécifiques tels que `sales` et `retail_data`.

    Args:
        folder_prefix (str): Préfixe S3 représentant le dossier contenant les fichiers/dossiers Parquet.

    Returns:
        None: Affiche les DataFrames concaténés ou les fichiers trouvés.
    """
    transformation_logger.info(f"Starting to read Parquet files from S3 folder: {folder_prefix}")

    # Sous-dossiers cibles pour concaténation
    target_subfolders = ["sales", "retail_data"]

    # Lister les objets dans le dossier S3
    try:
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=folder_prefix)
    except Exception as e:
        transformation_logger.error(f"Error listing objects in S3 folder {folder_prefix}: {e}")
        return

    # Vérifier si des fichiers existent dans le préfixe donné
    if "Contents" not in response:
        transformation_logger.warning(f"No files found in S3 folder: {folder_prefix}")
        return

    # Organiser les fichiers
    files_in_root = []
    files_by_folder = {}

    for obj in response["Contents"]:
        key = obj["Key"]

        # Vérifier si c'est un fichier Parquet directement sous extracted_data/
        if key.endswith(".parquet") and "/" not in key[len(folder_prefix):]:
            files_in_root.append(key)
        else:
            # Identifier le sous-dossier
            for subfolder in target_subfolders:
                if key.startswith(f"{folder_prefix}{subfolder}/") and key.endswith(
                    ".parquet"
                ):
                    folder = "/".join(key.split("/")[:-1])
                    files_by_folder.setdefault(folder, []).append(key)

    # Traiter les fichiers directement dans extracted_data/
    for file_key in files_in_root:
        transformation_logger.info(f"Reading Parquet file in root: s3://{BUCKET_NAME}/{file_key}")
        try:
            response = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
            buffer = io.BytesIO(response["Body"].read())
            df = pd.read_parquet(buffer)
            transformation_logger.info(f"File {file_key} read successfully with {len(df)} records.")
        except Exception as e:
            transformation_logger.error(f"Error reading Parquet file {file_key}: {e}")

    # Lire et concaténer les fichiers pour chaque sous-dossier cible
    for folder, files in files_by_folder.items():
        transformation_logger.info(f"Reading files in folder: s3://{BUCKET_NAME}/{folder}")

        data_frames = []
        for file_key in files:
            try:
                transformation_logger.info(f"Reading Parquet file: s3://{BUCKET_NAME}/{file_key}")
                response = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
                buffer = io.BytesIO(response["Body"].read())
                data_frames.append(pd.read_parquet(buffer))
            except Exception as e:
                transformation_logger.error(f"Error reading Parquet file {file_key}: {e}")
                continue

        # Concaténer les DataFrames si la liste n'est pas vide
        if data_frames:
            concatenated_df = pd.concat(data_frames, ignore_index=True)
            transformation_logger.info(
                f"Folder {folder} concatenated successfully with {len(concatenated_df)} records."
            )
        else:
            transformation_logger.warning(f"No files to concatenate in folder {folder}.")


# Point d'entrée pour exécuter la lecture et l'affichage des fichiers Parquet depuis S3.
if __name__ == "__main__":
    folder_prefix = "extracted_data/"  # Dossier racine dans S3
    transformation_logger.info("Starting transformation script.")
    try:
        read_parquet_files_from_s3(folder_prefix)
        transformation_logger.info("Transformation script completed successfully.")
    except Exception as e:
        transformation_logger.critical(f"Transformation script failed: {e}")
