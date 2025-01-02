from src.data_processing.extract.utils import fetch_from_api, save_to_s3

# Paramètres S3
S3_FOLDER = "extracted_data"


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
