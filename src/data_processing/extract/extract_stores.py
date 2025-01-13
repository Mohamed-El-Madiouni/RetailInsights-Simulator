from src.data_processing.extract.utils import fetch_from_api, save_to_s3

# Paramètres S3
S3_FOLDER = "extracted_data"


def fetch_and_save_stores():
    """
    Récupère les données des magasins depuis l'api et les sauvegarde sur S3.

    Raises:
        ValueError: Si aucune donnée n'est récupérée depuis l'api.
    """
    url = "http://127.0.0.1:8000/stores"
    data = fetch_from_api(url)  # Récupère les données depuis l'api

    if data:
        s3_key = f"{S3_FOLDER}/stores.parquet"  # Chemin dans S3
        save_to_s3(data, s3_key)
    else:
        print("Aucune donnée récupérée depuis l'api magasins.")


# Point d'entrée pour exécuter la récupération et la sauvegarde des données des magasins.
if __name__ == "__main__":
    fetch_and_save_stores()
