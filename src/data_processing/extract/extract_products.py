from utils import fetch_from_api, save_to_s3

# Paramètres S3
S3_FOLDER = "extracted_data"


def fetch_and_save_products():
    """
    Récupère et sauvegarde les données produits sur S3.
    """
    url = "http://127.0.0.1:8000/products"
    data = fetch_from_api(url)  # Récupère les données depuis l'API

    if data:
        s3_key = f"{S3_FOLDER}/products.parquet"  # Chemin dans S3
        save_to_s3(data, s3_key)
    else:
        print("Aucune donnée récupérée depuis l'API produits.")
        raise ValueError("Aucune donnée récupérée depuis l'API produits.")


if __name__ == "__main__":
    fetch_and_save_products()
