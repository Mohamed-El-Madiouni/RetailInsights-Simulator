from utils import fetch_from_api, save_to_s3
from src.data_processing.extract.logger_extraction import extraction_logger

# Paramètres S3
S3_FOLDER = "extracted_data"


def fetch_and_save_products():
    """
    Récupère les données produits depuis l'api et les sauvegarde sur S3.

    Raises:
        ValueError: Si aucune donnée n'est récupérée depuis l'api.
    """
    url = "http://127.0.0.1:8000/products"
    extraction_logger.info("Starting product data extraction.")

    try:
        data = fetch_from_api(url)  # Récupère les données depuis l'api
        if data:
            extraction_logger.info(f"Successfully fetched {len(data)} products from the API.")

            s3_key = f"{S3_FOLDER}/products.parquet"  # Chemin dans S3
            save_to_s3(data, s3_key)
            extraction_logger.info(f"Product data successfully saved to S3 at '{s3_key}'.")
        else:
            extraction_logger.warning("No data retrieved from the API.")
            raise ValueError("Aucune donnée récupérée depuis l'api produits.")
    except Exception as e:
        extraction_logger.error(f"Error during product data extraction or save process: {e}")
        raise


# Point d'entrée pour exécuter la récupération et la sauvegarde des données produits.
if __name__ == "__main__":
    try:
        fetch_and_save_products()
        extraction_logger.info("Product extraction process completed successfully.")
    except Exception as e:
        extraction_logger.critical(f"Product extraction process failed: {e}")
