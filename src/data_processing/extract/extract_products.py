from utils import create_output_folder, save_with_pandas, fetch_from_api
import os


# Récupérer et sauvegarder les données produits
def fetch_and_save_products():
    url = "http://127.0.0.1:8000/products"
    data = fetch_from_api(url)  # Récupère les données depuis l'API

    if data:
        output_folder = create_output_folder()
        output_file = os.path.join(output_folder, "products.parquet")
        save_with_pandas(data, output_file)  # Sauvegarde les données en format Parquet
        print(f"Fichier Parquet créé : {output_file}")
    else:
        print("Aucune donnée récupérée depuis l'API produits.")
        raise "Aucune donnée récupérée depuis l'API produits."


if __name__ == "__main__":
    fetch_and_save_products()
