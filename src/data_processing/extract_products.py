import requests


# Fonction pour requêter l'API
def fetch_data_products():
    url = f"http://127.0.0.1:8000/products"
    response = requests.get(url)
    if response.status_code == 200:
        print("Données de produits récupérées de l'API.")
        print(response.json())
    else:
        print(f"Erreur lors de la récupération des données de produits de l'API: {response.status_code}")


if __name__ == "__main__":
    fetch_data_products()
