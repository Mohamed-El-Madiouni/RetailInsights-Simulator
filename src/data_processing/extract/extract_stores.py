import requests


# Fonction pour requêter votre API
def fetch_data_stores():
    url = f"http://127.0.0.1:8000/stores"
    response = requests.get(url)
    if response.status_code == 200:
        print("Données de magasins récupérées de l'API.")
        print(response.json())
    else:
        print(f"Erreur lors de la récupération des données de magasins de l'API: {response.status_code}")


if __name__ == "__main__":
    fetch_data_stores()
