import json
import os
import random
from io import TextIOWrapper

from faker import Faker


class ClientGenerator:
    def __init__(self, data_dir="data_api"):
        self.data_dir = data_dir
        self.fake = Faker()
        self.clients = []
        self.cities = [
            "Paris",
            "Lyon",
            "Marseille",
            "Nice",
            "Toulouse",
        ]  # Liste des villes

    def generate_clients(self, num_clients=20000):
        """
        Génère une liste de clients avec des informations aléatoires et une carte de fidélité.
        """
        for _ in range(num_clients):
            name = self.fake.name()

            # Vérifier s'il y a une erreur de saisie
            if random.random() < 0.0002:  # 0.02% de chance d'erreur
                # Cas d'erreur : âge entre 200 et 1000
                age = random.randint(200, 1000)
            else:
                # Sinon, générer un âge réaliste entre 18 et 80
                age = random.randint(18, 80)

            gender = random.choice(["Homme", "Femme"])
            has_loyalty_card = (
                random.random() < 0.3
            )  # 30% des clients ont une carte de fidélité

            # Choisir une ville aléatoire
            city = random.choice(self.cities)

            # Ajouter les informations du client
            self.clients.append(
                {
                    "id": self.fake.uuid4(),
                    "name": name,
                    "age": age,
                    "gender": gender,
                    "loyalty_card": has_loyalty_card,
                    "city": city,
                }
            )

    def save_clients(self, filename="clients.json"):
        """Sauvegarde les clients dans un fichier JSON dans le dossier spécifié."""
        # Vérifie si le dossier spécifié existe, sinon le crée
        os.makedirs(self.data_dir, exist_ok=True)

        # Définir le chemin du fichier dans le dossier spécifié
        filepath = os.path.join(self.data_dir, filename)

        # Sauvegarde les données dans un fichier JSON
        with open(filepath, "w", encoding="utf-8") as f:
            assert isinstance(f, TextIOWrapper)
            json.dump(self.clients, f, ensure_ascii=False, indent=4)

    def get_clients(self):
        return self.clients


# Exemple d'utilisation
if __name__ == "__main__":
    client_generator = ClientGenerator()
    client_generator.generate_clients()
    client_generator.save_clients()  # Sauvegarde les clients dans le fichier JSON
    print("Clients sauvegardés dans le dossier 'data_api'.")
