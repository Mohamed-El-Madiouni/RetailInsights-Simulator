import random
import uuid
import json
import os
from io import TextIOWrapper


class StoreGenerator:
    def __init__(self):
        self.stores = []

    def generate_stores(self, num_stores=10):
        """
        Génère une liste de magasins avec un nom, emplacement, capacité, heures d'ouverture et de fermeture aléatoires.
        """
        for i in range(num_stores):
            store_name = f"Magasin_{i+1}"
            location = random.choice(['Paris', 'Lyon', 'Marseille', 'Nice', 'Toulouse'])
            capacity = random.randint(100, 1500)  # Nombre de clients qu'un magasin peut accueillir

            # Heures d'ouverture et de fermeture
            opening_hour = random.choice(['7', '8', '9'])
            closing_hour = random.choice(['19', '20', '21', '22'])
            self.stores.append({
                'id': str(uuid.uuid4()),
                'name': store_name,
                'location': location,
                'capacity': capacity,
                'opening_hour': opening_hour,
                'closing_hour': closing_hour
            })

    def save_stores(self, filename="stores.json"):
        """Sauvegarde les magasins dans un fichier JSON dans le dossier 'data'."""
        # Vérifie si le dossier 'data' existe, sinon le crée
        os.makedirs('data', exist_ok=True)

        # Définir le chemin du fichier dans le dossier 'data'
        filepath = os.path.join('data', filename)

        # Sauvegarde les données dans un fichier JSON
        with open(filepath, 'w', encoding='utf-8') as f:
            assert isinstance(f, TextIOWrapper)
            json.dump(self.stores, f, ensure_ascii=False, indent=4)

    def get_stores(self):
        return self.stores


# Exemple d'utilisation
if __name__ == "__main__":
    store_generator = StoreGenerator()
    store_generator.generate_stores()
    store_generator.save_stores()  # Sauvegarde les magasins dans le fichier JSON
    print("Magasins sauvegardés dans le dossier 'data'.")
