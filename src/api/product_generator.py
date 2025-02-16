import json
import os
import random
import uuid
from io import TextIOWrapper
from src.api.logger_generation import generation_logger


class ProductGenerator:
    def __init__(self, data_dir="data_api"):
        """
        Initialise la classe ProductGenerator.

        Args:
            data_dir (str): Répertoire où les fichiers JSON seront sauvegardés.
        """
        self.data_dir = data_dir
        self.categories = ["Électronique", "Mode", "Alimentation", "Beauté", "Meubles"]
        self.product_names = [
            "Smartphone",
            "Chaise",
            "Ordinateur",
            "Shampooing",
            "Télévision",
            "Chaussures",
            "T-shirt",
            "Bureau",
        ]
        self.product_counters = {
            name: 0 for name in self.product_names
        }  # Compteurs pour chaque produit
        self.products = []

    def generate_products(self, num_products=50):
        """
        Génère une liste de produits avec des catégories, des prix et un coût aléatoires.

        Args:
            num_products (int): Nombre de produits à générer. Par défaut, 50.
        """
        generation_logger.info(f"Starting product generation for {num_products} products.")
        for i in range(num_products):
            product_name = random.choice(self.product_names)
            category = None
            price = 0
            if product_name == "Smartphone":
                category = "Électronique"
                price = round(random.uniform(100, 2500), 2)
            elif product_name == "Chaise":
                category = "Meubles"
                price = round(random.uniform(10, 200), 2)
            elif product_name == "Ordinateur":
                category = "Électronique"
                price = round(random.uniform(200, 3000), 2)
            elif product_name == "Shampooing":
                category = "Beauté"
                price = round(random.uniform(1, 20), 2)
            elif product_name == "Maquillage":
                category = "Beauté"
                price = round(random.uniform(1, 50), 2)
            elif product_name == "Télévision":
                category = "Électronique"
                price = round(random.uniform(100, 2000), 2)
            elif product_name == "Fruits&legumes":
                category = "Alimentation"
                price = round(random.uniform(0.5, 10), 2)
            elif product_name == "Viande":
                category = "Alimentation"
                price = round(random.uniform(5, 20), 2)
            elif product_name == "Chaussures":
                category = "Mode"
                price = round(random.uniform(10, 300), 2)
            elif product_name == "T-shirt":
                category = "Mode"
                price = round(random.uniform(5, 50), 2)
            elif product_name == "Bureau":
                category = "Meubles"
                price = round(random.uniform(50, 400), 2)

            # Incrémente le compteur pour ce produit
            self.product_counters[product_name] += 1

            # Utilise le compteur pour créer un nom unique pour ce type de produit
            product_name_with_index = (
                f"{product_name}_{self.product_counters[product_name]}"
            )

            # Calculer le coût entre 50% et 80% du prix de vente
            cost = round(price * random.uniform(0.5, 0.8), 2)

            # Ajout du produit à la liste avec le champ 'cost'
            self.products.append(
                {
                    "id": str(uuid.uuid4()),
                    "name": product_name_with_index,
                    "category": category,
                    "price": price,
                    "cost": cost,
                }
            )
        generation_logger.info(f"Successfully generated {len(self.products)} products.")

    def save_products(self, filename="products.json"):
        """
        Sauvegarde la liste des produits générés dans un fichier JSON.

        Args:
            filename (str): Nom du fichier dans lequel sauvegarder les produits. Par défaut, 'products.json'.

        Raises:
            AssertionError: Si l'objet de fichier n'est pas une instance de TextIOWrapper.
        """
        # Vérifie si le dossier 'data_api' existe, sinon le crée
        os.makedirs(self.data_dir, exist_ok=True)

        # Définir le chemin du fichier dans le dossier spécifié
        filepath = os.path.join(self.data_dir, filename)

        try:
            # Sauvegarde les données dans un fichier JSON
            with open(filepath, "w", encoding="utf-8") as f:
                assert isinstance(f, TextIOWrapper)
                json.dump(self.products, f, ensure_ascii=False, indent=4)
            generation_logger.info(f"Products successfully saved to {filepath}.")
        except Exception as e:
            generation_logger.error(f"Error saving products to {filepath}: {str(e)}")

    def get_products(self):
        """
        Retourne la liste des produits générés.

        Returns:
            list: Liste des produits.
        """
        return self.products


# Exemple d'utilisation de la classe ProductGenerator pour générer et sauvegarder des produits.
if __name__ == "__main__":
    product_generator = ProductGenerator()
    product_generator.generate_products()
    product_generator.save_products()  # Sauvegarde les produits dans le fichier JSON
    print("Produits sauvegardés dans le dossier 'data_api'.")
