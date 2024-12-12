import random
import uuid


class ProductGenerator:
    def __init__(self):
        self.categories = ['Électronique', 'Mode', 'Alimentation', 'Beauté', 'Meubles']
        self.product_names = ['Smartphone', 'Chaise', 'Ordinateur', 'Shampooing', 'Télévision', 'Chaussures', 'T-shirt',
                              'Bureau']
        self.product_counters = {name: 0 for name in self.product_names}  # Compteurs pour chaque produit
        self.products = []

    def generate_products(self, num_products=50):
        """
        Génère une liste de produits avec des catégories et des prix aléatoires.
        """
        for i in range(num_products):
            product_name = random.choice(self.product_names)
            category = None
            price = 0
            if product_name == 'Smartphone':
                category = 'Électronique'
                price = round(random.uniform(100, 2500), 2)
            elif product_name == 'Chaise':
                category = 'Meubles'
                price = round(random.uniform(10, 200), 2)
            elif product_name == 'Ordinateur':
                category = 'Électronique'
                price = round(random.uniform(200, 3000), 2)
            elif product_name == 'Shampooing':
                category = 'Beauté'
                price = round(random.uniform(1, 20), 2)
            elif product_name == 'Maquillage':
                category = 'Beauté'
                price = round(random.uniform(1, 50), 2)
            elif product_name == 'Télévision':
                category = 'Électronique'
                price = round(random.uniform(100, 2000), 2)
            elif product_name == 'Fruits&legumes':
                category = 'Alimentation'
                price = round(random.uniform(0.5, 10), 2)
            elif product_name == 'Viande':
                category = 'Alimentation'
                price = round(random.uniform(5, 20), 2)
            elif product_name == 'Chaussures':
                category = 'Mode'
                price = round(random.uniform(10, 300), 2)
            elif product_name == 'T-shirt':
                category = 'Mode'
                price = round(random.uniform(5, 50), 2)
            elif product_name == 'Bureau':
                category = 'Meubles'
                price = round(random.uniform(50, 400), 2)

            # Incrémente le compteur pour ce produit
            self.product_counters[product_name] += 1

            # Utilise le compteur pour créer un nom unique pour ce type de produit
            product_name_with_index = f"{product_name}_{self.product_counters[product_name]}"

            # Ajout du produit à la liste
            self.products.append({
                'id': str(uuid.uuid4()),
                'name': product_name_with_index,
                'category': category,
                'price': price
            })

    def get_products(self):
        return self.products


# Exemple d'utilisation
if __name__ == "__main__":
    product_generator = ProductGenerator()
    product_generator.generate_products()
    for product in product_generator.get_products():
        print(
            f"ID: {product['id']}, Name: {product['name']}, Category: {product['category']}, Price: {product['price']}")
