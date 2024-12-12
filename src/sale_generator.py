import random
import uuid
from datetime import datetime

from client_generator import ClientGenerator
from product_generator import ProductGenerator
from store_generator import StoreGenerator


class SaleGenerator:
    def __init__(self, products, clients, stores):
        self.products = products
        self.clients = clients
        self.stores = stores
        self.sales = []

    def generate_sales(self, num_sales=500):
        """
        Génère des ventes aléatoires pour un nombre donné de transactions.
        """
        for _ in range(num_sales):
            # Choisir un client, un magasin et plusieurs produits
            client = random.choice(self.clients)
            store = random.choice(self.stores)
            nb_type_product = random.randint(1, 5)  # Nombre de types de produits achetés

            sale_id = str(uuid.uuid4())  # ID unique pour la vente
            total_sale_amount = 0  # Montant total de la vente

            # Créer des lignes pour chaque produit acheté
            for _ in range(nb_type_product):
                product = random.choice(self.products)
                quantity = random.randint(1, 5)  # Quantité achetée pour ce produit
                sale_amount = round(product['price'] * quantity, 2)
                total_sale_amount += sale_amount

                self.sales.append({
                    'sale_id': sale_id,
                    'nb_type_product': nb_type_product,
                    'product_id': product['id'],
                    'client_id': client['id'],
                    'store_id': store['id'],
                    'quantity': quantity,
                    'sale_amount': sale_amount,
                    'sale_date': datetime.now().strftime("%Y-%m-%d")
                })

    def get_sales(self):
        return self.sales


# Exemple d'utilisation
if __name__ == "__main__":
    # Exemple de génération des ventes après avoir généré les autres données
    product_generator = ProductGenerator()
    product_generator.generate_products()
    client_generator = ClientGenerator()
    client_generator.generate_clients()
    store_generator = StoreGenerator()
    store_generator.generate_stores()

    sale_generator = SaleGenerator(product_generator.get_products(), client_generator.get_clients(),
                                   store_generator.get_stores())
    sale_generator.generate_sales()
    for sale in sale_generator.get_sales():
        print(sale)
