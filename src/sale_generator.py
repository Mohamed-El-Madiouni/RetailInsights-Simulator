import random
import uuid
import json
from io import TextIOWrapper
import os


def load_products():
    """Charge les produits depuis le fichier JSON 'products.json'."""
    try:
        with open('data/products.json', 'r', encoding='utf-8') as f:
            products = json.load(f)
        return products
    except FileNotFoundError:
        print("Le fichier des produits n'existe pas.")
        return []


def load_clients():
    """Charge les clients depuis le fichier JSON 'clients.json'."""
    try:
        with open('data/clients.json', 'r', encoding='utf-8') as f:
            clients = json.load(f)
        return clients
    except FileNotFoundError:
        print("Le fichier des clients n'existe pas.")
        return []


def load_stores():
    """Charge les magasins depuis le fichier JSON 'stores.json'."""
    try:
        with open('data/stores.json', 'r', encoding='utf-8') as f:
            stores = json.load(f)
        return stores
    except FileNotFoundError:
        print("Le fichier des magasins n'existe pas.")
        return []


class SaleGenerator:
    def __init__(self, date_str, num_sales):
        self.products = load_products()
        self.clients = load_clients()
        self.stores = load_stores()
        self.sales = []
        self.date_str = date_str
        self.num_sales = num_sales

    def generate_sales(self):
        """
        Génère des ventes aléatoires pour un nombre donné de transactions.
        """
        for _ in range(self.num_sales):
            # Choisir un client, un magasin et plusieurs produits
            client = random.choice(self.clients)

            # Trouver un magasin dont la ville correspond à celle du client
            store = self._find_store_for_client(client)

            if store is None:  # Si aucun magasin ne correspond à la ville du client, passer à la prochaine itération
                continue

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
                    'sale_date': self.date_str  # La date de la vente passée en paramètre
                })

    def _find_store_for_client(self, client):
        """
        Trouve un magasin dont la ville correspond à celle du client.
        """
        matching_stores = [
            store for store in self.stores if store['location'] == client['city']
        ]

        # Vérifier si des magasins correspondent à la ville du client
        if not matching_stores:
            print(f"Aucun magasin trouvé pour la ville : {client['city']}")
            return None

        # Retourner un magasin choisi aléatoirement
        return random.choice(matching_stores)

    def get_sales(self):
        return self.sales

    def save_sales_to_file(self):
        """Sauvegarder les ventes générées dans un fichier JSON."""
        if os.path.exists('data/sales.json'):
            # Charger les données existantes si le fichier existe
            with open('data/sales.json', 'r', encoding='utf-8') as f:
                existing_sales = json.load(f)
            # Ajouter les nouvelles ventes aux données existantes
            existing_sales.extend(self.sales)
            with open('data/sales.json', 'w', encoding='utf-8') as f:
                assert isinstance(f, TextIOWrapper)
                json.dump(existing_sales, f, ensure_ascii=False, indent=4)
        else:
            # Si le fichier n'existe pas, créer un nouveau fichier avec les données
            with open('data/sales.json', 'w', encoding='utf-8') as f:
                assert isinstance(f, TextIOWrapper)
                json.dump(self.sales, f, ensure_ascii=False, indent=4)


# Exemple d'utilisation
if __name__ == "__main__":
    date_test = "2024-12-14"  # Exemple de date à renseigner
    sale_generator = SaleGenerator(date_str=date_test, num_sales=5000)
    sale_generator.generate_sales()
    sale_generator.save_sales_to_file()

    print(f"Données générées et sauvegardées dans 'data/sales.json' pour la date {date_test}")
