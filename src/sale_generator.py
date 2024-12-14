import random
import uuid
import json
from io import TextIOWrapper
import os
from store_generator import StoreGenerator


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


def generate_random_time(hour):
    """
    Génère une heure aléatoire sous la forme d'un objet datetime.time pour une heure donnée.

    :param hour: L'heure fixe (0-23) pour le champ HH.
    :return: Un objet datetime.time représentant l'heure.
    """
    if not (0 <= hour <= 23):
        raise ValueError("L'heure doit être entre 0 et 23 inclus.")

    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return f"{hour:02}:{minute:02}:{second:02}"


class SaleGenerator:
    def __init__(self, date_str, num_sales, store, hour):
        self.products = load_products()
        self.clients = load_clients()
        self.store = store
        self.sales = []
        self.date_str = date_str
        self.num_sales = num_sales
        self.hour = hour
        self.sale_time = None

    def generate_sales(self):
        """
        Génère des ventes aléatoires pour un nombre donné de transactions.
        """
        for _ in range(self.num_sales):
            # Trouver un client dont la ville correspond à celle du magasin
            client = self._find_client_for_store(self.store)

            if client is None:  # Si aucun client ne correspond à la ville du magasin, passer à la prochaine itération
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
                    'store_id': self.store['id'],
                    'quantity': quantity,
                    'sale_amount': sale_amount,
                    'sale_date': self.date_str,
                    'sale_time': generate_random_time(self.hour)
                })

    def _find_client_for_store(self, store):
        """
        Trouve un client dont la ville correspond à celle du magasin.
        """
        matching_clients = [
            client for client in self.clients if store['location'] == client['city']
        ]

        # Vérifier si des clients correspondent à la ville du magasin
        if not matching_clients:
            print(f"Aucun client trouvé pour la ville : {store['location']}")
            return None

        # Retourner un client choisi aléatoirement
        return random.choice(matching_clients)

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
    sale_generator = SaleGenerator(date_str=date_test, num_sales=5000, store=StoreGenerator(), hour=15)
    sale_generator.generate_sales()
    sale_generator.save_sales_to_file()

    print(f"Données générées et sauvegardées dans 'data/sales.json' pour la date {date_test}")
