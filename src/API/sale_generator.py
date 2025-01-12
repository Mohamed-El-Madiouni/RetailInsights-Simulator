import json
import os
import random
import uuid
from io import TextIOWrapper

from src.API.store_generator import StoreGenerator


def load_products(data_dir):
    """
    Charge les produits depuis le fichier JSON 'products.json'.

    Args:
        data_dir (str): Répertoire contenant le fichier 'products.json'.

    Returns:
        list: Liste des produits chargés depuis le fichier.
        []: Si le fichier est introuvable.
    """
    try:
        file_path = os.path.join(data_dir, "products.json")
        with open(file_path, "r", encoding="utf-8") as f:
            products = json.load(f)
        return products
    except FileNotFoundError:
        print("Le fichier des produits n'existe pas.")
        return []


def load_clients(data_dir):
    """
    Charge les clients depuis le fichier JSON 'clients.json'.

    Args:
        data_dir (str): Répertoire contenant le fichier 'clients.json'.

    Returns:
        list: Liste des clients chargés depuis le fichier.
        []: Si le fichier est introuvable.
    """
    try:
        file_path = os.path.join(data_dir, "clients.json")
        with open(file_path, "r", encoding="utf-8") as f:
            clients = json.load(f)
        return clients
    except FileNotFoundError:
        print("Le fichier des clients n'existe pas.")
        return []


def generate_random_time(hour):
    """
    Génère une heure aléatoire sous la forme d'une chaîne au format 'HH:MM:SS'.

    Args:
        hour (int): Heure fixe (0-23) pour le champ HH.

    Returns:
        str: Heure aléatoire au format 'HH:MM:SS'.

    Raises:
        ValueError: Si l'heure est en dehors de l'intervalle [0, 23].
    """
    if not (0 <= hour <= 23):
        raise ValueError("L'heure doit être entre 0 et 23 inclus.")

    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return f"{hour:02}:{minute:02}:{second:02}"


class SaleGenerator:
    """
    Classe permettant de générer des ventes aléatoires pour un magasin.

    Args:
        date_str (str): Date pour laquelle générer les ventes, au format 'YYYY-MM-DD'.
        num_sales (int): Nombre de ventes à générer.
        store (dict): Informations sur le magasin.
        hour (int): Heure de la journée pour laquelle générer les ventes.
        data_dir (str): Répertoire contenant les données JSON. Par défaut, 'data_api'.
    """

    def __init__(self, date_str, num_sales, store, hour, data_dir="data_api"):
        self.products = load_products(data_dir)
        self.clients = load_clients(data_dir)
        self.store = store
        self.sales = []
        self.date_str = date_str
        self.num_sales = num_sales
        self.hour = hour
        self.data_dir = data_dir

        # Préchargement les clients par ville pour optimiser la recherche
        self.clients_by_city = {}
        for client in self.clients:
            self.clients_by_city.setdefault(client["city"], []).append(client)

    def generate_sales(self):
        """
        Génère des ventes aléatoires pour un nombre donné de transactions.

        Returns:
            list: Liste des ventes générées.
        """
        if self.num_sales is not None:
            for _ in range(self.num_sales):
                # Trouver un client dont la ville correspond à celle du magasin
                client = self._find_client_for_store(self.store)

                if (
                    client is None
                ):  # Si aucun client ne correspond à la ville du magasin, passer à la prochaine itération
                    continue

                nb_type_product = random.randint(
                    1, 5
                )  # Nombre de types de produits achetés
                sale_id = str(uuid.uuid4())  # ID unique pour la vente
                sale_time = generate_random_time(
                    self.hour
                )  # Uniformiser le sale_time pour ce sale_id
                total_sale_amount = 0  # Montant total de la vente

                # Sélectionner des produits uniques
                selected_products = random.sample(self.products, k=nb_type_product)

                for product in selected_products:
                    quantity = random.randint(1, 5)
                    sale_amount = round(product["price"] * quantity, 2)
                    total_sale_amount += sale_amount

                    self.sales.append(
                        {
                            "sale_id": sale_id,
                            "nb_type_product": nb_type_product,
                            "product_id": product["id"],
                            "client_id": client["id"],
                            "store_id": self.store["id"],
                            "quantity": quantity,
                            "sale_amount": sale_amount,
                            "sale_date": self.date_str,
                            "sale_time": sale_time,
                        }
                    )
        return self.sales

    def _find_client_for_store(self, store):
        """
        Trouve un client dont la ville correspond à celle du magasin.

        Args:
            store (dict): Informations sur le magasin.

        Returns:
            dict: Informations sur le client trouvé.
            None: Si aucun client n'est trouvé pour cette ville.
        """
        clients_in_city = self.clients_by_city.get(store["location"], [])
        if clients_in_city:
            # Retourner un client choisi aléatoirement
            return random.choice(clients_in_city)
        else:
            print(f"Aucun client trouvé pour la ville : {store['location']}")
            return None

    def get_sales(self):
        """
        Retourne la liste des ventes générés.

        Returns:
            list: Liste des ventes.
        """
        return self.sales

    def save_sales_to_file(self, data_dir="data_api"):
        """
        Sauvegarde les ventes générées dans un fichier JSON 'sales.json'.

        Args:
            data_dir (str): Répertoire où le fichier sera sauvegardé.
        """
        # Vérifier si le dossier spécifié existe, sinon le créer
        os.makedirs(self.data_dir, exist_ok=True)
        file_path = os.path.join(self.data_dir, "sales.json")

        if os.path.exists(file_path):
            # Charger les données existantes si le fichier existe
            with open(file_path, "r", encoding="utf-8") as f:
                existing_sales = json.load(f)
            # Ajouter les nouvelles ventes aux données existantes
            existing_sales.extend(self.sales)
            with open(file_path, "w", encoding="utf-8") as f:
                assert isinstance(f, TextIOWrapper)
                json.dump(existing_sales, f, ensure_ascii=False, indent=4)
        else:
            # Si le fichier n'existe pas, créer un nouveau fichier avec les données
            with open(file_path, "w", encoding="utf-8") as f:
                assert isinstance(f, TextIOWrapper)
                json.dump(self.sales, f, ensure_ascii=False, indent=4)
        print("écriture dans le fichier 'sales.json'")


# Exemple d'utilisation pour générer et sauvegarder des ventes pour une date donnée.
if __name__ == "__main__":
    date_test = "2024-12-14"  # Exemple de date à renseigner
    sale_generator = SaleGenerator(
        date_str=date_test, num_sales=5000, store=StoreGenerator(), hour=15
    )
    sale_generator.generate_sales()
    sale_generator.save_sales_to_file()

    print(
        f"Données générées et sauvegardées dans 'data_api/sales.json' pour la date {date_test}"
    )
