import json
import os
import random
import sys
from datetime import datetime
from io import TextIOWrapper

from src.API.sale_generator import SaleGenerator


def get_current_date():
    """
    Retourne la date actuelle au format 'YYYY-MM-DD'.

    Returns:
        str: Date actuelle sous forme de chaîne.
    """
    return datetime.now().strftime("%Y-%m-%d")


def load_stores(data_dir):
    """
    Charge les informations des magasins depuis le fichier 'stores.json'.

    Args:
        data_dir (str): Répertoire contenant le fichier 'stores.json'.

    Returns:
        list: Liste des magasins chargés depuis le fichier.
        []: Si le fichier est introuvable.
    """
    try:
        file_path = os.path.join(data_dir, "stores.json")
        with open(file_path, "r", encoding="utf-8") as f:
            stores = json.load(f)
        return stores
    except FileNotFoundError:
        print("Le fichier des magasins n'existe pas.")
        return []


def generate_data(
    date_str,
    hour,
    store,
    data_dir,
    force_null=None,
    force_aberrant=None,
    normal_test=None,
):
    """
    Génère des données de visiteurs et de ventes pour un magasin à une heure donnée.

    Args:
        date_str (str): La date pour laquelle les données sont générées, au format 'YYYY-MM-DD'.
        hour (int): L'heure pour laquelle les données sont générées.
        store (dict): Informations sur le magasin (e.g., capacité, horaires d'ouverture).
        data_dir (str): Répertoire contenant les données JSON.
        force_null (bool, optional): Force les données nulles si True. Par défaut, None.
        force_aberrant (bool, optional): Force les données aberrantes si True. Par défaut, None.
        normal_test (bool, optional): Mode test, désactive certains comportements aléatoires. Par défaut, None.

    Returns:
        tuple: Contient deux éléments :
            dict: Données retail pour l'heure spécifiée.
            list: Liste des données des ventes générées.
    """
    date = datetime.strptime(date_str, "%Y-%m-%d")
    day_of_week = date.weekday()  # 0 = lundi, 6 = dimanche

    # Vérifier si l'heure est dans les horaires d'ouverture
    opening_hour = int(store["opening_hour"])
    closing_hour = int(store["closing_hour"])

    max_visitors = store["capacity"]
    max_sales = max_visitors * 0.4

    # Si le magasin est fermé à cette heure, on ne génère pas de visiteurs/ventes
    if hour < opening_hour or hour >= closing_hour:
        visitors = 0
        sales = 0
    else:
        # 0.2% de chance d'avoir une donnée nulle sauf si on force les données nulles
        is_null = (force_null is not None) or (random.random() < 0.002)
        # 0.1% de chance d'avoir une donnée aberrante sauf si on force les données aberrantes
        is_aberrant = (force_aberrant is not None) or (random.random() < 0.001)

        if is_null and normal_test is None:
            visitors = None
            sales = None
        else:
            # Générer le nombre de visiteurs basé sur l'heure
            if hour < 8:
                visitors = random.randint(
                    int(round(max_visitors * 0.05, 0)),
                    int(round(max_visitors * 0.35, 0)),
                )
                sales = random.randint(
                    int(round(max_sales * 0.05, 0)), int(round(max_sales * 0.35, 0))
                )
            elif 8 <= hour < 20:
                visitors = random.randint(
                    int(round(max_visitors * 0.2, 0)), int(round(max_visitors * 0.8, 0))
                )
                sales = random.randint(
                    int(round(max_sales * 0.2, 0)), int(round(max_sales * 0.8, 0))
                )
            else:
                visitors = random.randint(
                    int(round(max_visitors * 0.05, 0)),
                    int(round(max_visitors * 0.3, 0)),
                )
                sales = random.randint(
                    int(round(max_sales * 0.05, 0)), int(round(max_sales * 0.3, 0))
                )

        if is_aberrant and normal_test is None:
            visitors = random.randint(10000, 50000)

        # Appliquer la capacité maximale du magasin
        if visitors is not None and visitors > store["capacity"] and not is_aberrant:
            visitors = store["capacity"]

    if day_of_week in [5, 6]:  # Le week-end a tendance à avoir plus de monde
        visitors = int(visitors * 1.25) if visitors is not None else None
        sales = int(sales * 1.15) if sales is not None else None

    # Générer des ventes pour chaque heure (en fonction de la date et du nombre de ventes)
    sale_generator = SaleGenerator(
        date_str=date_str, num_sales=sales, store=store, hour=hour, data_dir=data_dir
    )
    sales_data = sale_generator.generate_sales()

    return {
        "store_id": store["id"],
        "store_name": store["name"],
        "date": date_str,
        "hour": hour,
        "visitors": visitors,
        "sales": sales,
    }, sales_data


class RetailDataGenerator:
    """
    Classe pour générer et gérer les données retail et les ventes.

    Args:
        data_dir (str): Répertoire où les fichiers JSON seront lus et sauvegardés.
    """

    def __init__(self, data_dir="data_api"):
        self.data_dir = data_dir
        self.stores = load_stores(
            self.data_dir
        )  # Charger les magasins depuis le fichier JSON
        self.retail_data = []  # Données retail
        self.sales_buffer = []  # Données des ventes

    def generate_data_day(self, date_str, is_test=None):
        """
        Génère des données retail et de ventes pour une journée complète.

        Args:
            date_str (str): La date pour laquelle générer les données, au format 'YYYY-MM-DD'.
            is_test (bool, optional): Si True, génère des données uniquement pour une heure. Par défaut, None.
        """
        # Réinitialiser les buffers de données avant de générer pour une nouvelle date
        self.retail_data = []
        self.sales_buffer = []
        for store in self.stores:
            if is_test:
                # Générer des données de retail pour une heure lors de tests
                retail_entry, sales = generate_data(date_str, 12, store, self.data_dir)
                self.retail_data.append(retail_entry)
                self.sales_buffer.extend(sales)
            else:
                for hour in range(24):
                    # Générer des données de retail pour chaque heure
                    retail_entry, sales = generate_data(
                        date_str, hour, store, self.data_dir
                    )
                    self.retail_data.append(retail_entry)
                    self.sales_buffer.extend(sales)

        # Sauvegarder les données retail et ventes dans un fichier JSON
        self.save_retail_data_to_file()
        self.save_sales_to_file()

    def save_retail_data_to_file(self):
        """
        Sauvegarde les données retail générées dans le fichier 'retail_data.json'.

        Les données existantes dans le fichier seront préservées et complétées.
        """
        file_name = os.path.join(self.data_dir, "retail_data.json")

        # Charger les ventes existantes si le fichier existe
        existing_sales = []
        if os.path.exists(file_name):
            with open(file_name, "r", encoding="utf-8") as f:
                existing_sales = json.load(f)

        # Ajouter les nouvelles ventes
        existing_sales.extend(self.retail_data)

        with open(file_name, "w", encoding="utf-8") as f:
            assert isinstance(f, TextIOWrapper)
            json.dump(existing_sales, f, ensure_ascii=False, indent=4)
        print("Données retail écrites dans 'retail_data.json'.")

    def save_sales_to_file(self):
        """
        Sauvegarde les données des ventes générées dans le fichier 'sales.json'.

        Les données existantes dans le fichier seront préservées et complétées.
        """
        file_path = os.path.join(self.data_dir, "sales.json")

        # Charger les ventes existantes si le fichier existe
        existing_sales = []
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                existing_sales = json.load(f)

        # Ajouter les nouvelles ventes
        existing_sales.extend(self.sales_buffer)

        # Écrire toutes les ventes dans le fichier au format liste JSON
        with open(file_path, "w", encoding="utf-8") as f:
            assert isinstance(f, TextIOWrapper)
            json.dump(existing_sales, f, ensure_ascii=False, indent=4)
        print("Toutes les ventes ont été écrites dans 'sales.json'.")


# Point d'entrée pour générer des données retail et des ventes pour une ou plusieurs dates.
if __name__ == "__main__":
    generator = RetailDataGenerator()
    if len(sys.argv) < 2:
        date_test = get_current_date()
        generator.generate_data_day(date_test)
        print(
            f"Données générées et sauvegardées dans 'data_api/retail_data.json' "
            f"et 'data_api/sales.json' pour la date {date_test}"
        )
    else:
        for i in range(len(sys.argv)):
            if i == 0:
                continue
            date_test = sys.argv[i]
            generator.generate_data_day(date_test)
            print(
                f"Données générées et sauvegardées dans 'data_api/retail_data.json' "
                f"et 'data_api/sales.json' pour la date {date_test}"
            )
