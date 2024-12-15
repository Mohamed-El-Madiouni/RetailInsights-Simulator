import random
from datetime import datetime
import json
from io import TextIOWrapper
import os
from src.sale_generator import SaleGenerator


def load_stores(data_dir):
    """Charge les magasins depuis le fichier JSON 'stores.json'."""
    try:
        file_path = os.path.join(data_dir, 'stores.json')
        with open(file_path, 'r', encoding='utf-8') as f:
            stores = json.load(f)
        return stores
    except FileNotFoundError:
        print("Le fichier des magasins n'existe pas.")
        return []


def generate_data(date_str, hour, store, data_dir, force_null=None, force_aberrant=None):
    """Génère un nombre de visiteurs et de ventes basé sur l'heure de la journée."""
    date = datetime.strptime(date_str, "%Y-%m-%d")
    day_of_week = date.weekday()  # 0 = lundi, 6 = dimanche

    # Vérifier si l'heure est dans les horaires d'ouverture
    opening_hour = int(store['opening_hour'])
    closing_hour = int(store['closing_hour'])

    max_visitors = store['capacity']
    max_sales = max_visitors*0.4

    # Si le magasin est fermé à cette heure, on ne génère pas de visiteurs/ventes
    if hour < opening_hour or hour >= closing_hour:
        visitors = 0
        sales = 0
    else:
        is_null = False
        is_aberrant = False
        if (force_null is not None) or (force_aberrant is not None):
            if force_null is not None:
                is_null = True
            if force_aberrant is not None:
                is_aberrant = True
        else:
            is_null = random.random() < 0.002  # 0.2% de chance d'avoir une donnée nulle
            is_aberrant = random.random() < 0.001  # 0.1% de chance d'avoir une donnée aberrante

        if is_null:
            visitors = None
            sales = None
        else:
            # Générer le nombre de visiteurs basé sur l'heure
            if hour < 8:
                visitors = random.randint(int(round(max_visitors*0.05, 0)), int(round(max_visitors*0.35, 0)))
                sales = random.randint(int(round(max_sales*0.05, 0)), int(round(max_sales*0.35, 0)))
            elif 8 <= hour < 20:
                visitors = random.randint(int(round(max_visitors*0.2, 0)), int(round(max_visitors*0.8, 0)))
                sales = random.randint(int(round(max_sales * 0.2, 0)), int(round(max_sales*0.8, 0)))
            else:
                visitors = random.randint(int(round(max_visitors*0.05, 0)), int(round(max_visitors*0.3, 0)))
                sales = random.randint(int(round(max_sales*0.05, 0)), int(round(max_sales*0.3, 0)))

        if is_aberrant:
            visitors = random.randint(10000, 50000)

        # Appliquer la capacité maximale du magasin
        if visitors is not None and visitors > store['capacity'] and not is_aberrant:
            visitors = store['capacity']

    if day_of_week in [5, 6]:  # Le week-end a tendance à avoir plus de monde
        visitors = int(visitors * 1.25) if visitors is not None else None
        sales = int(sales * 1.15) if sales is not None else None

    # Générer des ventes pour chaque heure (en fonction de la date et du nombre de ventes)
    sale_generator = SaleGenerator(date_str=date_str, num_sales=sales, store=store, hour=hour, data_dir=data_dir)
    sale_generator.generate_sales()
    sale_generator.save_sales_to_file()

    return {
        'store_id': store['id'],
        'store_name': store['name'],
        'date': date_str,
        'hour': hour,
        'visitors': visitors,
        'sales': sales
    }


class RetailDataGenerator:
    def __init__(self, data_dir='data'):
        self.data_dir = data_dir

        # Charger les magasins depuis le fichier JSON
        self.stores = load_stores(self.data_dir)

    def generate_data_day(self, date_str, is_test=None):
        data = []
        for store in self.stores:
            if is_test:
                for hour in range(1):
                    # Générer des données de retail pour une heure lors de tests
                    data.append(generate_data(date_str, hour, store, data_dir=self.data_dir))
            else:
                for hour in range(24):
                    # Générer des données de retail pour chaque heure
                    data.append(generate_data(date_str, hour, store, data_dir=self.data_dir))

        # Sauvegarder les données dans un fichier JSON
        self.save_data_to_file(data)

    def save_data_to_file(self, data):
        """Sauvegarde les données générées dans un fichier JSON."""
        file_name = os.path.join(self.data_dir, 'retail_data.json')
        if os.path.exists(file_name):
            with open(file_name, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
            existing_data.extend(data)
            with open(file_name, 'w', encoding='utf-8') as f:
                assert isinstance(f, TextIOWrapper)
                json.dump(existing_data, f, ensure_ascii=False, indent=4)
        else:
            with open(file_name, 'w', encoding='utf-8') as f:
                assert isinstance(f, TextIOWrapper)
                json.dump(data, f, ensure_ascii=False, indent=4)


# Exemple d'utilisation
if __name__ == "__main__":
    generator = RetailDataGenerator()
    date_test = "2024-12-14"  # Exemple de date
    generator.generate_data_day(date_test)
    print(f"Données générées et sauvegardées dans 'data/retail_data.json' et 'data/sales.json' pour la date {date_test}")
