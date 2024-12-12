# src/retail_data_generator.py
import random
from datetime import datetime


class RetailDataGenerator:
    def __init__(self, max_visitors=1000, min_visitors=10, max_sales=100, min_sales=1):
        """
        Initialisation de la classe avec un nombre maximum et minimum de visiteurs et de ventes.
        """
        self.max_visitors = max_visitors
        self.min_visitors = min_visitors
        self.max_sales = max_sales
        self.min_sales = min_sales

    def generate_data(self, date_str, hour):
        """
        Génère un nombre de visiteurs et de ventes basé sur l'heure de la journée.
        Le nombre de visiteurs et de ventes varie selon l'heure.
        """
        # Convertir la date passée en argument en un objet datetime
        date = datetime.strptime(date_str, "%Y-%m-%d")
        day_of_week = date.weekday()  # 0 = lundi, 6 = dimanche

        # Générer le nombre de visiteurs basé sur l'heure
        if 0 <= hour < 7:  # De minuit à 6h, pas trafic
            visitors = 0
            sales = 0
        elif 7 <= hour < 8:  # Le matin avant 8h, peu de trafic
            visitors = random.randint(100, 200)
            sales = random.randint(5, 20)
        elif 8 <= hour < 20:  # Période de grande affluence (de 8h à 19h)
            visitors = random.randint(300, self.max_visitors)
            sales = random.randint(30, self.max_sales)
        elif 20 <= hour < 22:  # Après 19h, trafic plus faible
            visitors = random.randint(self.min_visitors, 150)
            sales = random.randint(5, 25)
        else: # Après 22h, fermeture du magasin
            visitors = 0
            sales = 0

        # Pour rendre l'exemple plus crédible, augmenter les visites et ventes le week-end
        if day_of_week in [5, 6]:  # Le week-end a tendance à avoir plus de monde
            visitors = int(visitors * 1.25)  # Augmenter le trafic le week-end
            sales = int(sales * 1.15)  # Augmenter les ventes le week-end

        return visitors, sales

    def generate_data_day(self, date_str):
        for hour in range(24):  # Générer des données pour chaque heure de la journée
            visitors, sales = generator.generate_data(date_str, hour)
            print(f"{hour}h-{hour + 1}h : {visitors} visiteurs, {sales} ventes")


# Exemple d'utilisation
if __name__ == "__main__":
    generator = RetailDataGenerator(max_visitors=1000, min_visitors=50, max_sales=200, min_sales=5)

    # Tester la génération de données pour un jour et une heure donnés
    date_test = "2024-12-14"  # Exemple de date
    generator.generate_data_day(date_test)
