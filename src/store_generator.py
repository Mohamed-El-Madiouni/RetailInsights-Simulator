import random
import uuid


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

    def get_stores(self):
        return self.stores


# Exemple d'utilisation
if __name__ == "__main__":
    store_generator = StoreGenerator()
    store_generator.generate_stores()
    for store in store_generator.get_stores():
        print(f"ID: {store['id']}, Name: {store['name']}, Location: {store['location']}, "
              f"Capacity: {store['capacity']}, Opening Hour: {store['opening_hour']}, "
              f"Closing Hour: {store['closing_hour']}")
