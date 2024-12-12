from faker import Faker
import random


class ClientGenerator:
    def __init__(self):
        self.fake = Faker()
        self.clients = []

    def generate_clients(self, num_clients=20000):
        """
        Génère une liste de clients avec des informations aléatoires et une carte de fidélité.
        """
        for _ in range(num_clients):
            name = self.fake.name()

            # Vérifier s'il y a une erreur de saisie
            if random.random() < 0.0002:  # 0.02% de chance d'erreur
                # Cas d'erreur : âge entre 200 et 1000
                age = random.randint(200, 1000)
            else:
                # Sinon, générer un âge réaliste entre 18 et 80
                age = random.randint(18, 80)

            gender = random.choice(['Homme', 'Femme'])
            has_loyalty_card = random.random() < 0.3  # 30% des clients ont une carte de fidélité
            self.clients.append({
                'id': self.fake.uuid4(),
                'name': name,
                'age': age,
                'gender': gender,
                'loyalty_card': has_loyalty_card
            })

    def get_clients(self):
        return self.clients


# Exemple d'utilisation
if __name__ == "__main__":
    client_generator = ClientGenerator()
    client_generator.generate_clients()
    for client in client_generator.get_clients():
        print(f"ID: {client['id']}, Nom: {client['name']}, Age: {client['age']}, Genre: {client['gender']}, "
              f"Carte de fidélité: {client['loyalty_card']}")
