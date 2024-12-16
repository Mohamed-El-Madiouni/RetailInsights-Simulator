import os
import json
import pandas as pd
import duckdb
from pandas._testing import assert_frame_equal
from src.API.retail_data_generator import RetailDataGenerator, generate_data
from src.API.product_generator import ProductGenerator
from src.API.client_generator import ClientGenerator
from src.API.store_generator import StoreGenerator


# Test la méthode de génération de produits
def test_generate_products():
    # Utiliser un dossier de données spécifique pour les tests
    test_data_dir = os.path.join('data_test')

    # Créer une instance avec le dossier de test
    product_generator = ProductGenerator(data_dir=test_data_dir)
    product_generator.generate_products()
    product_generator.save_products()

    # Vérifier que le fichier a été créé dans le bon dossier
    test_file_path = os.path.join(test_data_dir, 'products.json')
    assert os.path.exists(test_file_path), f"Le fichier n'a pas été créé dans {test_file_path}"

    # Vérifier qu'il y a des produits générés
    products = product_generator.get_products()
    assert len(products) == 50
    assert all('id' in product for product in products)
    assert all('name' in product for product in products)
    assert all('category' in product for product in products)
    assert all('price' in product for product in products)


# Test la méthode de génération de clients
def test_generate_clients():
    # Utiliser un dossier de données spécifique pour les tests
    test_data_dir = os.path.join('data_test')

    # Créer une instance avec le dossier de test
    client_generator = ClientGenerator(data_dir=test_data_dir)
    client_generator.generate_clients()
    client_generator.save_clients()

    # Vérifier qu'il y a des clients générés
    clients = client_generator.get_clients()
    assert len(clients) > 0
    assert all('id' in client for client in clients)
    assert all('name' in client for client in clients)
    assert all('age' in client for client in clients)
    assert all('gender' in client for client in clients)


# Test la méthode de génération de magasins
def test_generate_stores():
    # Utiliser un dossier de données spécifique pour les tests
    test_data_dir = os.path.join('data_test')

    # Créer une instance avec le dossier de test
    store_generator = StoreGenerator(data_dir=test_data_dir)
    store_generator.generate_stores()
    store_generator.save_stores()

    # Vérifier qu'il y a des magasins générés
    stores = store_generator.get_stores()
    assert len(stores) > 0
    assert all('id' in store for store in stores)
    assert all('name' in store for store in stores)
    assert all('location' in store for store in stores)
    assert all('capacity' in store for store in stores)


# Test la génération des données pour un magasin donné
def test_generate_data_valid():
    test_data_dir = os.path.join('data_test')

    store = {
        "id": "store_1",
        "name": "Magasin_1",
        "location": "Paris",
        "capacity": 500,
        "opening_hour": "8",
        "closing_hour": "20"
    }
    date_str = "2024-12-14"

    # Vérifie si le dossier 'data_test' existe, sinon le crée
    os.makedirs('data_test', exist_ok=True)

    # Supposons un fichier JSON vide ou réparé
    with open('data_test/sales.json', 'w', encoding='utf-8') as f:
        f.write("[]")

    # Tester pour une heure valide, pendant les heures d'ouverture
    data = generate_data(date_str, 12, store, test_data_dir, None, None, 'normal_test')
    assert data["store_id"] == store["id"]
    assert data["store_name"] == store["name"]
    assert 0 <= data["visitors"] <= store["capacity"]
    assert 0 <= data["sales"] <= store["capacity"] * 0.4  # max sales = 40% capacity
    with open('data_test/sales.json', 'w', encoding='utf-8') as f:
        f.write("[]")


# Test la génération de données nulles
def test_generate_data_null():
    test_data_dir = os.path.join('data_test')

    store = {
        "id": "store_1",
        "name": "Magasin_1",
        "location": "Paris",
        "capacity": 500,
        "opening_hour": "8",
        "closing_hour": "20"
    }
    date_str = "2024-12-14"

    # Tester que des données nulles peuvent être générées
    for hour in range(8, 20):
        data = generate_data(date_str, hour, store, test_data_dir, 'force', None)
        assert data['visitors'] is None
        assert data['sales'] is None
    with open('data_test/sales.json', 'w', encoding='utf-8') as f:
        f.write("[]")


# Test la génération de données aberrantes
def test_generate_data_aberrant():
    test_data_dir = os.path.join('data_test')

    store = {
        "id": "store_1",
        "name": "Magasin_1",
        "location": "Paris",
        "capacity": 500,
        "opening_hour": "8",
        "closing_hour": "20"
    }
    date_str = "2024-12-14"

    # Tester la génération de données aberrantes
    for hour in range(8, 20):
        data = generate_data(date_str, hour, store, test_data_dir, None, 'force')
        assert data['visitors'] > store['capacity']
    with open('data_test/sales.json', 'w', encoding='utf-8') as f:
        f.write("[]")


# Test la création d'une journée complète de données pour un magasin
def test_generate_data_day():
    test_data_dir = os.path.join('data_test')
    file_name = os.path.join(test_data_dir, 'retail_data.json')
    generator = RetailDataGenerator(test_data_dir)
    date_test = "2024-12-14"

    # Générer les données pour une journée complète
    generator.generate_data_day(date_test, 'test')

    # Vérifie si le dossier 'data_test' existe, sinon le crée
    os.makedirs('data_test', exist_ok=True)

    # Vérifier que les données ont bien été générées
    with open(file_name, 'r', encoding='utf-8') as f:
        data = json.load(f)
        assert len(data) > 0  # S'assurer que des données ont été générées
        assert all('store_id' in record for record in data)  # Vérifier que le 'store_id' existe
        assert all(
            'visitors' in record and 'sales' in record for record in data)  # Vérifier que les données sont complètes


def test_validate_sales_consistency():
    retail = pd.read_json("data_test/retail_data.json")
    stores = pd.read_json("data_test/stores.json")
    sales = pd.read_json("data_test/sales.json")
    query1 = """
    SELECT
        store_id,
        count(distinct sale_id) as total_sales
    from sales
    group by
        store_id,
    having total_sales not null
    order by total_sales, store_id
    """
    df1 = duckdb.query(query1).df()

    query2 = """
    SELECT
        r.store_id as store_id,
        sum(r.sales) as total_sales
    from retail r
    left join stores s on r.store_id=s.id
    where r.hour >= s.opening_hour and r.hour < s.closing_hour
    group by
        store_id,
    having total_sales not null
    order by total_sales, store_id
    """
    df2 = duckdb.query(query2).df()
    assert_frame_equal(df1, df2, check_dtype=False)
