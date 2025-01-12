import json
import os
import tempfile
from datetime import datetime
from io import BytesIO, TextIOWrapper
from unittest.mock import MagicMock, mock_open, patch

import duckdb
import pytest

from src.API.client_generator import ClientGenerator
from src.API.product_generator import ProductGenerator
from src.API.retail_data_generator import (RetailDataGenerator, generate_data,
                                           get_current_date, load_stores)
from src.API.sale_generator import (SaleGenerator, generate_random_time,
                                    load_clients, load_products)
from src.API.store_generator import StoreGenerator


# Test la méthode de génération de produits
def test_generate_products():
    """
    Teste la génération de produits avec la classe ProductGenerator.
    Vérifie que les produits générés contiennent les informations nécessaires et que le fichier
    est correctement sauvegardé.
    """
    # Utiliser un dossier de données spécifique pour les tests
    test_data_dir = os.path.join("data_test")

    # Créer une instance avec le dossier de test
    product_generator = ProductGenerator(data_dir=test_data_dir)
    product_generator.generate_products()
    product_generator.save_products()

    # Vérifier que le fichier a été créé dans le bon dossier
    test_file_path = os.path.join(test_data_dir, "products.json")
    assert os.path.exists(
        test_file_path
    ), f"Le fichier n'a pas été créé dans {test_file_path}"

    # Vérifier qu'il y a des produits générés
    products = product_generator.get_products()
    assert len(products) == 50
    assert all("id" in product for product in products)
    assert all("name" in product for product in products)
    assert all("category" in product for product in products)
    assert all("price" in product for product in products)


# Test la méthode de génération de clients
def test_generate_clients():
    """
    Teste la génération de clients avec la classe ClientGenerator.
    Vérifie que les clients générés contiennent les informations nécessaires et que le fichier
    est correctement sauvegardé.
    """
    # Utiliser un dossier de données spécifique pour les tests
    test_data_dir = os.path.join("data_test")

    # Créer une instance avec le dossier de test
    client_generator = ClientGenerator(data_dir=test_data_dir)
    client_generator.generate_clients()
    client_generator.save_clients()

    # Vérifier qu'il y a des clients générés
    clients = client_generator.get_clients()
    assert len(clients) > 0
    assert all("id" in client for client in clients)
    assert all("name" in client for client in clients)
    assert all("age" in client for client in clients)
    assert all("gender" in client for client in clients)


# Test la méthode de génération de magasins
def test_generate_stores():
    """
    Teste la génération de magasins avec la classe StoreGenerator.
    Vérifie que les magasins générés contiennent les informations nécessaires et que le fichier
    est correctement sauvegardé.
    """
    # Utiliser un dossier de données spécifique pour les tests
    test_data_dir = os.path.join("data_test")

    # Créer une instance avec le dossier de test
    store_generator = StoreGenerator(data_dir=test_data_dir)
    store_generator.generate_stores()
    store_generator.save_stores()

    # Vérifier qu'il y a des magasins générés
    stores = store_generator.get_stores()
    assert len(stores) > 0
    assert all("id" in store for store in stores)
    assert all("name" in store for store in stores)
    assert all("location" in store for store in stores)
    assert all("capacity" in store for store in stores)


# Test la génération des données pour un magasin donné
def test_generate_data_valid():
    """
    Teste la génération de données retail valides pour un magasin donné.
    Vérifie que les données générées respectent les contraintes de capacité et d'heures d'ouverture du magasin.
    """
    test_data_dir = os.path.join("data_test")

    store = {
        "id": "store_1",
        "name": "Magasin_1",
        "location": "Paris",
        "capacity": 500,
        "opening_hour": "8",
        "closing_hour": "20",
    }
    date_str = "2024-12-14"

    # Vérifie si le dossier 'data_test' existe, sinon le crée
    os.makedirs("data_test", exist_ok=True)

    # Supposons un fichier JSON vide ou réparé
    with open("data_test/sales.json", "w", encoding="utf-8") as f:
        f.write("[]")

    # Tester pour une heure valide, pendant les heures d'ouverture
    data, _ = generate_data(
        date_str, 12, store, test_data_dir, None, None, "normal_test"
    )
    assert data["store_id"] == store["id"]
    assert data["store_name"] == store["name"]
    assert 0 <= data["visitors"] <= store["capacity"]
    assert 0 <= data["sales"] <= store["capacity"] * 0.4  # max sales = 40% capacity
    with open("data_test/sales.json", "w", encoding="utf-8") as f:
        f.write("[]")


# Test la génération de données nulles
def test_generate_data_null():
    """
    Teste la génération de données nulles pour un magasin.
    Vérifie que les visiteurs et les ventes sont définis comme `None` lorsqu'une contrainte impose des données nulles.
    """
    test_data_dir = os.path.join("data_test")

    store = {
        "id": "store_1",
        "name": "Magasin_1",
        "location": "Paris",
        "capacity": 500,
        "opening_hour": "8",
        "closing_hour": "20",
    }
    date_str = "2024-12-14"

    # Tester que des données nulles peuvent être générées
    for hour in range(8, 20):
        data, _ = generate_data(date_str, hour, store, test_data_dir, "force", None)
        assert data["visitors"] is None
        assert data["sales"] is None
    with open("data_test/sales.json", "w", encoding="utf-8") as f:
        f.write("[]")


# Test la génération de données aberrantes
def test_generate_data_aberrant():
    """
    Teste la génération de données aberrantes pour un magasin.
    Vérifie que les données générées dépassent les contraintes de capacité du magasin lorsque des données
    aberrantes sont forcées.
    """
    test_data_dir = os.path.join("data_test")

    store = {
        "id": "store_1",
        "name": "Magasin_1",
        "location": "Paris",
        "capacity": 500,
        "opening_hour": "8",
        "closing_hour": "20",
    }
    date_str = "2024-12-14"

    # Tester la génération de données aberrantes
    for hour in range(8, 20):
        data, _ = generate_data(date_str, hour, store, test_data_dir, None, "force")
        assert data["visitors"] > store["capacity"]
    with open("data_test/sales.json", "w", encoding="utf-8") as f:
        f.write("[]")


# Test la création d'une journée complète de données pour un magasin
def test_gen_data_day():
    """
    Teste la génération de données retail pour une journée complète avec RetailDataGenerator.
    Vérifie que les données générées sont complètes et conformes pour chaque magasin.
    """
    test_data_dir = os.path.join("data_test")
    file_name = os.path.join(test_data_dir, "retail_data.json")
    generator = RetailDataGenerator(test_data_dir)
    date_test = "2024-12-14"

    # Générer les données pour une journée complète
    generator.generate_data_day(date_test, "test")

    # Vérifie si le dossier 'data_test' existe, sinon le crée
    os.makedirs("data_test", exist_ok=True)

    # Vérifier que les données ont bien été générées
    with open(file_name, "r", encoding="utf-8") as f:
        data = json.load(f)
        assert len(data) > 0  # S'assurer que des données ont été générées
        assert all(
            "store_id" in record for record in data
        )  # Vérifier que le 'store_id' existe
        assert all(
            "visitors" in record and "sales" in record for record in data
        )  # Vérifier que les données sont complètes


def test_validate_sales_consistency():
    """
    Teste la cohérence des données de ventes entre les fichiers `sales.json` et `retail_data.json`.
    Compare les totaux des ventes calculés à partir des deux fichiers en utilisant DuckDB.
    """
    # Créer un répertoire temporaire pour stocker les fichiers
    with tempfile.TemporaryDirectory() as temp_dir:
        # Chemins des fichiers temporaires
        sales_file = os.path.join(temp_dir, "sales.json")
        retail_data_file = os.path.join(temp_dir, "retail_data.json")
        stores_file = os.path.join(temp_dir, "stores.json")

        # Contenu simulé pour les fichiers
        mock_sales_data = (
            '[{"sale_id": "1", "store_id": "1"}, {"sale_id": "2", "store_id": "1"}]'
        )
        mock_retail_data = '[{"store_id": "1", "sales": 2, "hour": 10}]'
        mock_stores_data = '[{"id": "1", "opening_hour": "9", "closing_hour": "18"}]'

        # Écrire les fichiers temporaires
        with open(sales_file, "w", encoding="utf-8") as f:
            f.write(mock_sales_data)
        with open(retail_data_file, "w", encoding="utf-8") as f:
            f.write(mock_retail_data)
        with open(stores_file, "w", encoding="utf-8") as f:
            f.write(mock_stores_data)

        # Requête DuckDB pour les ventes
        query1 = f"""
        SELECT
            store_id,
            count(distinct sale_id) as total_sales
        from read_json_auto('{sales_file}')
        group by
            store_id
        having total_sales not null
        order by total_sales, store_id
        """
        df1 = duckdb.query(query1).df()

        query2 = f"""
        SELECT
            r.store_id as store_id,
            sum(r.sales) as total_sales
        from read_json_auto('{retail_data_file}') r
        left join read_json_auto('{stores_file}') s on r.store_id=s.id
        where CAST(r.hour as INT) >= cast(s.opening_hour as INT) and cast(r.hour as INT) < cast(s.closing_hour as INT)
        group by
            store_id
        having total_sales not null
        order by total_sales, store_id
        """
        df2 = duckdb.query(query2).df()

        # Vérification
        from pandas.testing import assert_frame_equal

        assert_frame_equal(df1, df2, check_dtype=False)


@patch("os.makedirs")
@patch("os.path.exists")
@patch(
    "src.API.sale_generator.load_products",
    return_value=[{"product_id": "1", "name": "Product A", "price": 10}],
)
@patch(
    "src.API.sale_generator.load_clients",
    return_value=[{"client_id": "1", "name": "Client A", "city": "Paris"}],
)
def test_save_sales_to_file(
    mock_load_clients, mock_load_products, mock_exists, mock_makedirs
):
    """
    Teste la méthode `save_sales_to_file` pour s'assurer que les ventes
    sont correctement sauvegardées dans un fichier JSON, en simulant le système de fichiers.
    """
    # Mock pour éviter toute interaction avec le système de fichiers
    mock_exists.return_value = False
    mock_makedirs.return_value = None

    # Utiliser un fichier temporaire pour simuler l'écriture
    with tempfile.NamedTemporaryFile(
        delete=False, mode="w", encoding="utf-8"
    ) as temp_file:
        temp_file_path = temp_file.name

    try:
        # Sauvegarder une référence à la fonction `open` originale
        original_open = open

        # Rediriger `open` pour n'affecter que le chemin spécifique
        def mocked_open(file, mode="r", encoding=None):
            if file == os.path.join("data_api", "sales.json"):
                return original_open(temp_file_path, mode, encoding=encoding)
            return original_open(file, mode, encoding=encoding)

        with patch("builtins.open", new=mocked_open):
            # Initialisation d'un objet SaleGenerator
            sale_generator = SaleGenerator(
                date_str="2023-12-01", num_sales=10, store=MagicMock(), hour=15
            )
            sale_generator.sales = [{"sale_id": "1", "amount": 100}]

            # Appel de la méthode
            sale_generator.save_sales_to_file()

            # Vérifications
            mock_makedirs.assert_called_once_with(
                sale_generator.data_dir, exist_ok=True
            )
            with original_open(temp_file_path, "r", encoding="utf-8") as f:
                saved_data = f.read()
                assert '"sale_id": "1",' in saved_data
                assert '"amount": 100' in saved_data

    finally:
        # Nettoyage du fichier temporaire
        os.remove(temp_file_path)


def test_get_current_date():
    """
    Teste que la méthode `get_current_date` retourne la date actuelle
    correctement formatée sous la forme 'YYYY-MM-DD'.
    """
    # Mock de la classe `datetime`
    with patch("src.API.retail_data_generator.datetime") as mock_datetime:
        # Configurer le mock pour simuler `datetime.now`
        mock_datetime.now.return_value = datetime(2023, 12, 1)

        # Vérification
        assert get_current_date() == "2023-12-01"


@patch("os.makedirs")
@patch("os.path.exists")
def test_save_products(mock_exists, mock_makedirs):
    """
    Teste que la méthode `save_products` sauvegarde correctement les produits dans un fichier JSON
    et que le fichier contient les données attendues.
    """
    mock_exists.return_value = False

    # Utiliser un fichier temporaire pour simuler l'écriture
    with tempfile.NamedTemporaryFile(
        delete=False, mode="w", encoding="utf-8"
    ) as temp_file:
        temp_file_path = temp_file.name

    try:
        # Rediriger `open` pour n'affecter que le chemin du fichier attendu
        original_open = open  # Garder une référence à la fonction `open` originale

        def mocked_open(file, mode="r", encoding=None, *args, **kwargs):
            if file == os.path.join("data_api", "products.json"):
                return original_open(
                    temp_file_path, mode, encoding=encoding, *args, **kwargs
                )
            return original_open(file, mode, encoding=encoding, *args, **kwargs)

        with patch("builtins.open", new=mocked_open):
            # Initialisation d'un ProductGenerator
            product_generator = ProductGenerator()
            product_generator.products = [{"product_id": "1", "name": "Product A"}]

            # Appel de la méthode
            product_generator.save_products()

            # Vérifications
            mock_makedirs.assert_called_once_with(
                product_generator.data_dir, exist_ok=True
            )
            with open(temp_file_path, "r", encoding="utf-8") as f:
                # Vérifier que le fichier est considéré comme un TextIOWrapper
                assert isinstance(f, TextIOWrapper)
                # Vérifier le contenu du fichier
                content = f.read()
                assert '"product_id": "1"' in content
                assert '"name": "Product A"' in content
    finally:
        # Supprimer le fichier temporaire après le test
        os.remove(temp_file_path)


@patch("builtins.open", side_effect=FileNotFoundError)
def test_load_stores_file_not_found(mock_open):
    """
    Teste que la fonction `load_stores` retourne une liste vide lorsque le fichier n'existe pas.
    """
    result = load_stores("non_existent_dir")
    assert result == []


def test_generate_data_store_closed():
    """
    Teste que la fonction `generate_data` retourne des ventes et visiteurs nuls
    lorsque le magasin est fermé (heure en dehors des heures d'ouverture).
    """
    store = {
        "id": "1",
        "name": "Store A",
        "opening_hour": "10",
        "closing_hour": "18",
        "capacity": 100,
    }
    result, _ = generate_data("2023-12-01", 9, store, "data_api")
    assert result["visitors"] == 0
    assert result["sales"] == 0


def test_generate_data_early_hours():
    """
    Teste que la fonction `generate_data` génère des visiteurs et des ventes
    dans des limites attendues tôt le matin, lorsque le magasin est ouvert.
    """
    store = {
        "id": "1",
        "name": "Store A",
        "opening_hour": "8",
        "closing_hour": "22",
        "capacity": 100,
    }
    result, _ = generate_data("2023-12-01", 7, store, "data_api")
    assert 0 <= result["visitors"] <= 35
    assert 0 <= result["sales"] <= 35


def test_generate_data_late_hours():
    """
    Teste que la fonction `generate_data` génère des visiteurs et des ventes
    dans des limites attendues tard le soir, lorsque le magasin est encore ouvert.
    """
    store = {
        "id": "1",
        "name": "Store A",
        "opening_hour": "8",
        "closing_hour": "22",
        "capacity": 100,
        "location": "Paris",
    }
    result, _ = generate_data("2023-12-01", 21, store, "data_api")
    assert 0 <= result["visitors"] <= 30
    assert 0 <= result["sales"] <= 30


def test_generate_data_exceed_capacity():
    """
    Teste que la fonction `generate_data` limite les visiteurs générés à la capacité maximale
    du magasin même si une valeur aberrante est simulée.
    """
    store = {
        "id": "1",
        "name": "Store A",
        "opening_hour": "8",
        "closing_hour": "22",
        "capacity": 100,
        "location": "Paris",
    }
    with patch(
        "random.randint", return_value=120
    ):  # Simule un dépassement de la capacité
        with patch(
            "src.API.sale_generator.SaleGenerator.generate_sales", return_value=[]
        ):  # Mock des ventes
            result, _ = generate_data("2023-12-01", 15, store, "data_api")
    assert result["visitors"] == 100  # Capacité max


@patch(
    "src.API.retail_data_generator.load_stores",
    return_value=[{"id": "1", "name": "Store A", "capacity": 100}],
)
@patch("src.API.retail_data_generator.generate_data", return_value=({}, []))
@patch("os.makedirs")
@patch("os.path.exists")
def test_generate_data_day(
    mock_exists, mock_makedirs, mock_generate_data, mock_load_stores
):
    """
    Teste que la méthode `generate_data_day` génère correctement des données de retail
    pour une journée spécifique et les sauvegarde dans un fichier.
    """
    # Répertoire temporaire pour éviter toute écriture réelle
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Temp dir: {temp_dir}")

        # Configurer le mock pour `os.path.exists`
        def mock_exists_side_effect(path):
            # Retourner True uniquement pour le répertoire temporaire
            if path == temp_dir:
                return True
            return False

        mock_exists.side_effect = mock_exists_side_effect

        # Mock `open` pour retourner un objet compatible avec TextIOWrapper
        def mock_open_wrapper(file, mode="r", encoding=None):
            print(f"Mocking file: {file}, mode: {mode}")
            if "w" in mode:
                # Simuler un fichier en écriture basé sur BytesIO pour supporter bytes
                mock_file = BytesIO()
                return TextIOWrapper(mock_file, encoding=encoding)
            elif "r" in mode:
                # Simuler un fichier vide en lecture basé sur BytesIO
                mock_file = BytesIO(b"[]")
                return TextIOWrapper(mock_file, encoding=encoding)
            else:
                raise ValueError("Unsupported mode in test")

        with patch("builtins.open", new=mock_open_wrapper):
            generator = RetailDataGenerator()
            generator.data_dir = temp_dir  # Utiliser le répertoire temporaire

            # Appeler la méthode
            generator.generate_data_day("2023-12-01")

            # Vérifier que `os.makedirs` est appelé uniquement si nécessaire
            if not mock_exists(temp_dir):
                mock_makedirs.assert_called_once_with(temp_dir, exist_ok=True)
            else:
                print(f"{temp_dir} already exists, os.makedirs not called.")


@patch("builtins.open")  # Mock explicite de `open`
@patch("os.path.exists", return_value=True)  # Mock pour simuler que le fichier existe
def test_save_retail_data_to_file_existing(mock_exists, mock_open):
    """
    Teste que la méthode `save_retail_data_to_file` sauvegarde correctement
    les données retail dans un fichier existant, en simulant les appels à `open`.
    """

    # Simuler des fichiers compatibles avec TextIOWrapper
    def mock_open_wrapper(file, mode="r", encoding=None):
        if "r" in mode:
            # Fichier JSON existant simulé
            mock_file = BytesIO(b"[]")
            return TextIOWrapper(mock_file, encoding="utf-8")
        elif "w" in mode:
            # Fichier en écriture simulé
            mock_file = BytesIO()
            return TextIOWrapper(mock_file, encoding="utf-8")
        else:
            raise ValueError("Unsupported mode")

    mock_open.side_effect = (
        mock_open_wrapper  # Utiliser le wrapper pour chaque appel d'`open`
    )

    generator = RetailDataGenerator()
    generator.retail_data = [{"store_id": "1", "date": "2023-12-01", "visitors": 100}]

    # Appel de la méthode à tester
    generator.save_retail_data_to_file()

    # Vérifier que `open` est appelé pour la lecture et l'écriture
    file_path = os.path.join(generator.data_dir, "retail_data.json")
    mock_open.assert_any_call(file_path, "r", encoding="utf-8")
    mock_open.assert_any_call(file_path, "w", encoding="utf-8")


@patch("builtins.open", side_effect=FileNotFoundError)
def test_load_products_file_not_found(mock_open):
    """
    Teste que la fonction `load_products` retourne une liste vide lorsque le fichier n'existe pas.
    """
    result = load_products("non_existent_dir")
    assert result == []


@patch("builtins.open", side_effect=FileNotFoundError)
def test_load_clients_file_not_found(mock_open):
    """
    Teste que la fonction `load_clients` retourne une liste vide lorsque le fichier n'existe pas.
    """
    result = load_clients("non_existent_dir")
    assert result == []


def test_generate_random_time_invalid_hour():
    """
    Teste que la fonction `generate_random_time` lève une exception ValueError
    lorsque l'heure fournie est en dehors de l'intervalle valide (0-23).
    """
    with pytest.raises(ValueError, match="L'heure doit être entre 0 et 23 inclus."):
        generate_random_time(25)


def test_find_client_for_store_no_client():
    """
    Teste que la méthode `_find_client_for_store` retourne `None`
    lorsqu'aucun client n'est disponible dans la ville du magasin.
    """
    sale_generator = SaleGenerator(
        date_str="2023-12-01", num_sales=10, store={}, hour=15
    )
    sale_generator.clients_by_city = {}
    store = {"location": "Paris"}
    result = sale_generator._find_client_for_store(store)
    assert result is None


@patch("builtins.open")  # Mock explicite de `open`
@patch("os.path.exists", return_value=True)  # Mock pour simuler que le fichier existe
def test_save_sales_to_file_existing(mock_exists, mock_open):
    """
    Teste que la méthode `save_sales_to_file` sauvegarde correctement les données de vente
    dans un fichier JSON existant, en simulant les appels à `open`.
    """

    # Mock d'un fichier compatible avec TextIOWrapper
    def mock_open_wrapper(file, mode="r", encoding=None):
        if "r" in mode:
            # Simuler un fichier JSON existant avec des données vides
            mock_file = BytesIO(b"[]")
            return TextIOWrapper(mock_file, encoding="utf-8")
        elif "w" in mode:
            # Simuler un fichier prêt à être écrit
            mock_file = BytesIO()
            return TextIOWrapper(mock_file, encoding="utf-8")
        else:
            raise ValueError("Unsupported mode for file")

    mock_open.side_effect = (
        mock_open_wrapper  # Utiliser le wrapper pour chaque appel à `open`
    )

    # Initialiser un `SaleGenerator`
    sale_generator = SaleGenerator(
        date_str="2023-12-01", num_sales=10, store={}, hour=15
    )
    sale_generator.sales = [{"sale_id": "1", "amount": 100}]

    # Appel de la méthode à tester
    sale_generator.save_sales_to_file()

    # Vérification des appels
    file_path = os.path.join(sale_generator.data_dir, "sales.json")
    mock_open.assert_any_call(file_path, "r", encoding="utf-8")
    mock_open.assert_any_call(file_path, "w", encoding="utf-8")
