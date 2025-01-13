import json
from unittest.mock import mock_open, patch

import pytest
import pytest_asyncio
from httpx import AsyncClient

from src.api.main import app
from src.api.routes.sales_route import load_sales
from src.api.routes.stores_route import load_stores
from io import StringIO


@pytest_asyncio.fixture
async def async_client():
    """
    Fixture asynchrone pour créer un client HTTP asynchrone à utiliser dans les tests.
    Permet d'effectuer des requêtes à l'application FastAPI.
    """
    async with AsyncClient(app=app, base_url="http://test") as client:
        yield client


# Test des routes clients
@pytest.mark.asyncio
@patch("os.path.exists", return_value=True)  # Simuler que le fichier existe
@patch("builtins.open")  # Mock explicite de `open`
async def test_get_clients_valid(mock_open, mock_exists, async_client):
    """
    Teste la route `/clients` avec une ville valide.
    Vérifie que la réponse contient une liste de clients avec les champs requis (id, name, city).
    """
    # Simuler un contenu JSON valide pour le fichier `clients.json`
    file_content = (
        '[{"id": "1", "name": "John Doe", "city": "Paris", "age": 72, "gender": "Homme", "loyalty_card": true}]'
    )

    # Fournir un fichier simulé avec StringIO
    def mock_open_wrapper(file, mode="r", encoding=None):
        if "r" in mode:
            return StringIO(file_content)  # Retourner un fichier en lecture
        raise ValueError(f"Mode non supporté: {mode}")

    mock_open.side_effect = mock_open_wrapper  # Associer le wrapper personnalisé

    # Logs pour déboguer les mocks
    print(f"Mock exists called: {mock_exists.call_count} times")
    print(f"Mock open: {mock_open}")

    # Effectuer la requête de test
    try:
        print("Tentative de requête GET sur /clients?city=Paris")
        response = await async_client.get("/clients?city=Paris")
        print(f"Réponse du serveur : {response.status_code}, {response.text}")
    except Exception as e:
        print(f"Exception lors de la requête : {e}")
        raise

    # Vérification des assertions
    try:
        print("Vérification des assertions")
        assert response.status_code == 200, f"Statut attendu : 200, obtenu : {response.status_code}"
        assert isinstance(response.json(), list), f"Type attendu : list, obtenu : {type(response.json())}"
        if response.json():
            client = response.json()[0]
            print(f"Client extrait : {client}")
            assert "id" in client, "Champ 'id' manquant"
            assert "name" in client, "Champ 'name' manquant"
            assert "city" in client, "Champ 'city' manquant"
    except AssertionError as e:
        print(f"Erreur dans les assertions : {e}")
        raise

    print("Test terminé avec succès")


@pytest.mark.asyncio
async def test_get_clients_invalid(async_client):
    """
    Teste la route `/clients` avec une ville inexistante.
    Vérifie que la réponse retourne un code d'erreur 404 et un message approprié.
    """
    response = await async_client.get("/clients?city=UnknownCity")
    assert response.status_code == 404
    assert response.json() == {"error": "No clients found in city: UnknownCity"}


# Test des routes produits
@pytest.mark.asyncio
async def test_get_products(async_client):
    """
    Teste la route `/products`.
    Vérifie que la réponse contient une liste de produits avec les champs requis (id, name, category, price).
    """
    response = await async_client.get("/products")
    assert response.status_code == 200
    assert isinstance(response.json(), list)
    if response.json():
        product = response.json()[0]
        assert "id" in product
        assert "name" in product
        assert "category" in product
        assert "price" in product


# Test des routes retail data
@pytest.mark.asyncio
async def test_get_retail_data_valid(async_client):
    """
    Teste la route `/retail_data` avec une date valide.
    Vérifie que la réponse contient une liste de données retail avec les champs requis (store_id, date, visitors, sales)
    """
    response = await async_client.get("/retail_data?date=2024-12-14")
    assert response.status_code == 200
    assert isinstance(response.json(), list)
    if response.json():
        data = response.json()[0]
        assert "store_id" in data
        assert "date" in data
        assert "visitors" in data
        assert "sales" in data


@pytest.mark.asyncio
async def test_get_retail_data_invalid(async_client):
    """
    Teste la route `/retail_data` avec une date invalide.
    Vérifie que la réponse retourne un code d'erreur 400 et un message d'erreur indiquant un format de date incorrect.
    """
    response = await async_client.get("/retail_data?date=invalid-date")
    assert response.status_code == 400
    assert response.json() == {"error": "Date format is incorrect. Use 'YYYY-MM-DD'."}


def test_load_sales_existing_file():
    """
    Teste que la fonction `load_sales` charge correctement les données d'un fichier JSON existant.
    """
    # Contenu simulé du fichier sales.json
    mock_sales_data = [
        {"sale_id": "1", "sale_date": "2023-12-01", "store_id": "store_1"}
    ]
    with patch("builtins.open", mock_open(read_data=json.dumps(mock_sales_data))):
        sales = load_sales()
        assert sales == mock_sales_data


def test_load_sales_file_not_found():
    """
    Teste que la fonction `load_sales` retourne une liste vide lorsqu'un fichier JSON est introuvable.
    """
    # Simuler l'absence du fichier
    with patch("builtins.open", side_effect=FileNotFoundError):
        sales = load_sales()
        assert sales == []  # Doit retourner une liste vide


@pytest.mark.asyncio
async def test_get_sales_valid():
    """
    Teste que la route `GET /sales` retourne correctement les ventes
    correspondant à une date et un magasin spécifiques.
    """
    mock_sales_data = [
        {
            "sale_id": "1",
            "sale_date": "2023-12-01",
            "store_id": "store_1",
            "nb_type_product": 1,
            "product_id": "product_1",
            "client_id": "client_1",
            "quantity": 10,
            "sale_amount": 100.0,
            "sale_time": "10:00",
        },
        {
            "sale_id": "2",
            "sale_date": "2023-12-01",
            "store_id": "store_2",
            "nb_type_product": 2,
            "product_id": "product_2",
            "client_id": "client_2",
            "quantity": 20,
            "sale_amount": 200.0,
            "sale_time": "11:00",
        },
    ]
    with patch("src.api.routes.sales_route.load_sales", return_value=mock_sales_data):
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get("/sales?sale_date=2023-12-01&store_id=store_1")
            assert response.status_code == 200
            assert response.json() == [
                {
                    "sale_id": "1",
                    "sale_date": "2023-12-01",
                    "store_id": "store_1",
                    "nb_type_product": 1,
                    "product_id": "product_1",
                    "client_id": "client_1",
                    "quantity": 10,
                    "sale_amount": 100.0,
                    "sale_time": "10:00",
                }
            ]


@pytest.mark.asyncio
async def test_get_sales_no_match():
    """
    Teste que la route `GET /sales` retourne une erreur si aucune vente ne correspond
    à la date ou au magasin spécifiés.
    """
    mock_sales_data = [
        {"sale_id": "1", "sale_date": "2023-12-01", "store_id": "store_1"},
    ]
    with patch("src.api.routes.sales_route.load_sales", return_value=mock_sales_data):
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get("/sales?sale_date=2023-12-02&store_id=store_1")

            # Vérifier le code de statut
            assert response.status_code == 200

            # Vérifier le contenu de la réponse
            json_response = response.json()
            assert isinstance(
                json_response, list
            )  # Doit être un dictionnaire en cas d'erreur
            assert json_response == [
                {"error": "No sales found for store ID: store_1 on 2023-12-02"}
            ]


@pytest.mark.asyncio
async def test_get_sales_by_hour_valid():
    """
    Teste que la route `GET /sales/hour` retourne correctement les ventes
    correspondant à une heure spécifique pour un magasin.
    """
    mock_sales_data = [
        {
            "sale_id": "1",
            "sale_date": "2023-12-01",
            "store_id": "store_1",
            "sale_time": "14:00",
            "product_id": "prod_1",
            "client_id": "client_1",
            "quantity": 5,
            "sale_amount": 100.0,
            "nb_type_product": 3,
        },
        {
            "sale_id": "2",
            "sale_date": "2023-12-01",
            "store_id": "store_1",
            "sale_time": "15:00",
            "product_id": "prod_2",
            "client_id": "client_2",
            "quantity": 3,
            "sale_amount": 50.0,
            "nb_type_product": 2,
        },
    ]

    with patch("src.api.routes.sales_route.load_sales", return_value=mock_sales_data):
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get("/sales/hour?sale_date=2023-12-01&hour=14")
            assert response.status_code == 200
            assert response.json() == [
                {
                    "sale_id": "1",
                    "sale_date": "2023-12-01",
                    "store_id": "store_1",
                    "sale_time": "14:00",
                    "product_id": "prod_1",
                    "client_id": "client_1",
                    "quantity": 5,
                    "sale_amount": 100.0,
                    "nb_type_product": 3,
                }
            ]


@pytest.mark.asyncio
async def test_get_sales_by_hour_no_match():
    """
    Teste que la route `GET /sales/hour` retourne une erreur si aucune vente ne correspond
    à l'heure spécifiée pour un magasin.
    """
    mock_sales_data = [
        {
            "sale_id": "1",
            "sale_date": "2023-12-01",
            "store_id": "store_1",
            "sale_time": "14:00",
        },
    ]
    with patch("src.api.routes.sales_route.load_sales", return_value=mock_sales_data):
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get("/sales/hour?sale_date=2023-12-01&hour=15")
            assert response.status_code == 200
            assert response.json() == [
                {"error": "No sales found for hour: 15 on 2023-12-01"}
            ]


# Test de la fonction `load_stores`
def test_load_stores_valid():
    """
    Teste que la fonction `load_stores` charge correctement les données d'un fichier JSON valide.
    """
    mock_data = '[{"id": "1", "name": "Store A", "location": "Paris"}]'
    with patch("builtins.open", mock_open(read_data=mock_data)):
        result = load_stores()
        assert result == [{"id": "1", "name": "Store A", "location": "Paris"}]


def test_load_stores_file_not_found():
    """
    Teste que la fonction `load_stores` retourne une liste vide lorsque le fichier JSON des magasins est introuvable.
    """
    with patch("builtins.open", side_effect=FileNotFoundError):
        result = load_stores()
        assert result == []


# Test de la route `get_stores`
@pytest.mark.asyncio
async def test_get_stores_valid():
    """
    Teste que la route `GET /stores` retourne correctement les données des magasins.
    """
    mock_stores_data = [
        {
            "id": "1",
            "name": "Store A",
            "location": "Paris",
            "capacity": 760,
            "opening_hour": "8",
            "closing_hour": "22",
        }
    ]
    with patch(
        "src.api.routes.stores_route.load_stores", return_value=mock_stores_data
    ):
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get("/stores")
            assert response.status_code == 200
            assert response.json() == mock_stores_data


@pytest.mark.asyncio
async def test_get_stores_file_not_found():
    """
    Teste que la route `GET /stores` retourne une erreur si le fichier des magasins est introuvable.
    """
    with patch(
        "src.api.routes.stores_route.load_stores", side_effect=FileNotFoundError
    ):
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get("/stores")
            assert response.status_code == 200
            assert response.json() == [{"error": "Stores data file not found."}]


# Test de la route `get_store_visitors`
@pytest.mark.asyncio
async def test_get_store_visitors_valid():
    """
    Teste que la route `GET /retail_data/store` retourne correctement les données de visiteurs
    pour un magasin spécifique à une date donnée.
    """
    mock_retail_data = [
        {
            "store_id": "1",
            "store_name": "Store A",
            "date": "2023-12-01",
            "hour": 14,
            "visitors": 100,
            "sales": 50,
        }
    ]
    with patch(
        "src.api.routes.retail_data_route.load_retail_data",
        return_value=mock_retail_data,
    ):
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get("/retail_data/store?date=2023-12-01&store_id=1")
            assert response.status_code == 200
            assert response.json() == mock_retail_data


@pytest.mark.asyncio
async def test_get_store_visitors_invalid_date():
    """
    Teste que la route `GET /retail_data/store` retourne une erreur lorsque la date est invalide.
    """
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/retail_data/store?date=invalid-date&store_id=1")
        assert response.status_code == 200
        assert response.json() == [
            {"error": "Date format is incorrect. Use 'YYYY-MM-DD'."}
        ]


@pytest.mark.asyncio
async def test_get_store_visitors_file_not_found():
    """
    Teste que la route `GET /retail_data/store` retourne une erreur si le fichier des données retail est introuvable.
    """
    with patch(
        "src.api.routes.retail_data_route.load_retail_data",
        side_effect=FileNotFoundError,
    ):
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get("/retail_data/store?date=2023-12-01&store_id=1")
            assert response.status_code == 200
            assert response.json() == [{"error": "Retail data file not found."}]


@pytest.mark.asyncio
async def test_get_store_visitors_no_match():
    """
    Teste que la route `GET /retail_data/store` retourne une erreur si aucune donnée de visiteurs
    ne correspond au magasin ou à la date spécifiés.
    """
    mock_retail_data = [
        {
            "store_id": "1",
            "store_name": "Store A",
            "date": "2023-12-01",
            "hour": 14,
            "visitors": 100,
            "sales": 50,
        }
    ]
    with patch(
        "src.api.routes.retail_data_route.load_retail_data",
        return_value=mock_retail_data,
    ):
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get("/retail_data/store?date=2023-12-02&store_id=2")
            assert response.status_code == 200
            assert response.json() == [
                {"error": "No retail data found for store ID: 2 on 2023-12-02."}
            ]
