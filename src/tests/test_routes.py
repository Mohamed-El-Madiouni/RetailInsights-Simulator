import pytest
import pytest_asyncio
from httpx import AsyncClient

from src.API.main import app


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
async def test_get_clients_valid(async_client):
    """
    Teste la route `/clients` avec une ville valide.
    Vérifie que la réponse contient une liste de clients avec les champs requis (id, name, city).
    """
    response = await async_client.get("/clients?city=Paris")
    assert response.status_code == 200
    assert isinstance(response.json(), list)
    if response.json():
        client = response.json()[0]
        assert "id" in client
        assert "name" in client
        assert "city" in client


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
    Vérifie que la réponse contient une liste de données retail avec les champs requis (store_id, date, visitors, sales).
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
