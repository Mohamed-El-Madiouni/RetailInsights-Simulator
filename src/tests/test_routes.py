import pytest
import pytest_asyncio
from httpx import AsyncClient

from src.API.main import app


@pytest_asyncio.fixture
async def async_client():
    async with AsyncClient(app=app, base_url="http://test") as client:
        yield client


# Test des routes clients
@pytest.mark.asyncio
async def test_get_clients_valid(async_client):
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
    response = await async_client.get("/clients?city=UnknownCity")
    assert response.status_code == 404
    assert response.json() == {"error": "No clients found in city: UnknownCity"}


# Test des routes produits
@pytest.mark.asyncio
async def test_get_products(async_client):
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
    response = await async_client.get("/retail_data?date=invalid-date")
    assert response.status_code == 400
    assert response.json() == {"error": "Date format is incorrect. Use 'YYYY-MM-DD'."}
