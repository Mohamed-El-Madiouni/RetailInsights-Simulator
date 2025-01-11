from unittest.mock import patch

import pytest
import pytest_asyncio
from httpx import AsyncClient

from src.API.main import app
from src.data_processing.extract.extract_clients import fetch_and_save_clients
from src.data_processing.extract.utils import save_to_s3


# Mock pour save_to_s3
@pytest.fixture
def mock_save_to_s3():
    """Fixture pour mocker save_to_s3."""
    with patch("src.data_processing.extract.extract_clients.save_to_s3") as mock:
        yield mock


# Test de l'extraction et sauvegarde des clients
def test_fetch_and_save_clients(mock_save_to_s3):
    """
    Teste la fonction fetch_and_save_clients pour s'assurer que les données
    des clients sont correctement extraites et sauvegardées sur S3.
    """
    # Mock des villes retournées par fetch_cities
    with patch(
        "src.data_processing.extract.extract_clients.fetch_cities",
        return_value=["Paris", "Lyon", "Nice"],
    ):
        # Mock pour les appels à fetch_from_api
        with patch(
            "src.data_processing.extract.utils.fetch_from_api"
        ) as mock_fetch_from_api:
            # Définir les réponses mockées en fonction de l'URL
            mock_fetch_from_api.side_effect = lambda url: (
                [{"id": "1", "name": "Client A", "city": "Paris"}]
                if "Paris" in url
                else (
                    [{"id": "2", "name": "Client B", "city": "Lyon"}]
                    if "Lyon" in url
                    else (
                        [{"id": "3", "name": "Client C", "city": "Nice"}]
                        if "Nice" in url
                        else []
                    )
                )
            )

            # Appeler la fonction à tester
            fetch_and_save_clients(is_test=True)

            # Vérifier que les données sont sauvegardées sur S3
            mock_save_to_s3.assert_called_once()
            args, kwargs = mock_save_to_s3.call_args
            assert args[1] == "extracted_data/clients.parquet"


# Test en cas d'erreur avec l'API
def test_fetch_and_save_clients_api_error():
    """
    Teste la gestion des erreurs de l'API dans la fonction fetch_and_save_clients.
    Vérifie que save_to_s3 n'est pas appelé en cas d'erreur API.
    """
    with patch(
        "src.data_processing.extract.extract_clients.fetch_from_api",
        side_effect=Exception("API Error"),
    ) as mock_api, patch(
        "src.data_processing.extract.extract_clients.save_to_s3"
    ) as mock_save:

        # Appeler la fonction et capturer l'exception
        with pytest.raises(Exception, match="API Error"):
            fetch_and_save_clients(is_test=True)

        # Vérifier que save_to_s3 n'est pas appelé
        mock_save.assert_not_called()
