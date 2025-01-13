import io
import os
import tempfile
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from src.data_processing.extract.extract_clients import fetch_and_save_clients
from src.data_processing.extract.utils import (create_output_folder,
                                               read_parquet_from_s3,
                                               save_to_s3, save_with_pandas)
from io import BytesIO, TextIOWrapper


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


# Test en cas d'erreur avec l'api
def test_fetch_and_save_clients_api_error():
    """
    Teste la gestion des erreurs de l'API dans la fonction fetch_and_save_clients.
    Vérifie que save_to_s3 n'est pas appelé en cas d'erreur API.
    """

    def mock_open_wrapper(file, mode="r", encoding=None):
        if "r" in mode:
            mock_file = BytesIO(b"[]")  # Fichier JSON vide simulé
            return TextIOWrapper(mock_file, encoding="utf-8")
        elif "w" in mode:
            mock_file = BytesIO()  # Fichier prêt à être écrit
            return TextIOWrapper(mock_file, encoding="utf-8")
        else:
            raise ValueError("Unsupported mode for file")

    # Mock des fonctions et du fichier
    with patch(
        "src.data_processing.extract.extract_clients.fetch_from_api",
        side_effect=Exception("API Error"),
    ) as mock_api, patch(
        "src.data_processing.extract.extract_clients.save_to_s3"
    ) as mock_save, patch(
        "src.data_processing.extract.extract_clients.open", new_callable=MagicMock
    ) as mock_open, patch(
        "os.path.exists", return_value=True
    ), patch(
        "src.data_processing.extract.extract_clients.fetch_cities",
        return_value=["Paris", "Lyon"],  # Simuler une liste de villes
    ) as mock_fetch_cities:
        # Associer le comportement du fichier simulé
        mock_open.side_effect = mock_open_wrapper

        try:
            fetch_and_save_clients(is_test=True)
        except Exception as e:
            print(f"Exception caught: {e}")
            assert str(e) == "API Error"

        # Vérifiez que fetch_cities a été appelé
        assert mock_fetch_cities.call_count == 1, "fetch_cities n'a pas été appelé."

        # Vérifiez que fetch_from_api a été appelé
        assert mock_api.call_count == 1, "fetch_from_api n'a pas été appelé."

        # Vérifiez que save_to_s3 n'est pas appelé
        mock_save.assert_not_called()


def test_create_output_folder():
    """
    Teste que la fonction `create_output_folder` crée correctement un dossier
    à l'emplacement spécifié et retourne son chemin.
    """
    # Créer un dossier temporaire
    with tempfile.TemporaryDirectory() as temp_dir:
        folder_path = os.path.join(temp_dir, "test_folder")

        # Appeler la fonction
        result = create_output_folder(folder_path)

        # Vérifier que le dossier a été créé
        assert os.path.exists(result)
        assert result == folder_path


def test_save_with_pandas():
    """
    Teste que la fonction `save_with_pandas` sauvegarde des données au format Parquet
    dans un fichier et que le fichier peut être relu avec les données intactes.
    """
    # Données simulées
    data = [{"col1": 1, "col2": 2}, {"col1": 3, "col2": 4}]

    # Utiliser un fichier temporaire
    with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as temp_file:
        temp_file_path = temp_file.name

    try:
        # Appeler la fonction
        save_with_pandas(data, temp_file_path)

        # Vérifier que le fichier a été créé et peut être lu
        df = pd.read_parquet(temp_file_path)
        assert not df.empty
        assert list(df.columns) == ["col1", "col2"]
    finally:
        # Nettoyer le fichier temporaire
        os.remove(temp_file_path)


@patch("src.data_processing.extract.utils.s3")
def test_save_to_s3(mock_s3):
    """
    Teste que la fonction `save_to_s3` envoie correctement un fichier contenant des
    données au format Parquet vers un emplacement S3 donné.
    """
    # Données simulées
    data = [{"col1": 1, "col2": 2}, {"col1": 3, "col2": 4}]
    s3_key = "test_data/test_file.parquet"

    # Mock S3 client
    mock_s3.upload_file.return_value = None

    # Appeler la fonction
    save_to_s3(data, s3_key)

    # Vérifier que le fichier a été envoyé sur S3
    mock_s3.upload_file.assert_called_once()


@patch("src.data_processing.extract.utils.s3")
def test_read_parquet_from_s3(mock_s3):
    """
    Teste que la fonction `read_parquet_from_s3` lit correctement un fichier Parquet
    depuis S3 et retourne les données sous forme de DataFrame Pandas.
    """
    # Données simulées
    test_data = pd.DataFrame({"col1": [1, 3], "col2": [2, 4]})
    buffer = io.BytesIO()
    test_data.to_parquet(buffer, engine="pyarrow")
    buffer.seek(0)

    # Mock de la réponse S3
    mock_s3.get_object.return_value = {"Body": buffer}

    # Appeler la fonction
    result = read_parquet_from_s3("test_data/test_file.parquet")

    # Vérifier les données
    pd.testing.assert_frame_equal(result, test_data)
    mock_s3.get_object.assert_called_once()
