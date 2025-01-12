import io
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.data_processing.transform.aggregate_daily_metrics import (
    aggregate_retail_data, append_to_existing_metrics, calculate_daily_metrics,
    calculate_final_metrics, calculate_moving_averages, calculate_store_ratio,
    get_historical_data, get_processed_dates, process_best_selling,
    read_parquet_from_s3, read_parquet_from_s3_filtered)


def test_calculate_final_metrics():
    """
    Teste la fonction `calculate_final_metrics`.

    Vérifie que les métriques calculées, telles que le taux de conversion, la valeur moyenne des transactions,
    et la marge, sont correctes pour un ensemble de données d'entrée factices.
    """
    # Données d'entrée factices
    data = pd.DataFrame(
        {
            "total_transactions": [10, 20, 0],
            "total_visitors": [100, 200, 0],
            "total_revenue": [1000, 2500, 0],
            "total_cost": [500, 1500, 0],
            "avg_visitors_last_4_weeks": [90, 180, 100],
            "avg_sales_last_4_weeks": [8, 18, 5],
            "avg_revenue_last_4_weeks": [950, 2400, 500],
        }
    )

    # Calculer les métriques
    metrics = calculate_final_metrics(data)

    # Vérifier les résultats
    assert metrics["conversion_rate"].tolist() == [10.0, 10.0, pd.NA]
    assert metrics["avg_transaction_value"].tolist() == [100.0, 125.0, pd.NA]
    assert metrics["revenue_per_visitor"].tolist() == [10.0, 12.5, pd.NA]
    assert metrics["total_margin"].tolist() == [500.0, 1000.0, 0.0]
    assert metrics["margin_per_visitor"].tolist() == [5.0, 5.0, pd.NA]


def test_calculate_moving_averages():
    """
    Teste la fonction `calculate_final_metrics`.

    Vérifie que les métriques calculées, telles que le taux de conversion, la valeur moyenne des transactions,
    et la marge, sont correctes pour un ensemble de données d'entrée factices.
    """
    # Données d'entrée factices avec la colonne `date`
    historical_data = pd.DataFrame(
        {
            "store_id": ["store_1", "store_1", "store_1"],
            "day_of_week": ["Monday", "Monday", "Monday"],
            "total_transactions": [10, 20, 30],
            "total_visitors": [100, 200, 300],
            "total_revenue": [1000, 2000, 3000],
            "date": ["2023-12-01", "2023-12-08", "2023-12-15"],  # Dates historiques
        }
    )

    current_data = pd.DataFrame(
        {
            "store_id": ["store_1"],
            "day_of_week": ["Monday"],
            "total_transactions": [40],
            "total_visitors": [400],
            "total_revenue": [4000],
            "date": ["2023-12-22"],  # Date actuelle
        }
    )

    # Calculer les moyennes glissantes
    result = calculate_moving_averages(current_data, historical_data)

    # Vérifier les résultats
    assert result["avg_sales_last_4_weeks"].iloc[0] == 25.0
    assert result["avg_visitors_last_4_weeks"].iloc[0] == 250.0
    assert result["avg_revenue_last_4_weeks"].iloc[0] == 2500.0


def test_calculate_final_metrics_with_missing_data():
    """
    Teste la fonction `calculate_final_metrics` avec des données manquantes.

    Vérifie que la fonction gère correctement les valeurs manquantes et ne produit pas d'erreurs lors du calcul.
    """
    # Données avec des valeurs manquantes
    data = pd.DataFrame(
        {
            "total_transactions": [10, None, 0],
            "total_visitors": [100, None, 0],
            "total_revenue": [1000, None, 0],
            "total_cost": [500, None, 0],
        }
    )

    # Calculer les métriques
    metrics = calculate_final_metrics(data)

    # Vérifier que les NaN sont correctement gérés
    assert metrics["conversion_rate"].isna().iloc[1]
    assert metrics["avg_transaction_value"].isna().iloc[1]
    assert metrics["revenue_per_visitor"].isna().iloc[1]


def test_calculate_store_ratio():
    """
    Teste la fonction `calculate_store_ratio`.

    Vérifie que les ratios des visiteurs et des ventes sont correctement ajustés,
    notamment pour les cas où les visiteurs dépassent un seuil aberrant.
    """
    data = pd.DataFrame(
        {
            "visitors": [100, 6000, 200],
            "sales": [10, 15, 20],
        }
    )

    # Calculer les ratios
    result = calculate_store_ratio(data)

    # Vérifier les résultats
    assert result["visitors"].tolist() == [
        100,
        150,
        200,
    ]  # Le magasin avec 6000 visiteurs est ajusté


def test_aggregate_retail_data():
    """
    Teste la fonction `aggregate_retail_data`.

    Vérifie que les données retail sont correctement agrégées pour chaque magasin et date,
    et que les heures de pointe des ventes et des visiteurs sont correctement identifiées.
    """
    data = pd.DataFrame(
        {
            "date": ["2023-12-01", "2023-12-01", "2023-12-02"],
            "store_id": ["store_1", "store_1", "store_2"],
            "visitors": [100, 200, 300],
            "sales": [10, 20, 30],
            "hour": [10, 14, 16],
        }
    )

    result = aggregate_retail_data(data)

    # Vérifier les résultats
    assert result["total_visitors"].tolist() == [300, 300]
    assert result["total_transactions"].tolist() == [30, 30]
    assert result["peak_hour_sales"].tolist() == [14, 16]
    assert result["peak_hour_visitors"].tolist() == [14, 16]


def test_process_best_selling():
    """
    Teste la fonction `process_best_selling`.

    Vérifie que le produit le plus vendu est correctement identifié pour chaque magasin et chaque date,
    en fonction des quantités vendues.
    """
    data = pd.DataFrame(
        {
            "sale_date": ["2023-12-01", "2023-12-01", "2023-12-02"],
            "store_id": ["store_1", "store_1", "store_2"],
            "product_id": ["prod_1", "prod_2", "prod_3"],
            "name": ["Product A", "Product B", "Product C"],
            "quantity": [10, 20, 15],
        }
    )

    result = process_best_selling(data)

    # Vérifier les résultats
    assert result["best_selling_product_name"].tolist() == ["Product B", "Product C"]
    assert result["max_quantity"].tolist() == [20, 15]


def test_append_to_existing_metrics():
    """
    Teste la fonction `append_to_existing_metrics`.

    Vérifie que les nouvelles métriques sont correctement ajoutées au fichier existant et
    que l'upload sur S3 est effectué correctement.
    """
    # Mock S3 client
    mock_s3 = MagicMock()

    # Nouvelles métriques
    new_metrics = pd.DataFrame(
        {
            "date": ["2023-12-01"],
            "store_id": ["store_1"],
            "total_visitors": [300],
        }
    )

    # Patcher l'objet S3 utilisé dans la fonction
    with patch("src.data_processing.transform.aggregate_daily_metrics.s3", mock_s3):
        # Appeler la fonction
        append_to_existing_metrics(
            new_metrics, "processed_data/traffic_metrics.parquet", is_test=True
        )

        # Vérifier que le fichier est bien uploadé sur S3
        mock_s3.upload_fileobj.assert_called_once()


def test_read_parquet_from_s3_filtered():
    """
    Teste la fonction `read_parquet_from_s3_filtered`.

    Vérifie que les fichiers Parquet dans S3 sont correctement filtrés en fonction des dates traitées,
    et que les données valides sont correctement chargées dans un DataFrame.
    """
    # Mock S3 client
    mock_s3 = MagicMock()

    # Configurer la réponse S3 pour les fichiers listés
    mock_s3.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "extracted_data/data_2023-12-01.parquet"},
            {"Key": "extracted_data/data_2023-12-02.parquet"},
        ]
    }

    # Créer un fichier Parquet valide en mémoire
    data = pd.DataFrame(
        {
            "date": ["2023-12-02"],
            "store_id": ["store_1"],
            "visitors": [100],
        }
    )
    buffer = io.BytesIO()
    table = pa.Table.from_pandas(data)
    pq.write_table(table, buffer)
    buffer.seek(0)

    # Configurer la réponse S3 pour get_object
    def mock_get_object(Bucket, Key):
        if Key == "extracted_data/data_2023-12-01.parquet":
            return {"Body": io.BytesIO(b"")}  # Fichier vide simulé
        elif Key == "extracted_data/data_2023-12-02.parquet":
            return {"Body": buffer}
        return {"Body": io.BytesIO(b"")}

    mock_s3.get_object.side_effect = mock_get_object

    # Patcher l'objet S3 utilisé dans la fonction
    with patch("src.data_processing.transform.aggregate_daily_metrics.s3", mock_s3):
        # Appeler la fonction avec une date déjà traitée
        result = read_parquet_from_s3_filtered(
            "extracted_data/", processed_dates={"2023-12-01"}
        )

        # Vérifier le DataFrame
        assert not result.empty  # Doit contenir les données du fichier valide
        assert len(result) == 1
        assert result.iloc[0]["date"] == "2023-12-02"


def test_get_processed_dates():
    """
    Teste la récupération des dates déjà traitées à partir d'un fichier Parquet sur S3.
    Vérifie le comportement pour un fichier existant et un fichier manquant.
    """
    # Mock des données existantes
    existing_data = pd.DataFrame({"date": ["2023-12-01", "2023-12-02"]})
    buffer = io.BytesIO()
    existing_data.to_parquet(buffer, engine="pyarrow")
    buffer.seek(0)

    # Mock S3 client
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {"Body": io.BytesIO(buffer.getvalue())}

    # Définir une exception custom pour `NoSuchKey`
    class NoSuchKey(Exception):
        pass

    mock_s3.exceptions = MagicMock(NoSuchKey=NoSuchKey)

    with patch("src.data_processing.transform.aggregate_daily_metrics.s3", mock_s3):
        # Cas avec un fichier existant
        result = get_processed_dates("processed_data/traffic_metrics.parquet")
        assert result == {"2023-12-01", "2023-12-02"}

        # Cas avec un fichier inexistant
        mock_s3.get_object.side_effect = NoSuchKey
        result = get_processed_dates("processed_data/traffic_metrics.parquet")
        assert result == set()


def test_get_historical_data():
    """
    Teste la récupération des données historiques sur S3 pour une période donnée.
    Vérifie que seules les dates dans les 4 dernières semaines sont incluses.
    Vérifie également le comportement lorsqu'aucune donnée n'est disponible.
    """
    # Mock S3 client
    mock_s3 = MagicMock()

    # Configurer la réponse S3 avec des données conformes au filtre
    historical_data = pd.DataFrame(
        {
            "date": ["2023-11-03", "2023-11-15", "2023-12-01"],  # Dates dans la plage
            "store_id": ["store_1", "store_1", "store_1"],
            "total_visitors": [100, 200, 300],
        }
    )
    buffer = io.BytesIO()
    historical_data.to_parquet(buffer, engine="pyarrow")
    buffer.seek(0)

    mock_s3.get_object.return_value = {"Body": io.BytesIO(buffer.getvalue())}

    # Patcher `s3` utilisé dans la fonction
    with patch("src.data_processing.transform.aggregate_daily_metrics.s3", mock_s3):
        # Cas avec des données valides
        result = get_historical_data(pd.Timestamp("2023-12-01"), ["store_1"])
        assert not result.empty
        assert len(result) == 2  # Seulement 2 dates dans les 4 dernières semaines

        # Convertir les dates en chaînes avant la comparaison
        result_dates = set(result["date"].dt.strftime("%Y-%m-%d"))
        assert result_dates == {"2023-11-03", "2023-11-15"}

        # Cas sans données
        mock_s3.get_object.side_effect = Exception
        result = get_historical_data(pd.Timestamp("2023-12-01"), ["store_1"])
        assert result.empty


def test_read_parquet_from_s3():
    """
    Teste la lecture et la concaténation des fichiers Parquet dans un dossier S3.
    Vérifie le comportement avec des fichiers disponibles et avec un dossier vide.
    """
    # Mock S3 client
    mock_s3 = MagicMock()

    # Configurer la réponse S3 pour le mode test
    mock_s3.list_objects_v2.return_value = {
        "Contents": [{"Key": "test_data_2023-12-01.parquet"}]
    }
    buffer = io.BytesIO()
    pd.DataFrame({"col1": [1], "col2": [2]}).to_parquet(buffer, engine="pyarrow")
    buffer.seek(0)
    mock_s3.get_object.return_value = {"Body": buffer}

    # Patcher `s3` utilisé dans la fonction
    with patch("src.data_processing.transform.aggregate_daily_metrics.s3", mock_s3):
        # Appeler la fonction en mode test avec des fichiers présents
        result = read_parquet_from_s3("processed_data/", is_test=True)
        assert not result.empty
        assert len(result) == 1

        # Appeler la fonction en mode test avec aucun fichier
        result = read_parquet_from_s3("", is_test=True)
        assert result.empty


def test_calculate_daily_metrics():
    """
    Teste la fonction principale de calcul des métriques journalières.
    Mocke les dépendances internes pour vérifier que les données sont lues, fusionnées,
    et sauvegardées correctement.
    """
    # Mock des fonctions internes
    with patch(
        "src.data_processing.transform.aggregate_daily_metrics.get_processed_dates",
        return_value={"2023-12-01"},
    ), patch(
        "src.data_processing.transform.aggregate_daily_metrics.read_parquet_from_s3_filtered"
    ) as mock_read_filtered, patch(
        "src.data_processing.transform.aggregate_daily_metrics.read_parquet_from_s3"
    ) as mock_read, patch(
        "src.data_processing.transform.aggregate_daily_metrics.append_to_existing_metrics"
    ) as mock_append:

        # Mock des données retournées
        mock_read_filtered.side_effect = [
            pd.DataFrame(
                {
                    "date": ["2023-12-01"],
                    "store_id": ["store_1"],
                    "visitors": [100],
                    "sales": [50],
                    "hour": [14],  # Ajout de la colonne `hour`
                }
            ),
            pd.DataFrame(
                {
                    "sale_date": ["2023-12-01"],
                    "store_id": ["store_1"],
                    "quantity": [10],
                    "sale_amount": [500],
                    "product_id": ["product_1"],
                }
            ),
        ]  # Simuler les retail_data et sales_data

        mock_read.return_value = pd.DataFrame(
            {"id": ["product_1"], "cost": [10], "name": ["Product A"]}
        )

        # Appeler la fonction principale
        calculate_daily_metrics()

        # Vérifier que les données sont correctement fusionnées et sauvegardées
        mock_append.assert_called_once()
