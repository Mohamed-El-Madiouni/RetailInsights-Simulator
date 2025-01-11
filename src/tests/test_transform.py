import pandas as pd
import io
from src.data_processing.transform.aggregate_daily_metrics import calculate_final_metrics, calculate_moving_averages, calculate_store_ratio, aggregate_retail_data, process_best_selling, append_to_existing_metrics, read_parquet_from_s3_filtered
from unittest.mock import MagicMock, patch
import pyarrow as pa
import pyarrow.parquet as pq


def test_calculate_final_metrics():
    # Données d'entrée factices
    data = pd.DataFrame({
        "total_transactions": [10, 20, 0],
        "total_visitors": [100, 200, 0],
        "total_revenue": [1000, 2500, 0],
        "total_cost": [500, 1500, 0],
        "avg_visitors_last_4_weeks": [90, 180, 100],
        "avg_sales_last_4_weeks": [8, 18, 5],
        "avg_revenue_last_4_weeks": [950, 2400, 500]
    })

    # Calculer les métriques
    metrics = calculate_final_metrics(data)

    # Vérifier les résultats
    assert metrics["conversion_rate"].tolist() == [10.0, 10.0, pd.NA]
    assert metrics["avg_transaction_value"].tolist() == [100.0, 125.0, pd.NA]
    assert metrics["revenue_per_visitor"].tolist() == [10.0, 12.5, pd.NA]
    assert metrics["total_margin"].tolist() == [500.0, 1000.0, 0.0]
    assert metrics["margin_per_visitor"].tolist() == [5.0, 5.0, pd.NA]


def test_calculate_moving_averages():
    # Données d'entrée factices avec la colonne `date`
    historical_data = pd.DataFrame({
        "store_id": ["store_1", "store_1", "store_1"],
        "day_of_week": ["Monday", "Monday", "Monday"],
        "total_transactions": [10, 20, 30],
        "total_visitors": [100, 200, 300],
        "total_revenue": [1000, 2000, 3000],
        "date": ["2023-12-01", "2023-12-08", "2023-12-15"]  # Dates historiques
    })

    current_data = pd.DataFrame({
        "store_id": ["store_1"],
        "day_of_week": ["Monday"],
        "total_transactions": [40],
        "total_visitors": [400],
        "total_revenue": [4000],
        "date": ["2023-12-22"]  # Date actuelle
    })

    # Calculer les moyennes glissantes
    result = calculate_moving_averages(current_data, historical_data)

    # Vérifier les résultats
    assert result["avg_sales_last_4_weeks"].iloc[0] == 25.0
    assert result["avg_visitors_last_4_weeks"].iloc[0] == 250.0
    assert result["avg_revenue_last_4_weeks"].iloc[0] == 2500.0


def test_calculate_final_metrics_with_missing_data():
    # Données avec des valeurs manquantes
    data = pd.DataFrame({
        "total_transactions": [10, None, 0],
        "total_visitors": [100, None, 0],
        "total_revenue": [1000, None, 0],
        "total_cost": [500, None, 0]
    })

    # Calculer les métriques
    metrics = calculate_final_metrics(data)

    # Vérifier que les NaN sont correctement gérés
    assert metrics["conversion_rate"].isna().iloc[1]
    assert metrics["avg_transaction_value"].isna().iloc[1]
    assert metrics["revenue_per_visitor"].isna().iloc[1]


def test_calculate_store_ratio():
    data = pd.DataFrame({
        "visitors": [100, 6000, 200],
        "sales": [10, 15, 20],
    })

    # Calculer les ratios
    result = calculate_store_ratio(data)

    # Vérifier les résultats
    assert result["visitors"].tolist() == [100, 150, 200]  # Le magasin avec 6000 visiteurs est ajusté


def test_aggregate_retail_data():
    data = pd.DataFrame({
        "date": ["2023-12-01", "2023-12-01", "2023-12-02"],
        "store_id": ["store_1", "store_1", "store_2"],
        "visitors": [100, 200, 300],
        "sales": [10, 20, 30],
        "hour": [10, 14, 16],
    })

    result = aggregate_retail_data(data)

    # Vérifier les résultats
    assert result["total_visitors"].tolist() == [300, 300]
    assert result["total_transactions"].tolist() == [30, 30]
    assert result["peak_hour_sales"].tolist() == [14, 16]
    assert result["peak_hour_visitors"].tolist() == [14, 16]


def test_process_best_selling():
    data = pd.DataFrame({
        "sale_date": ["2023-12-01", "2023-12-01", "2023-12-02"],
        "store_id": ["store_1", "store_1", "store_2"],
        "product_id": ["prod_1", "prod_2", "prod_3"],
        "name": ["Product A", "Product B", "Product C"],
        "quantity": [10, 20, 15],
    })

    result = process_best_selling(data)

    # Vérifier les résultats
    assert result["best_selling_product_name"].tolist() == ["Product B", "Product C"]
    assert result["max_quantity"].tolist() == [20, 15]


def test_append_to_existing_metrics():
    # Mock S3 client
    mock_s3 = MagicMock()

    # Nouvelles métriques
    new_metrics = pd.DataFrame({
        "date": ["2023-12-01"],
        "store_id": ["store_1"],
        "total_visitors": [300],
    })

    # Patcher l'objet S3 utilisé dans la fonction
    with patch("src.data_processing.transform.aggregate_daily_metrics.s3", mock_s3):
        # Appeler la fonction
        append_to_existing_metrics(new_metrics, "processed_data/traffic_metrics.parquet", is_test=True)

        # Vérifier que le fichier est bien uploadé sur S3
        mock_s3.upload_fileobj.assert_called_once()


def test_read_parquet_from_s3_filtered():
    # Mock S3 client
    mock_s3 = MagicMock()

    # Configurer la réponse S3 pour les fichiers listés
    mock_s3.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "extracted_data/data_2023-12-01.parquet"},
            {"Key": "extracted_data/data_2023-12-02.parquet"}
        ]
    }

    # Créer un fichier Parquet valide en mémoire
    data = pd.DataFrame({
        "date": ["2023-12-02"],
        "store_id": ["store_1"],
        "visitors": [100],
    })
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
        result = read_parquet_from_s3_filtered("extracted_data/", processed_dates={"2023-12-01"})

        # Vérifier le DataFrame
        assert not result.empty  # Doit contenir les données du fichier valide
        assert len(result) == 1
        assert result.iloc[0]["date"] == "2023-12-02"
