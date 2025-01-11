import pytest
import pandas as pd
from src.data_processing.transform.aggregate_daily_metrics import calculate_final_metrics
from src.data_processing.transform.aggregate_daily_metrics import calculate_moving_averages


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
