import io
import os
import re

import boto3
import pandas as pd
from dotenv import load_dotenv
from pandas.api.types import CategoricalDtype

# Charger les variables d'environnement
load_dotenv()

# Initialiser le client S3
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)

BUCKET_NAME = "retail-insights-bucket"


def get_processed_dates(s3_key):
    """
    Récupérer les dates déjà présentes dans le fichier traffic_metrics.parquet.
    """
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        buffer = io.BytesIO(response["Body"].read())
        existing_metrics = pd.read_parquet(buffer)
        return set(existing_metrics["date"].unique())
    except s3.exceptions.NoSuchKey:
        print("Aucun fichier traffic_metrics.parquet existant. Pas de dates traitées.")
        return set()


def get_historical_data(start_date, store_ids):
    """
    Récupère les données historiques pour le calcul des moyennes glissantes.
    """
    try:
        response = s3.get_object(
            Bucket=BUCKET_NAME, Key="processed_data/traffic_metrics.parquet"
        )
        buffer = io.BytesIO(response["Body"].read())
        historical_data = pd.read_parquet(buffer)

        # Filtrer pour n'obtenir que les 4 dernières semaines avant start_date
        historical_data["date"] = pd.to_datetime(historical_data["date"])
        mask = (historical_data["date"] >= start_date - pd.Timedelta(weeks=4)) & (
            historical_data["date"] < start_date
        )
        historical_data = historical_data[mask]

        return historical_data
    except:
        return pd.DataFrame()


def read_parquet_from_s3(s3_folder):
    """
    Lit et concatène tous les fichiers Parquet dans un dossier S3.

    :param s3_folder: Chemin du dossier dans le bucket S3.
    :return: DataFrame Pandas contenant les données concaténées.
    """
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=s3_folder)

    if "Contents" not in response:
        print(f"Aucun fichier trouvé dans s3://{BUCKET_NAME}/{s3_folder}")
        return pd.DataFrame()

    data_frames = []
    for obj in response["Contents"]:
        file_key = obj["Key"]
        if file_key.endswith(".parquet"):
            response = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
            buffer = io.BytesIO(response["Body"].read())
            data_frames.append(pd.read_parquet(buffer))

    return pd.concat(data_frames, ignore_index=True) if data_frames else pd.DataFrame()


def read_parquet_from_s3_filtered(s3_folder, processed_dates):
    """
    Lit et concatène tous les fichiers Parquet non traités dans un dossier S3.
    """
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=s3_folder)
    if "Contents" not in response:
        print(f"Aucun fichier trouvé dans s3://{BUCKET_NAME}/{s3_folder}")
        return pd.DataFrame()

    data_frames = []
    date_pattern = re.compile(r".*_(\d{4}-\d{2}-\d{2})\.parquet")

    for obj in response["Contents"]:
        file_key = obj["Key"]
        if file_key.endswith(".parquet"):
            match = date_pattern.match(file_key)
            if match:
                file_date = match.group(1)
                if file_date not in processed_dates:
                    response = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
                    buffer = io.BytesIO(response["Body"].read())
                    data_frames.append(pd.read_parquet(buffer))

    return pd.concat(data_frames, ignore_index=True) if data_frames else pd.DataFrame()


def calculate_store_ratio(group):
    """Version optimisée du calcul de ratio"""
    valid_mask = (group["visitors"] <= 5000) & (group["visitors"] > 0)
    if valid_mask.any():
        ratio_mean = (
            100
            * group.loc[valid_mask, "sales"].sum()
            / group.loc[valid_mask, "visitors"].sum()
        )

        mask = group["visitors"] > 5000
        group.loc[mask, "visitors"] = group.loc[mask, "sales"] * ratio_mean

    return group


def aggregate_retail_data(retail_data):
    """Version optimisée de l'agrégation des données retail"""
    # Calculer les sommes
    aggs = {"visitors": "sum", "sales": "sum"}
    retail_agg = retail_data.groupby(["date", "store_id"]).agg(aggs)
    retail_agg.columns = ["total_visitors", "total_transactions"]

    # Trouver les heures de pointe
    idx_max_sales = retail_data.groupby(["date", "store_id"])["sales"].transform(
        "idxmax"
    )
    idx_max_visitors = retail_data.groupby(["date", "store_id"])["visitors"].transform(
        "idxmax"
    )

    peak_hours = pd.DataFrame(
        {
            "date": retail_data.loc[idx_max_sales].drop_duplicates(
                subset=["date", "store_id"]
            )["date"],
            "store_id": retail_data.loc[idx_max_sales].drop_duplicates(
                subset=["date", "store_id"]
            )["store_id"],
            "peak_hour_sales": retail_data.loc[idx_max_sales].drop_duplicates(
                subset=["date", "store_id"]
            )["hour"],
            "peak_hour_visitors": retail_data.loc[idx_max_visitors].drop_duplicates(
                subset=["date", "store_id"]
            )["hour"],
        }
    )

    # Fusionner les résultats
    retail_agg = retail_agg.reset_index().merge(peak_hours, on=["date", "store_id"])

    return retail_agg


def calculate_moving_averages(metrics_df, historical_data=None):
    """Version optimisée du calcul des moyennes glissantes"""
    if not historical_data.empty:
        combined_data = pd.concat([historical_data, metrics_df]).sort_values(["date"])
    else:
        combined_data = metrics_df.copy()

    combined_data["date"] = pd.to_datetime(combined_data["date"])

    # Préparer le DataFrame résultat
    result_df = metrics_df.copy()

    # Calculer les moyennes glissantes de manière vectorisée
    for col, avg_col in [
        ("total_transactions", "avg_sales_last_4_weeks"),
        ("total_visitors", "avg_visitors_last_4_weeks"),
        ("total_revenue", "avg_revenue_last_4_weeks"),
    ]:
        # Trier et grouper les données
        grouped = combined_data.sort_values("date").groupby(["store_id", "day_of_week"])

        # Calculer la moyenne glissante pour tous les groupes en une fois
        rolling_means = grouped[col].rolling(window=4, min_periods=1).mean()

        # Réindexer pour correspondre au DataFrame final
        for (store, day), group in metrics_df.groupby(["store_id", "day_of_week"]):
            if (store, day) in rolling_means.index:
                mask = (result_df["store_id"] == store) & (
                    result_df["day_of_week"] == day
                )
                values = rolling_means.loc[(store, day)].tail(len(group))
                result_df.loc[mask, avg_col] = values.values.round(2)

    return result_df


def process_best_selling(sales_with_cost):
    """Version optimisée du calcul des meilleurs vendeurs"""
    # Calcul des quantités totales
    quantities = (
        sales_with_cost.groupby(["sale_date", "store_id", "product_id", "name"])[
            "quantity"
        ]
        .sum()
        .reset_index()
    )

    # Trouver le meilleur produit par date/magasin
    idx = (
        quantities.groupby(["sale_date", "store_id"])["quantity"].transform("max")
        == quantities["quantity"]
    )
    best_selling = quantities[idx].rename(
        columns={
            "product_id": "best_selling_product_id",
            "name": "best_selling_product_name",
            "quantity": "max_quantity",
            "sale_date": "date",
        }
    )

    return best_selling


def calculate_final_metrics(df):
    """Version optimisée du calcul des métriques finales"""
    metrics = df.copy()

    # Ajouter des colonnes moyennes glissantes manquantes avec des valeurs par défaut
    for col in [
        "avg_visitors_last_4_weeks",
        "avg_sales_last_4_weeks",
        "avg_revenue_last_4_weeks",
    ]:
        if col not in metrics.columns:
            metrics[col] = pd.NA

    # Calculer toutes les métriques d'un coup
    metrics_dict = {
        "conversion_rate": lambda x: (
            (x["total_transactions"] * 100 / x["total_visitors"])
            if pd.notna(x["total_transactions"])
            and pd.notna(x["total_visitors"])
            and x["total_visitors"] > 0
            else pd.NA
        ),
        "avg_transaction_value": lambda x: (
            (x["total_revenue"] / x["total_transactions"])
            if pd.notna(x["total_revenue"])
            and pd.notna(x["total_transactions"])
            and x["total_transactions"] > 0
            else pd.NA
        ),
        "revenue_per_visitor": lambda x: (
            (x["total_revenue"] / x["total_visitors"])
            if pd.notna(x["total_revenue"])
            and pd.notna(x["total_visitors"])
            and x["total_visitors"] > 0
            else pd.NA
        ),
        "total_margin": lambda x: (
            (x["total_revenue"] - x["total_cost"])
            if pd.notna(x["total_revenue"]) and pd.notna(x["total_cost"])
            else pd.NA
        ),
        "margin_per_visitor": lambda x: (
            ((x["total_revenue"] - x["total_cost"]) / x["total_visitors"])
            if pd.notna(x["total_revenue"])
            and pd.notna(x["total_cost"])
            and pd.notna(x["total_visitors"])
            and x["total_visitors"] > 0
            else pd.NA
        ),
    }

    for col, func in metrics_dict.items():
        metrics[col] = metrics.apply(func, axis=1)

    # Calculer les variations en une fois
    for metric, (col, avg_col) in {
        "visitors": ("total_visitors", "avg_visitors_last_4_weeks"),
        "transactions": ("total_transactions", "avg_sales_last_4_weeks"),
        "revenue": ("total_revenue", "avg_revenue_last_4_weeks"),
    }.items():
        metrics[f"{metric}_variation_vs_avg_4w_percent"] = (
            (metrics[col] - metrics[avg_col]) * 100 / metrics[avg_col]
        ).where(pd.notna(metrics[avg_col]), pd.NA)

    # Calcul spécial pour transactions_amount
    metrics["transactions_amount_variation_vs_avg_4w_percent"] = (
        (
            metrics["avg_transaction_value"]
            - (metrics["avg_revenue_last_4_weeks"] / metrics["avg_sales_last_4_weeks"])
        )
        * 100
        / (metrics["avg_revenue_last_4_weeks"] / metrics["avg_sales_last_4_weeks"])
    ).where(
        pd.notna(metrics["avg_revenue_last_4_weeks"])
        & pd.notna(metrics["avg_sales_last_4_weeks"]),
        pd.NA,
    )

    return metrics


def append_to_existing_metrics(new_metrics, s3_key, is_test=False):
    """
    Ajouter les nouvelles métriques au fichier existant sur S3.
    """
    try:
        if is_test:
            # Simuler un fichier existant dans S3 pour les tests
            existing_metrics = pd.DataFrame(
                {
                    "date": ["2023-11-01"],
                    "store_id": ["store_1"],
                    "total_visitors": [250],
                }
            )
        else:
            # Récupérer les métriques existantes depuis S3
            response = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
            buffer = io.BytesIO(response["Body"].read())
            existing_metrics = pd.read_parquet(buffer)

        # Convertir la date en datetime
        existing_metrics["date"] = pd.to_datetime(existing_metrics["date"])
        new_metrics["date"] = pd.to_datetime(new_metrics["date"])

        # Combiner les métriques
        combined_metrics = pd.concat([existing_metrics, new_metrics], ignore_index=True)
        combined_metrics = combined_metrics.drop_duplicates(
            subset=["date", "store_id"], keep="last"
        )

        # Convertir la date en string
        combined_metrics["date"] = combined_metrics["date"].dt.strftime("%Y-%m-%d")

    except s3.exceptions.NoSuchKey if not is_test else Exception:
        # Si le fichier n'existe pas, utiliser uniquement les nouvelles métriques
        combined_metrics = new_metrics
        combined_metrics["date"] = pd.to_datetime(combined_metrics["date"]).dt.strftime(
            "%Y-%m-%d"
        )

    # Sauvegarder sur S3 ou simuler pour les tests
    buffer = io.BytesIO()
    combined_metrics.to_parquet(
        buffer, engine="pyarrow", compression="snappy", index=False
    )
    buffer.seek(0)
    if is_test:
        s3.upload_fileobj(buffer, BUCKET_NAME, s3_key)
    else:
        s3.upload_fileobj(buffer, BUCKET_NAME, s3_key)


def calculate_daily_metrics():
    """Fonction principale optimisée"""

    processed_dates = get_processed_dates("processed_data/traffic_metrics.parquet")
    print(f"Dates déjà traitées récupérées: {processed_dates}")

    # Lire les données
    retail_data = read_parquet_from_s3_filtered(
        "extracted_data/retail_data/", processed_dates
    )
    sales_data = read_parquet_from_s3_filtered("extracted_data/sales/", processed_dates)
    products_data = read_parquet_from_s3("extracted_data/products.parquet")

    if retail_data.empty or sales_data.empty or products_data.empty:
        print("Aucune nouvelle donnée à traiter.")
        return

    # Traitement des données retail
    retail_data = retail_data[
        ~(retail_data["visitors"].isna() & retail_data["sales"].isna())
    ]
    retail_data = (
        retail_data.groupby("store_id")
        .apply(calculate_store_ratio)
        .reset_index(drop=True)
    )
    retail_agg = aggregate_retail_data(retail_data)

    # Traitement des ventes
    sales_agg = (
        sales_data.groupby(["sale_date", "store_id"])
        .agg(total_quantity=("quantity", "sum"), total_revenue=("sale_amount", "sum"))
        .reset_index()
        .rename(columns={"sale_date": "date"})
    )

    sales_with_cost = sales_data.merge(
        products_data[["id", "cost", "name"]], left_on="product_id", right_on="id"
    )

    # Calculer les coûts
    sales_with_cost["total_cost"] = (
        sales_with_cost["quantity"] * sales_with_cost["cost"]
    )
    sales_cost_agg = (
        sales_with_cost.groupby(["sale_date", "store_id"])["total_cost"].sum().round(2)
    )
    sales_cost_agg = sales_cost_agg.reset_index().rename(columns={"sale_date": "date"})

    # Calculer les meilleurs vendeurs
    best_selling = process_best_selling(sales_with_cost)

    # Fusionner toutes les données
    daily_metrics = (
        retail_agg.merge(sales_agg, on=["date", "store_id"], how="outer")
        .merge(sales_cost_agg, on=["date", "store_id"], how="outer")
        .merge(
            best_selling[
                [
                    "date",
                    "store_id",
                    "best_selling_product_id",
                    "best_selling_product_name",
                ]
            ],
            on=["date", "store_id"],
            how="outer",
        )
        .fillna(0)
    )

    # Ajouter jour de la semaine
    daily_metrics["date"] = pd.to_datetime(daily_metrics["date"])
    daily_metrics["day_of_week"] = daily_metrics["date"].dt.day_name()
    daily_metrics["day_of_week"] = daily_metrics["day_of_week"].astype(
        CategoricalDtype(
            [
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
                "Sunday",
            ],
            ordered=True,
        )
    )

    # Calculer les moyennes glissantes
    min_date = daily_metrics["date"].min()
    store_ids = daily_metrics["store_id"].unique()
    historical_data = get_historical_data(min_date, store_ids)
    daily_metrics = calculate_moving_averages(daily_metrics, historical_data)

    # Calculer les métriques finales
    daily_metrics = calculate_final_metrics(daily_metrics)

    # Réorganiser les colonnes
    daily_metrics = daily_metrics[
        [
            "date",
            "day_of_week",
            "store_id",
            "total_visitors",
            "avg_visitors_last_4_weeks",
            "visitors_variation_vs_avg_4w_percent",
            "total_transactions",
            "avg_sales_last_4_weeks",
            "transactions_variation_vs_avg_4w_percent",
            "total_quantity",
            "best_selling_product_id",
            "best_selling_product_name",
            "total_revenue",
            "avg_revenue_last_4_weeks",
            "revenue_variation_vs_avg_4w_percent",
            "total_cost",
            "total_margin",
            "conversion_rate",
            "avg_transaction_value",
            "transactions_amount_variation_vs_avg_4w_percent",
            "revenue_per_visitor",
            "margin_per_visitor",
            "peak_hour_sales",
            "peak_hour_visitors",
        ]
    ]

    # Sauvegarder les résultats
    append_to_existing_metrics(daily_metrics, "processed_data/traffic_metrics.parquet")
    print(f"Ajout de {len(daily_metrics)} nouvelles lignes")


if __name__ == "__main__":
    calculate_daily_metrics()
