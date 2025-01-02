import boto3
import pandas as pd
import io
from dotenv import load_dotenv
import os
from pandas.api.types import CategoricalDtype

# Charger les variables d'environnement
load_dotenv()

# Initialiser le client S3
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

BUCKET_NAME = "retail-insights-bucket"


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


def save_parquet_to_s3(df, s3_key):
    """
    Sauvegarde un DataFrame Pandas au format Parquet sur S3.

    :param df: DataFrame Pandas à sauvegarder.
    :param s3_key: Chemin du fichier dans le bucket S3.
    """
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", compression="snappy", index=False)
    buffer.seek(0)
    s3.upload_fileobj(buffer, BUCKET_NAME, s3_key)
    print(f"Fichier sauvegardé sur S3 : s3://{BUCKET_NAME}/{s3_key}")


def calculate_daily_metrics():
    # Lire les données retail_data, sales et products
    retail_data = read_parquet_from_s3("extracted_data/retail_data/")
    sales_data = read_parquet_from_s3("extracted_data/sales/")
    products_data = read_parquet_from_s3("extracted_data/products.parquet")
    clients_data = read_parquet_from_s3("extracted_data/clients.parquet")

    if retail_data.empty or sales_data.empty or products_data.empty or clients_data.empty:
        print("Données manquantes. Agrégation annulée.")
        return

    # Filtrer les âges aberrants dans clients_data
    clients_data["age"] = clients_data["age"].apply(lambda x: x if x <= 120 else None)

    # Supprimer les lignes avec des valeurs nulles pour sales et visitors
    retail_data = retail_data[~(retail_data["visitors"].isna() & retail_data["sales"].isna())]

    # Calculer un ratio par magasin pour corriger les valeurs aberrantes
    def calculate_store_ratio(group):
        valid_visitors = group[(group["visitors"] <= 5000) & (group["visitors"] > 0)]
        ratio_mean = (
                valid_visitors["sales"].sum() / valid_visitors["visitors"].sum()
        ) if not valid_visitors.empty else 0
        group["visitors"] = group.apply(
            lambda row: row["sales"] * ratio_mean if row["visitors"] > 5000 else row["visitors"],
            axis=1
        )
        return group

    # Remplacer les valeurs aberrantes dans visitors par une estimation basée sur le ratio
    retail_data = retail_data.groupby("store_id").apply(calculate_store_ratio).reset_index(drop=True)

    # Agrégation de retail_data
    retail_agg = retail_data.groupby(["date", "store_id"]).apply(
        lambda group: pd.Series({
            "total_visitors": group["visitors"].sum(),
            "total_transactions": group["sales"].sum(),
            "peak_hour_sales": group.loc[group["sales"].idxmax(), "hour"],
            "peak_hour_visitors": group.loc[group["visitors"].idxmax(), "hour"]
        })
    ).reset_index()

    retail_agg["total_visitors"] = retail_agg["total_visitors"].round(0).astype(int)
    retail_agg["total_transactions"] = retail_agg["total_transactions"].round(0).astype(int)
    retail_agg["peak_hour_sales"] = retail_agg["peak_hour_sales"].round(0).astype(int)
    retail_agg["peak_hour_visitors"] = retail_agg["peak_hour_visitors"].round(0).astype(int)

    # Agrégation de sales_data
    sales_agg = sales_data.groupby(["sale_date", "store_id"]).agg(
        total_quantity=("quantity", "sum"),
        total_revenue=("sale_amount", "sum")
    ).reset_index().rename(columns={"sale_date": "date"})

    # Arrondir les colonnes
    sales_agg["total_quantity"] = sales_agg["total_quantity"].round(0).astype(int)
    sales_agg["total_revenue"] = sales_agg["total_revenue"].round(2)

    # Associer sales_data avec products pour calculer les coûts
    sales_with_cost = sales_data.merge(products_data, left_on="product_id", right_on="id")
    sales_cost_agg = sales_with_cost.groupby(["sale_date", "store_id"]).agg(
        total_cost=("quantity", lambda x: (x * sales_with_cost["cost"]).sum()),
    ).reset_index()

    sales_cost_agg["total_cost"] = sales_cost_agg["total_cost"].round(2)

    # Calculer le best_selling_product
    best_selling_product = (
        sales_with_cost.groupby(["sale_date", "store_id", "product_id", "name"])["quantity"]
        .sum()
        .reset_index()
        .sort_values(["sale_date", "store_id", "quantity"], ascending=[True, True, False])
        .groupby(["sale_date", "store_id"])
        .first()
        .reset_index()
        .rename(columns={"product_id": "best_selling_product_id", "name": "best_selling_product_name", "quantity": "max_quantity"})
    )

    # Fusionner les deux agrégations
    sales_cost_agg = sales_cost_agg.merge(
        best_selling_product[["sale_date", "store_id", "best_selling_product_id", "best_selling_product_name"]],
        on=["sale_date", "store_id"],
        how="left"
    ).rename(columns={"sale_date": "date"})

    # Fusionner les agrégations
    daily_metrics = retail_agg.merge(sales_agg, on=["date", "store_id"], how="outer").merge(
        sales_cost_agg, on=["date", "store_id"], how="outer"
    ).fillna(0)

    # Ajouter une colonne pour le jour de la semaine
    daily_metrics["day_of_week"] = pd.to_datetime(daily_metrics["date"]).dt.day_name()
    day_of_week_cat = CategoricalDtype([
        "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"
    ], ordered=True)
    daily_metrics["day_of_week"] = daily_metrics["day_of_week"].astype(day_of_week_cat)

    # Ajouter une moyenne glissante sur les 4 derniers mêmes jours de la semaine par magasin
    daily_metrics["avg_sales_last_4_weeks"] = round((
        daily_metrics.groupby(["store_id", "day_of_week"])["total_transactions"]
        .rolling(window=4, min_periods=1)
        .mean()
        .reset_index(level=[0, 1], drop=True)
    ), 2)
    daily_metrics["avg_visitors_last_4_weeks"] = round((
        daily_metrics.groupby(["store_id", "day_of_week"])["total_visitors"]
        .rolling(window=4, min_periods=1)
        .mean()
        .reset_index(level=[0, 1], drop=True)
    ), 2)
    daily_metrics["avg_revenue_last_4_weeks"] = round((
        daily_metrics.groupby(["store_id", "day_of_week"])["total_revenue"]
        .rolling(window=4, min_periods=1)
        .mean()
        .reset_index(level=[0, 1], drop=True)
    ), 2)

    # Calcul des métriques finales
    daily_metrics["conversion_rate"] = round(daily_metrics["total_transactions"] * 100 / daily_metrics["total_visitors"], 2)
    daily_metrics["avg_transaction_value"] = round(daily_metrics["total_revenue"] / daily_metrics["total_transactions"], 2)
    daily_metrics["revenue_per_visitor"] = round(daily_metrics["total_revenue"] / daily_metrics["total_visitors"], 2)
    daily_metrics["total_margin"] = round(daily_metrics["total_revenue"] - daily_metrics["total_cost"], 2)
    daily_metrics["margin_per_visitor"] = round(daily_metrics["total_margin"] / daily_metrics["total_visitors"], 2)
    daily_metrics["visitors_variation_vs_avg_4w_percent"] = round((
                                                                daily_metrics["total_visitors"] -
                                                                daily_metrics["avg_visitors_last_4_weeks"])
                                                        * 100 / daily_metrics["avg_visitors_last_4_weeks"], 2)
    daily_metrics["transactions_variation_vs_avg_4w_percent"] = round((
                                                                daily_metrics["total_transactions"] -
                                                                daily_metrics["avg_sales_last_4_weeks"])
                                                        * 100 / daily_metrics["avg_sales_last_4_weeks"], 2)
    daily_metrics["revenue_variation_vs_avg_4w_percent"] = round((
                                                                daily_metrics["total_revenue"] -
                                                                daily_metrics["avg_revenue_last_4_weeks"])
                                                        * 100 / daily_metrics["avg_revenue_last_4_weeks"], 2)
    daily_metrics["transactions_amount_variation_vs_avg_4w_percent"] = round((
                                                                daily_metrics["avg_transaction_value"] -
                                                                (daily_metrics["avg_revenue_last_4_weeks"] /
                                                                 daily_metrics["avg_sales_last_4_weeks"]))
                                                        * 100 / (daily_metrics["avg_revenue_last_4_weeks"] /
                                                                 daily_metrics["avg_sales_last_4_weeks"]), 2)

    daily_metrics = daily_metrics[[
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
        "peak_hour_visitors"
    ]]

    # Sauvegarder les métriques enrichies sur S3
    save_parquet_to_s3(daily_metrics, "processed_data/traffic_metrics.parquet")
    print("Métriques enrichies calculées et sauvegardées avec succès.")
    print(daily_metrics.T)


if __name__ == "__main__":
    calculate_daily_metrics()
