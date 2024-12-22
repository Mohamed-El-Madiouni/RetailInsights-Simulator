import boto3
import pandas as pd
import io
from dotenv import load_dotenv
import os

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
    sales_data = sales_data[sales_data["store_id"] == "ad8ea008-bc1c-4f27-845a-2fad67b68471"]

    if retail_data.empty or sales_data.empty or products_data.empty or clients_data.empty:
        print("Données manquantes. Agrégation annulée.")
        return

    # Agrégation de retail_data
    retail_agg = retail_data.groupby("date").apply(
        lambda group: pd.Series({
            "total_visitors": group["visitors"].sum(),
            "total_transactions": group["sales"].sum(),
            "peak_hour": group.loc[group["sales"].idxmax(), "hour"]
        })
    ).reset_index()

    # Agrégation de sales_data
    sales_agg = sales_data.groupby("sale_date").agg(
        total_quantity=("quantity", "sum"),
        total_revenue=("sale_amount", "sum")
    ).reset_index().rename(columns={"sale_date": "date"})

    # Associer sales_data avec products pour calculer les coûts
    sales_with_cost = sales_data.merge(products_data, left_on="product_id", right_on="id")
    sales_cost_agg = sales_with_cost.groupby("sale_date").agg(
        total_cost=("quantity", lambda x: (x * sales_with_cost["cost"]).sum()),
        best_selling_product=("product_id", lambda x: x.value_counts().idxmax())
    ).reset_index().rename(columns={"sale_date": "date"})

    # Fusionner les agrégations
    daily_metrics = retail_agg.merge(sales_agg, on="date", how="outer").merge(
        sales_cost_agg, on="date", how="outer"
    ).fillna(0)

    # Calcul des métriques finales
    daily_metrics["conversion_rate"] = daily_metrics["total_transactions"] / daily_metrics["total_visitors"]
    daily_metrics["avg_transaction_value"] = daily_metrics["total_revenue"] / daily_metrics["total_transactions"]
    daily_metrics["revenue_per_visitor"] = daily_metrics["total_revenue"] / daily_metrics["total_visitors"]
    daily_metrics["cost_per_visitor"] = daily_metrics["total_cost"] / daily_metrics["total_visitors"]
    daily_metrics["total_margin"] = daily_metrics["total_revenue"] - daily_metrics["total_cost"]
    daily_metrics["margin_per_visitor"] = daily_metrics["total_margin"] / daily_metrics["total_visitors"]

    # Sauvegarder les métriques enrichies sur S3
    save_parquet_to_s3(daily_metrics, "processed_data/traffic_metrics.parquet")
    print("Métriques enrichies calculées et sauvegardées avec succès.")


if __name__ == "__main__":
    calculate_daily_metrics()
