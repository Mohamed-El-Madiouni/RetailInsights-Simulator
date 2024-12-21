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
            print(f"Lecture du fichier Parquet : s3://{BUCKET_NAME}/{file_key}")
            response = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
            buffer = io.BytesIO(response["Body"].read())
            data_frames.append(pd.read_parquet(buffer))

    if data_frames:
        return pd.concat(data_frames, ignore_index=True)
    else:
        return pd.DataFrame()


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


def aggregate_daily_metrics():
    """
    Agrège les données `retail_data` et `sales` pour obtenir des métriques journalières.

    :return: DataFrame contenant les métriques journalières.
    """
    # Charger les données retail_data et sales
    retail_data = read_parquet_from_s3("extracted_data/retail_data/")
    sales_data = read_parquet_from_s3("extracted_data/sales/")

    if retail_data.empty or sales_data.empty:
        print("Les données retail_data ou sales sont vides. Agrégation annulée.")
        return

    # Agrégation de retail_data
    retail_agg = retail_data.groupby("date").agg(
        total_visitors=("visitors", "sum"),
        total_transactions=("sales", "sum")
    ).reset_index()

    # Agrégation de sales
    sales_agg = sales_data.groupby("sale_date").agg(
        total_quantity=("quantity", "sum"),
        total_revenue=("sale_amount", "sum")
    ).reset_index().rename(columns={"sale_date": "date"})

    # Joindre les deux agrégations
    daily_metrics = pd.merge(retail_agg, sales_agg, on="date", how="outer").fillna(0)

    # Sauvegarder les résultats dans S3
    output_path = "processed_data/traffic_metrics.parquet"
    save_parquet_to_s3(daily_metrics, output_path)

    print("Agrégation terminée avec succès. Résultats sauvegardés sur S3.")


if __name__ == "__main__":
    aggregate_daily_metrics()
