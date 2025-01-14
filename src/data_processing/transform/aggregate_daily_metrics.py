import io
import os
import re

import boto3
import pandas as pd
from dotenv import load_dotenv
from pandas.api.types import CategoricalDtype
from src.data_processing.transform.logger_transformation import transformation_logger

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
    Récupère les dates déjà présentes dans le fichier `traffic_metrics.parquet` sur S3.

    Args:
        s3_key (str): Chemin du fichier Parquet dans le bucket S3.

    Returns:
        set: Ensemble des dates traitées déjà présentes dans le fichier.
    """
    try:
        transformation_logger.info(f"Fetching processed dates from S3 key: {s3_key}")
        response = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        buffer = io.BytesIO(response["Body"].read())
        existing_metrics = pd.read_parquet(buffer)
        processed_dates = set(existing_metrics["date"].unique())
        transformation_logger.info(f"Successfully fetched {len(processed_dates)} processed dates.")
        return processed_dates
    except s3.exceptions.NoSuchKey:
        transformation_logger.warning(f"No existing file at {s3_key}. No processed dates found.")
        return set()


def get_historical_data(start_date, store_ids):
    """
    Récupère les données historiques depuis le fichier `traffic_metrics.parquet` pour calculer les moyennes glissantes.

    Args:
        start_date (datetime): Date de début pour filtrer les données historiques.
        store_ids (list): Liste des identifiants de magasins.

    Returns:
        pd.DataFrame: Données historiques filtrées.
    """
    try:
        transformation_logger.info("Fetching historical data for moving averages.")
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
        transformation_logger.info(f"Fetched historical data with {len(historical_data)} records.")
        return historical_data
    except Exception as e:
        transformation_logger.error(f"Error fetching historical data: {e}")
        return pd.DataFrame()


def read_parquet_from_s3(s3_folder, is_test=False):
    """
    Lit et concatène tous les fichiers Parquet présents dans un dossier S3.

    Args:
        s3_folder (str): Chemin du dossier dans le bucket S3.
        is_test (bool): Indique si l'exécution est en mode test.

    Returns:
        pd.DataFrame: Données concaténées depuis les fichiers Parquet.
    """
    transformation_logger.info(f"Reading Parquet files from S3 folder: {s3_folder}")
    if is_test:
        transformation_logger.info("Simulating test mode for reading Parquet files.")
        # Simuler un comportement réaliste
        response = (
            {"Contents": [{"Key": "test_data_2023-12-01.parquet"}]}
            if s3_folder
            else {"Contents": []}
        )
        if "Contents" not in response or not response["Contents"]:
            transformation_logger.warning(f"No files found in simulated S3 folder: {s3_folder}")
            return pd.DataFrame()  # Renvoie un DataFrame vide si aucun fichier

        # Simuler le contenu d'un fichier Parquet
        buffer = io.BytesIO()
        pd.DataFrame({"col1": [1], "col2": [2]}).to_parquet(buffer, engine="pyarrow")
        buffer.seek(0)
        result = pd.read_parquet(buffer)
        transformation_logger.info("Read test file successfully.")
        return result

    try:
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=s3_folder)

        if "Contents" not in response:
            transformation_logger.warning(f"No files found in S3 folder: {s3_folder}")
            return pd.DataFrame()

        data_frames = []
        for obj in response["Contents"]:
            file_key = obj["Key"]
            if file_key.endswith(".parquet"):
                response = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
                buffer = io.BytesIO(response["Body"].read())
                data_frames.append(pd.read_parquet(buffer))
                transformation_logger.info(f"Read file {file_key} successfully.")

        concatenated_df = pd.concat(data_frames, ignore_index=True) if data_frames else pd.DataFrame()
        transformation_logger.info(f"Successfully concatenated {len(concatenated_df)} records.")
        return concatenated_df
    except Exception as e:
        transformation_logger.error(f"Error reading Parquet files from S3 folder {s3_folder}: {e}")
        return pd.DataFrame()


def read_parquet_from_s3_filtered(s3_folder, processed_dates):
    """
    Lit et concatène uniquement les fichiers Parquet non traités dans un dossier S3.

    Args:
        s3_folder (str): Chemin du dossier dans le bucket S3.
        processed_dates (set): Ensemble des dates déjà traitées.

    Returns:
        pd.DataFrame: Données des fichiers non traités.
    """
    transformation_logger.info(f"Reading filtered Parquet files from S3 folder: {s3_folder}")
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=s3_folder)
    if "Contents" not in response:
        transformation_logger.warning(f"No files found in S3 folder: {s3_folder}")
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
                    try:
                        response = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
                        buffer = io.BytesIO(response["Body"].read())
                        data_frames.append(pd.read_parquet(buffer))
                        transformation_logger.info(f"File {file_key} read successfully.")
                    except Exception as e:
                        transformation_logger.error(f"Error reading file {file_key}: {e}")

    concatenated_df = pd.concat(data_frames, ignore_index=True) if data_frames else pd.DataFrame()
    transformation_logger.info(f"Successfully concatenated {len(concatenated_df)} records from filtered files.")
    return concatenated_df


def calculate_store_ratio(group):
    """
    Calcule le ratio ventes/visiteurs pour un groupe de données et ajuste les valeurs aberrantes.

    Args:
        group (pd.DataFrame): Données groupées par magasin.

    Returns:
        pd.DataFrame: Groupe ajusté avec les ratios recalculés.
    """
    try:
        transformation_logger.info("Calculating store ratio.")
        valid_mask = (group["visitors"] <= 5000) & (group["visitors"] > 0)
        if valid_mask.any():
            ratio_mean = (
                100
                * group.loc[valid_mask, "sales"].sum()
                / group.loc[valid_mask, "visitors"].sum()
            )

            mask = group["visitors"] > 5000
            group.loc[mask, "visitors"] = group.loc[mask, "sales"] * ratio_mean

        transformation_logger.info("Store ratio calculated successfully.")
        return group
    except Exception as e:
        transformation_logger.error(f"Error calculating store ratio: {e}")
        raise


def aggregate_retail_data(retail_data):
    """
    Agrège les données retail par date et magasin, incluant les heures de pointe.

    Args:
        retail_data (pd.DataFrame): Données retail.

    Returns:
        pd.DataFrame: Données agrégées avec les sommations et heures de pointe.
    """
    try:
        transformation_logger.info("Aggregating retail data.")
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
        transformation_logger.info(f"Retail data aggregated successfully with {len(retail_agg)} records.")
        return retail_agg
    except Exception as e:
        transformation_logger.error(f"Error aggregating retail data: {e}")
        raise


def calculate_moving_averages(metrics_df, historical_data=None):
    """
    Calcule les moyennes glissantes des métriques sur 4 semaines.

    Args:
        metrics_df (pd.DataFrame): Données métriques quotidiennes.
        historical_data (pd.DataFrame, optional): Données historiques. Par défaut, None.

    Returns:
        pd.DataFrame: Données avec les moyennes glissantes calculées.
    """
    try:
        transformation_logger.info("Calculating moving averages.")
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

        transformation_logger.info("Moving averages calculated successfully.")
        return result_df
    except Exception as e:
        transformation_logger.error(f"Error calculating moving averages: {e}")
        raise


def process_best_selling(sales_with_cost):
    """
    Identifie les produits les plus vendus par magasin et par date.

    Args:
        sales_with_cost (pd.DataFrame): Données de ventes incluant les coûts des produits.

    Returns:
        pd.DataFrame: Meilleurs produits par date et magasin.
    """
    try:
        transformation_logger.info("Processing best-selling products.")
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

        transformation_logger.info("Best-selling products processed successfully.")
        return best_selling
    except Exception as e:
        transformation_logger.error(f"Error processing best-selling products: {e}")
        raise


def calculate_final_metrics(df):
    """
    Calcule les métriques finales pour les magasins, incluant les taux de conversion et les variations.

    Args:
        df (pd.DataFrame): Données avec les métriques de base.

    Returns:
        pd.DataFrame: Données avec les métriques finales calculées.
    """
    try:
        transformation_logger.info("Calculating final metrics.")
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

        transformation_logger.info("Final metrics calculated successfully.")
        return metrics
    except Exception as e:
        transformation_logger.error(f"Error calculating final metrics: {e}")
        raise


def append_to_existing_metrics(new_metrics, s3_key, is_test=False):
    """
    Ajoute les nouvelles métriques aux données existantes sur S3.

    Args:
        new_metrics (pd.DataFrame): Nouvelles métriques à ajouter.
        s3_key (str): Chemin du fichier sur S3.
        is_test (bool, optional): Si True, simule une sauvegarde pour les tests. Par défaut, False.
    """
    try:
        transformation_logger.info(f"Appending metrics to S3 file: {s3_key}.")
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
        transformation_logger.warning(f"No existing metrics file found at {s3_key}. Creating new file.")
        # Si le fichier n'existe pas, utiliser uniquement les nouvelles métriques
        combined_metrics = new_metrics
        combined_metrics["date"] = pd.to_datetime(combined_metrics["date"]).dt.strftime(
            "%Y-%m-%d"
        )

    try:
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
        transformation_logger.info(f"Metrics successfully saved to S3 at {s3_key}.")
    except Exception as e:
        transformation_logger.error(f"Error saving metrics to S3: {e}")
        raise


def calculate_daily_metrics():
    """
    Fonction principale pour traiter les données quotidiennes, calculer les métriques, et les sauvegarder.
    """
    try:
        transformation_logger.info("Starting daily metrics calculation.")

        processed_dates = get_processed_dates("processed_data/traffic_metrics.parquet")
        transformation_logger.info(f"Fetched {len(processed_dates)} processed dates.")

        # Lire les données
        retail_data = read_parquet_from_s3_filtered(
            "extracted_data/retail_data/", processed_dates
        )
        sales_data = read_parquet_from_s3_filtered("extracted_data/sales/", processed_dates)
        products_data = read_parquet_from_s3("extracted_data/products.parquet")

        if retail_data.empty or sales_data.empty or products_data.empty:
            transformation_logger.warning("No new data to process.")
            return

        # Traitement des données retail
        transformation_logger.info("Processing retail data.")
        retail_data = retail_data[
            ~(retail_data["visitors"].isna() & retail_data["sales"].isna())
        ]
        retail_data = (
            retail_data.groupby("store_id")
            .apply(calculate_store_ratio)
            .reset_index(drop=True)
        )
        retail_agg = aggregate_retail_data(retail_data)
        transformation_logger.info(f"Retail data processed successfully with {len(retail_agg)} records.")

        # Traitement des ventes
        transformation_logger.info("Processing sales data.")
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
        transformation_logger.info("Sales data processed successfully.")

        # Fusionner toutes les données
        transformation_logger.info("Merging all data for final metrics.")
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
        transformation_logger.info("Calculating moving averages and final metrics.")
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
        transformation_logger.info(f"Metrics calculated successfully for {len(daily_metrics)} records.")

        # Sauvegarder les résultats
        append_to_existing_metrics(daily_metrics, "processed_data/traffic_metrics.parquet")
        transformation_logger.info(f"Added {len(daily_metrics)} new rows to metrics.")
    except Exception as e:
        transformation_logger.error(f"Error in daily metrics calculation: {e}")
        raise


if __name__ == "__main__":
    try:
        calculate_daily_metrics()
        transformation_logger.info("Daily metrics calculation completed successfully.")
    except Exception as e:
        transformation_logger.critical(f"Critical failure in daily metrics calculation: {e}")
