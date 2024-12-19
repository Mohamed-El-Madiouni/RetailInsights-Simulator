import os
import pandas as pd
from pyspark.sql import SparkSession


# Créer le dossier de sortie s'il n'existe pas
def create_output_folder(folder_name="data"):
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
        print(f"Dossier créé : {folder_name}")
    return folder_name


# Sauvegarder les données en Parquet avec Pandas
def save_with_pandas(data, output_file):
    df = pd.DataFrame(data)
    df.to_parquet(output_file, engine="pyarrow", compression="snappy")
    print(f"Fichier sauvegardé avec Pandas : {output_file}")


# Sauvegarder les données en Parquet avec Spark
def save_with_spark(data, output_file):
    spark = SparkSession.builder \
        .appName("API Data to Parquet") \
        .master("local[*]") \
        .getOrCreate()

    df = spark.read.json(spark.sparkContext.parallelize([data]))
    df.write.parquet(output_file, mode="overwrite", compression="snappy")
    print(f"Fichier sauvegardé avec Spark : {output_file}")

    spark.stop()


# Récupérer des données depuis une API
def fetch_from_api(url):
    import requests
    response = requests.get(url)
    if response.status_code == 200:
        print(f"Données récupérées depuis {url}")
        return response.json()
    else:
        print(f"Erreur lors de la récupération des données depuis {url}: {response.status_code}")
        raise f"Erreur lors de la récupération des données depuis {url}: {response.status_code}"
