import streamlit as st
import duckdb
from dotenv import load_dotenv
import os
import boto3
import pandas as pd
import io

# Charger les variables d'environnement
load_dotenv()

# Récupérer les clés et région depuis le fichier .env
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
region_name = os.getenv("AWS_REGION")

# Initialiser le client S3
s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name,
)

# Configuration des fichiers S3
BUCKET_NAME = "retail-insights-bucket"
STORES_KEY = "extracted_data/stores.parquet"
METRICS_KEY = "processed_data/traffic_metrics.parquet"


def load_data_from_s3(s3_key):
    """
    Charge les données depuis un fichier Parquet stocké sur S3.

    :param s3_key: Chemin du fichier dans le bucket S3.
    :return: DataFrame Pandas contenant les données.
    """
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        buffer = io.BytesIO(response["Body"].read())
        return pd.read_parquet(buffer)
    except Exception as e:
        st.error(f"Erreur lors du chargement des données depuis S3 : {e}")
        return pd.DataFrame()

def main():
    st.title("Visualisation des métriques des magasins")
    st.markdown("### Sélectionnez un magasin pour afficher ses métriques")

    # Charger les données des magasins et des métriques
    stores_df = load_data_from_s3(STORES_KEY)
    metrics_df = load_data_from_s3(METRICS_KEY)

    if stores_df.empty or metrics_df.empty:
        st.warning("Données manquantes ou non disponibles.")
        return

    # Afficher une liste déroulante avec les noms des magasins
    store_names = stores_df['name'].unique() if 'name' in stores_df.columns else []
    selected_store = st.selectbox("Choisissez un magasin :", store_names)

    if selected_store:
        # Récupérer l'ID du magasin sélectionné
        selected_store_id = stores_df.loc[stores_df['name'] == selected_store, 'id'].values[0]

        # Filtrer les métriques pour ce magasin
        store_metrics = metrics_df[metrics_df['store_id'] == selected_store_id]

        if store_metrics.empty:
            st.warning("Aucune métrique disponible pour ce magasin.")
        else:
            st.write(f"**Métriques quotidiennes pour le magasin : {selected_store}**")
            st.dataframe(store_metrics)

            # Option pour télécharger les données en CSV
            csv_data = store_metrics.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Télécharger les données en CSV",
                data=csv_data,
                file_name=f'metrics_{selected_store_id}.csv',
                mime='text/csv'
            )


if __name__ == "__main__":
    main()
