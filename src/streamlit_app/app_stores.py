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

# Configuration du bucket et du fichier
BUCKET_NAME = "retail-insights-bucket"
S3_KEY = "extracted_data/stores.parquet"


def load_store_data_from_s3():
    """
    Charge les données des magasins depuis un fichier Parquet stocké sur S3.

    :return: DataFrame Pandas contenant les données des magasins.
    """
    try:
        # Télécharger le fichier Parquet depuis S3
        response = s3.get_object(Bucket=BUCKET_NAME, Key=S3_KEY)
        buffer = io.BytesIO(response["Body"].read())

        # Charger le contenu dans un DataFrame Pandas
        df = pd.read_parquet(buffer)
        return df
    except Exception as e:
        st.error(f"Erreur lors du chargement des données depuis S3 : {e}")
        return pd.DataFrame()


def main():
    st.title("Visualisation des magasins")
    st.markdown("### Sélectionnez un magasin pour afficher ses informations")

    # Charger les données des magasins depuis S3
    stores_df = load_store_data_from_s3()

    if stores_df.empty:
        st.warning("Aucune donnée de magasin disponible.")
        return

    # Afficher une liste déroulante avec les noms des magasins
    store_names = stores_df['name'].unique() if 'name' in stores_df.columns else []
    selected_store = st.selectbox("Choisissez un magasin :", store_names)

    if selected_store:
        # Afficher les détails du magasin sélectionné
        store_details = stores_df[stores_df['name'] == selected_store]
        st.write(f"**Informations sur le magasin sélectionné :**")
        st.dataframe(store_details)


if __name__ == "__main__":
    main()
