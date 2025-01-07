import streamlit as st
import duckdb
from dotenv import load_dotenv
import os
import boto3
import pandas as pd
import io
import matplotlib.pyplot as plt

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


@st.cache_data
def load_data_from_s3(s3_key):
    """
    Charge les données depuis un fichier Parquet stocké sur S3 et les met en cache.

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


def plot_theme_metrics(data, metrics, title):
    """
    Génère et affiche un ensemble de courbes pour un thème.

    :param data: DataFrame contenant les données filtrées.
    :param metrics: Liste des métriques à tracer.
    :param title: Titre du graphique regroupé.
    """
    plt.figure(figsize=(12, 6))
    for metric in metrics:
        if metric in data.columns:
            plt.plot(data['date'], data[metric], marker='o', label=metric)
    plt.title(title)
    plt.xlabel("Date")
    plt.ylabel("Valeurs")
    plt.grid(True)
    plt.legend()
    plt.xticks(rotation=90)
    st.pyplot(plt)


def main():
    st.title("Visualisation des métriques des magasins")
    st.markdown("### Sélectionnez un magasin, une période et un thème pour afficher les métriques")

    # Charger les données des magasins et des métriques (caching appliqué)
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
            return

        # Extraire les années et mois disponibles
        store_metrics['date'] = pd.to_datetime(store_metrics['date'])
        store_metrics['year'] = store_metrics['date'].dt.year
        store_metrics['month'] = store_metrics['date'].dt.month

        years = store_metrics['year'].unique()
        selected_year = st.selectbox("Choisissez une année :", sorted(years))

        if selected_year:
            filtered_metrics = store_metrics[store_metrics['year'] == selected_year]

            months = filtered_metrics['month'].unique()
            month_names = {1: "Janvier", 2: "Février", 3: "Mars", 4: "Avril", 5: "Mai",
                           6: "Juin", 7: "Juillet", 8: "Août", 9: "Septembre", 10: "Octobre",
                           11: "Novembre", 12: "Décembre"}
            selected_month = st.selectbox("Choisissez un mois :", [month_names[m] for m in sorted(months)])

            if selected_month:
                # Convertir le nom du mois sélectionné en numéro
                selected_month_num = [k for k, v in month_names.items() if v == selected_month][0]
                final_metrics = filtered_metrics[filtered_metrics['month'] == selected_month_num]

                if final_metrics.empty:
                    st.warning("Aucune donnée disponible pour cette période.")
                    return

                st.write(f"**Métriques quotidiennes pour le magasin : {selected_store} - {selected_month} {selected_year}**")

                # Définir les thèmes et les métriques associées
                themes = {
                    "Performance des visiteurs": [
                        "total_visitors", "avg_visitors_last_4_weeks", "visitors_variation_vs_avg_4w_percent"
                    ],
                    "Performance des transactions": [
                        "total_transactions", "avg_sales_last_4_weeks", "transactions_variation_vs_avg_4w_percent"
                    ],
                    "Quantité et revenus": [
                        "total_quantity", "total_revenue", "avg_revenue_last_4_weeks", "revenue_variation_vs_avg_4w_percent"
                    ],
                    "Marges et coûts": [
                        "total_cost", "total_margin"
                    ],
                    "Efficacité des ventes": [
                        "conversion_rate", "avg_transaction_value", "transactions_amount_variation_vs_avg_4w_percent"
                    ],
                    "Indicateurs spécifiques par visiteur": [
                        "revenue_per_visitor", "margin_per_visitor"
                    ],
                }

                # Sélectionner un thème
                selected_theme = st.selectbox("Choisissez un thème :", list(themes.keys()))
                if selected_theme:
                    selected_metrics = themes[selected_theme]

                    # Afficher un graphique regroupé pour le thème sélectionné
                    plot_theme_metrics(final_metrics, selected_metrics,
                                       f"{selected_theme} - {selected_store} ({selected_month} {selected_year})")

                # Option pour télécharger les données en CSV
                csv_data = final_metrics.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Télécharger les données en CSV",
                    data=csv_data,
                    file_name=f'metrics_{selected_store_id}_{selected_year}_{selected_month_num}.csv',
                    mime='text/csv'
                )


if __name__ == "__main__":
    main()
