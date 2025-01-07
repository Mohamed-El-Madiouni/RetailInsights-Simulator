import streamlit as st
import boto3
import pandas as pd
import io
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import os

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
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        buffer = io.BytesIO(response["Body"].read())
        return pd.read_parquet(buffer)
    except Exception as e:
        st.error(f"Erreur lors du chargement des données depuis S3 : {e}")
        return pd.DataFrame()


def plot_theme_metrics(data, metrics, title, period_type):
    plt.figure(figsize=(12, 6))

    # Métriques à sommer
    sum_metrics = [
        'total_visitors', 'total_transactions', 'total_quantity',
        'total_revenue', 'total_cost', 'total_margin'
    ]

    for metric in metrics:
        if metric in data.columns:
            agg_func = 'sum' if metric in sum_metrics else 'mean'

            if period_type == "Mensuel":
                data_grouped = data.groupby(['year', 'month'])[metric].agg(agg_func).reset_index()
                data_grouped['date'] = pd.to_datetime(
                    data_grouped['year'].astype(str) + '-' + data_grouped['month'].astype(str).str.zfill(2))
                plt.plot(data_grouped['date'], data_grouped[metric], marker='o', label=metric)
            elif period_type == "Annuel":
                data_grouped = data.groupby(['year'])[metric].agg(agg_func).reset_index()
                plt.plot(data_grouped['year'], data_grouped[metric], marker='o', label=metric)
                plt.xticks(data_grouped['year'], data_grouped['year'].astype(int))
            elif period_type == "Quotidien":
                plt.plot(data['date'], data[metric], marker='o', label=metric)
            elif period_type == "Trimestriel":
                data_sorted = data.sort_values(['year', 'quarter'])
                data_grouped = data_sorted.groupby('quarter')[metric].agg(agg_func).reset_index()
                plt.plot(data_grouped['quarter'], data_grouped[metric], marker='o', label=metric)

    plt.title(title)
    plt.xlabel(period_type)
    plt.ylabel("Valeurs")
    plt.grid(True)
    plt.legend()
    plt.xticks(rotation=90)
    st.pyplot(plt)


def get_themes(period_type):
    if period_type == "Quotidien":
        return {
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
            ]
        }
    else:
        return {
            "Performance des visiteurs": ["total_visitors"],
            "Performance des transactions": ["total_transactions"],
            "Quantité et revenus": ["total_quantity", "total_revenue"],
            "Marges et coûts": ["total_cost", "total_margin"],
            "Efficacité des ventes": ["conversion_rate", "avg_transaction_value"],
            "Indicateurs spécifiques par visiteur": ["revenue_per_visitor", "margin_per_visitor"]
        }


def main():
    st.title("Visualisation des métriques des magasins")
    st.markdown("### Sélectionnez un magasin, un type de graphique et une période pour afficher les métriques")

    stores_df = load_data_from_s3(STORES_KEY)
    metrics_df = load_data_from_s3(METRICS_KEY)

    if stores_df.empty or metrics_df.empty:
        st.warning("Données manquantes ou non disponibles.")
        return

    store_names = stores_df['name'].unique() if 'name' in stores_df.columns else []
    selected_store = st.selectbox("Choisissez un magasin :", store_names)

    if selected_store:
        selected_store_id = stores_df.loc[stores_df['name'] == selected_store, 'id'].values[0]
        store_metrics = metrics_df[metrics_df['store_id'] == selected_store_id]

        if store_metrics.empty:
            st.warning("Aucune métrique disponible pour ce magasin.")
            return

        store_metrics['date'] = pd.to_datetime(store_metrics['date'])
        store_metrics['year'] = store_metrics['date'].dt.year
        store_metrics['month'] = store_metrics['date'].dt.month

        graph_types = ["Annuel", "Trimestriel", "Mensuel", "Quotidien"]
        selected_graph_type = st.selectbox("Choisissez un type de graphique :", graph_types)

        if selected_graph_type == "Annuel":
            filtered_metrics = store_metrics.copy()
        elif selected_graph_type == "Trimestriel":
            filtered_metrics = store_metrics.copy()
            # Créer une colonne de tri et une colonne d'affichage
            filtered_metrics['quarter_for_sort'] = pd.to_datetime(filtered_metrics['date']).dt.to_period('Q')
            filtered_metrics['quarter'] = filtered_metrics['quarter_for_sort'].astype(str).str.replace('Q', '-Q')

            # Séparer les colonnes à sommer et à moyenner
            sum_metrics = [
                'total_visitors', 'total_transactions', 'total_quantity',
                'total_revenue', 'total_cost', 'total_margin'
            ]
            mean_metrics = filtered_metrics.select_dtypes(include=['number']).columns.difference(sum_metrics)

            # Appliquer les agrégations appropriées
            agg_dict = {metric: 'sum' for metric in sum_metrics}
            agg_dict.update({metric: 'mean' for metric in mean_metrics})

            filtered_metrics = filtered_metrics.groupby(['quarter', 'store_id', 'quarter_for_sort']).agg(
                agg_dict).reset_index()
            filtered_metrics = filtered_metrics.sort_values('quarter_for_sort')
        elif selected_graph_type == "Mensuel":
            years = store_metrics['year'].unique()
            selected_year = st.selectbox("Choisissez une année :", sorted(years))
            if selected_year:
                filtered_metrics = store_metrics[store_metrics['year'] == selected_year]
            else:
                filtered_metrics = pd.DataFrame()
        else:  # Quotidien
            years = store_metrics['year'].unique()
            selected_year = st.selectbox("Choisissez une année :", sorted(years))
            if selected_year:
                months = store_metrics[store_metrics['year'] == selected_year]['month'].unique()
                month_names = {1: "Janvier", 2: "Février", 3: "Mars", 4: "Avril", 5: "Mai",
                               6: "Juin", 7: "Juillet", 8: "Août", 9: "Septembre", 10: "Octobre",
                               11: "Novembre", 12: "Décembre"}
                selected_month = st.selectbox("Choisissez un mois :", [month_names[m] for m in sorted(months)])
                if selected_month:
                    selected_month_num = [k for k, v in month_names.items() if v == selected_month][0]
                    filtered_metrics = store_metrics[(store_metrics['year'] == selected_year) &
                                                     (store_metrics['month'] == selected_month_num)]
                else:
                    filtered_metrics = pd.DataFrame()
            else:
                filtered_metrics = pd.DataFrame()

        if filtered_metrics.empty:
            st.warning("Aucune donnée disponible pour cette période.")
            return

        st.write(f"**Métriques pour le magasin : {selected_store} - {selected_graph_type}**")

        themes = get_themes(selected_graph_type)


        selected_theme = st.selectbox("Choisissez un thème :", list(themes.keys()))
        if selected_theme:
            selected_metrics = themes[selected_theme]

            plot_theme_metrics(filtered_metrics, selected_metrics,
                               f"{selected_theme} - {selected_store} ({selected_graph_type})", selected_graph_type)

        csv_data = filtered_metrics.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Télécharger les données en CSV",
            data=csv_data,
            file_name=f'metrics_{selected_store_id}_{selected_graph_type}.csv',
            mime='text/csv'
        )


if __name__ == "__main__":
    main()
