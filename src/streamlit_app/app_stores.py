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


def get_metric_groups_and_labels(theme):
    groups = {
        "Performance des visiteurs": {
            "group1": {
                "metrics": ["total_visitors", "avg_visitors_last_4_weeks"],
                "label": "Nombre de visiteurs",
                "title": "Affluence visiteurs"
            },
            "group2": {
                "metrics": ["visitors_variation_vs_avg_4w_percent"],
                "label": "Variation (%)",
                "title": "Évolution de l'affluence"
            }
        },
        "Performance des transactions": {
            "group1": {
                "metrics": ["total_transactions", "avg_sales_last_4_weeks"],
                "label": "Nombre de transactions",
                "title": "Volume des transactions"
            },
            "group2": {
                "metrics": ["transactions_variation_vs_avg_4w_percent"],
                "label": "Variation (%)",
                "title": "Évolution des transactions"
            }
        },
        "Quantité et revenus": {
            "group1": {
                "metrics": ["total_revenue", "avg_revenue_last_4_weeks"],
                "label": "Revenus (€)",
                "title": "Chiffre d'affaires"
            },
            "group2": {
                "metrics": ["total_quantity", "revenue_variation_vs_avg_4w_percent"],
                "label": "Quantité / Variation (%)",
                "title": "Quantités vendues et variations"
            }
        },
        "Marges et coûts": {
            "group1": {
                "metrics": ["total_cost", "total_margin"],
                "label": "Euros (€)",
                "title": "Marges et coûts"
            }
        },
        "Efficacité des ventes": {
            "group1": {
                "metrics": ["conversion_rate", "transactions_amount_variation_vs_avg_4w_percent"],
                "label": "Taux (%)",
                "title": "Taux de conversion"
            },
            "group2": {
                "metrics": ["avg_transaction_value"],
                "label": "Valeur moyenne (€)",
                "title": "Panier moyen"
            }
        },
        "Indicateurs spécifiques par visiteur": {
            "group1": {
                "metrics": ["revenue_per_visitor", "margin_per_visitor"],
                "label": "Euros par visiteur (€)",
                "title": "Performance par visiteur"
            }
        }
    }
    return groups.get(theme, {})


def plot_theme_metrics(data, metrics, title, period_type):
    theme_name = title.split(" - ")[0]
    metric_groups = get_metric_groups_and_labels(theme_name)

    sum_metrics = [
        'total_visitors', 'total_transactions', 'total_quantity',
        'total_revenue', 'total_cost', 'total_margin'
    ]
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']
    store_period = title.split(" - ", 1)[1]

    # Cas spécial pour "Quantité et revenus" groupe 2
    if theme_name == "Quantité et revenus" and "group2" in metric_groups:
        fig, ax1 = plt.subplots(figsize=(12, 6))
        ax2 = ax1.twinx()

        # total_quantity sur axe gauche
        if "total_quantity" in metrics and "total_quantity" in data.columns:
            plot_data = prepare_plot_data(data, "total_quantity", period_type, 'sum')
            ax1.plot(plot_data['x'], plot_data['y'], marker='o', label="total_quantity", color=colors[0])
            ax1.set_ylabel("Quantité")

        # variation sur axe droit
        if "revenue_variation_vs_avg_4w_percent" in metrics and "revenue_variation_vs_avg_4w_percent" in data.columns:
            plot_data = prepare_plot_data(data, "revenue_variation_vs_avg_4w_percent", period_type, 'mean')
            ax2.plot(plot_data['x'], plot_data['y'], marker='s', linestyle='--',
                     label="revenue_variation_vs_avg_4w_percent", color=colors[1])
            ax2.set_ylabel("Variation (%)")

        ax1.set_xlabel(period_type)
        plt.xticks(rotation=90)

        # Combiner les légendes
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')

        plt.title(f"{metric_groups['group2']['title']} - {store_period}")
        plt.grid(True)
        st.pyplot(fig)
        return

    if not metric_groups:
        fig, ax1 = plt.subplots(figsize=(12, 6))
        for i, metric in enumerate(metrics):
            if metric in data.columns:
                agg_func = 'sum' if metric in sum_metrics else 'mean'
                plot_data = prepare_plot_data(data, metric, period_type, agg_func)
                ax1.plot(plot_data['x'], plot_data['y'], marker='o', label=metric, color=colors[i])

        ax1.set_xlabel(period_type)
        ax1.set_ylabel("Valeurs")
        ax1.grid(True)
        ax1.legend()
        plt.xticks(rotation=90)
        plt.title(title)
        st.pyplot(fig)
    else:
        # Graphique pour le groupe 1
        if "group1" in metric_groups:
            fig1, ax1 = plt.subplots(figsize=(12, 6))
            for i, metric in enumerate(metric_groups["group1"]["metrics"]):
                if metric in data.columns and metric in metrics:
                    agg_func = 'sum' if metric in sum_metrics else 'mean'
                    plot_data = prepare_plot_data(data, metric, period_type, agg_func)
                    ax1.plot(plot_data['x'], plot_data['y'], marker='o', label=metric, color=colors[i])

            ax1.set_xlabel(period_type)
            ax1.set_ylabel(metric_groups["group1"]["label"])
            ax1.grid(True)
            ax1.legend()
            plt.xticks(rotation=90)
            plt.title(f"{metric_groups['group1']['title']} - {store_period}")
            st.pyplot(fig1)

        # Graphique pour le groupe 2 (sauf pour "Quantité et revenus" qui est géré plus haut)
        if "group2" in metric_groups and theme_name != "Quantité et revenus":
            fig2, ax2 = plt.subplots(figsize=(12, 6))
            for i, metric in enumerate(metric_groups["group2"]["metrics"]):
                if metric in data.columns and metric in metrics:
                    agg_func = 'sum' if metric in sum_metrics else 'mean'
                    plot_data = prepare_plot_data(data, metric, period_type, agg_func)
                    ax2.plot(plot_data['x'], plot_data['y'], marker='o', label=metric, color=colors[i])

            ax2.set_xlabel(period_type)
            ax2.set_ylabel(metric_groups["group2"]["label"])
            ax2.grid(True)
            ax2.legend()
            plt.xticks(rotation=90)
            plt.title(f"{metric_groups['group2']['title']} - {store_period}")
            st.pyplot(fig2)


def prepare_plot_data(data, metric, period_type, agg_func):
    if period_type == "Mensuel":
        data_grouped = data.groupby(['year', 'month'])[metric].agg(agg_func).reset_index()
        data_grouped['x'] = pd.to_datetime(
            data_grouped['year'].astype(str) + '-' + data_grouped['month'].astype(str).str.zfill(2))
        return {'x': data_grouped['x'], 'y': data_grouped[metric]}

    elif period_type == "Annuel":
        data_grouped = data.groupby(['year'])[metric].agg(agg_func).reset_index()
        return {'x': data_grouped['year'], 'y': data_grouped[metric]}

    elif period_type == "Quotidien":
        return {'x': data['date'], 'y': data[metric]}

    elif period_type == "Trimestriel":
        data_sorted = data.sort_values('quarter_for_sort')
        data_grouped = data_sorted.groupby('quarter')[metric].agg(agg_func).reset_index()
        return {'x': data_grouped['quarter'], 'y': data_grouped[metric]}

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
