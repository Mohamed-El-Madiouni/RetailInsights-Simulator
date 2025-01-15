import io
import os
from datetime import datetime

import boto3
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from src.streamlit_app.logger_streamlit import streamlit_logger

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
    Charge les données d'un fichier Parquet depuis un bucket S3.

    Args:
        s3_key (str): Chemin du fichier dans le bucket S3.

    Returns:
        pd.DataFrame: Données chargées sous forme de DataFrame Pandas.
    """
    try:
        streamlit_logger.info(f"Loading data from S3: {s3_key}")
        response = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        buffer = io.BytesIO(response["Body"].read())
        data = pd.read_parquet(buffer)
        streamlit_logger.info(f"Data loaded successfully from {s3_key} with {len(data)} records.")
        return data
    except Exception as e:
        streamlit_logger.error(f"Error loading data from S3 ({s3_key}): {e}")
        st.error(f"Erreur lors du chargement des données depuis S3 : {e}")
        return pd.DataFrame()


def get_metric_groups_and_labels(theme, period_type):
    """
    Récupère les groupes de métriques et leurs étiquettes pour un thème et un type de période spécifiques.

    Args:
        theme (str): Thème des métriques (ex. "Quantité et revenus").
        period_type (str): Type de période (ex. "Quotidien", "Mensuel").

    Returns:
        dict: Groupes de métriques organisés par thème.
    """
    try:
        streamlit_logger.info(f"Fetching metric groups for theme '{theme}' and period '{period_type}'.")
        groups = {
            "Performance des visiteurs": {
                "group1": {
                    "metrics": ["total_visitors", "avg_visitors_last_4_weeks"],
                    "label": "Nombre de visiteurs",
                    "title": "Affluence visiteurs",
                },
                "group2": {
                    "metrics": ["visitors_variation_vs_avg_4w_percent"],
                    "label": "Variation (%)",
                    "title": "Évolution de l'affluence",
                },
            },
            "Performance des transactions": {
                "group1": {
                    "metrics": ["total_transactions", "avg_sales_last_4_weeks"],
                    "label": "Nombre de transactions",
                    "title": "Volume des transactions",
                },
                "group2": {
                    "metrics": ["transactions_variation_vs_avg_4w_percent"],
                    "label": "Variation (%)",
                    "title": "Évolution des transactions",
                },
            },
            "Quantité et revenus": {
                "group1": {
                    "metrics": ["total_revenue"],
                    "label": "Revenus (€)",
                    "title": "Chiffre d'affaires",
                },
                "group2": {
                    "metrics": ["total_quantity"],
                    "label": "Quantité",
                    "title": "Quantités vendues",
                },
            },
            "Marges et coûts": {
                "group1": {
                    "metrics": ["total_cost", "total_margin"],
                    "label": "Euros (€)",
                    "title": "Marges et coûts",
                }
            },
            "Efficacité des ventes": {
                "group1": {
                    "metrics": ["conversion_rate"],
                    "label": "Taux (%)",
                    "title": "Taux de conversion",
                },
                "group2": {
                    "metrics": ["avg_transaction_value"],
                    "label": "Valeur moyenne (€)",
                    "title": "Panier moyen",
                },
            },
            "Indicateurs spécifiques par visiteur": {
                "group1": {
                    "metrics": ["revenue_per_visitor", "margin_per_visitor"],
                    "label": "Euros par visiteur (€)",
                    "title": "Performance par visiteur",
                }
            },
        }

        if period_type == "Quotidien":
            # Ajouter les métriques supplémentaires pour le quotidien
            groups["Quantité et revenus"]["group1"]["metrics"].append(
                "avg_revenue_last_4_weeks"
            )
            groups["Quantité et revenus"]["group2"]["metrics"].append(
                "revenue_variation_vs_avg_4w_percent"
            )

        if theme in groups:
            theme_groups = groups[theme].copy()

            if period_type != "Quotidien":
                # Pour les périodes non quotidiennes, supprimer le groupe des variations
                # sauf pour "Quantité et revenus"
                if theme != "Quantité et revenus":
                    if "group2" in theme_groups and any(
                        "variation" in metric
                        for metric in theme_groups["group2"]["metrics"]
                    ):
                        del theme_groups["group2"]
                # Supprimer les métriques de moyenne mobile du groupe 1
                if "group1" in theme_groups:
                    theme_groups["group1"]["metrics"] = [
                        m for m in theme_groups["group1"]["metrics"] if "avg" not in m
                    ]

            streamlit_logger.info(f"Metric groups fetched successfully for theme '{theme}' and period '{period_type}'.")
            return theme_groups

        streamlit_logger.warning(f"No metric groups found for theme '{theme}'.")
        return {}
    except Exception as e:
        streamlit_logger.error(f"Error fetching metric groups: {e}")
        return {}


def plot_theme_metrics(data, metrics, title, period_type):
    """
    Génère et affiche des graphiques interactifs pour un thème donné.

    Args:
        data (pd.DataFrame): Données à afficher.
        metrics (list): Liste des métriques à inclure dans le graphique.
        title (str): Titre du graphique.
        period_type (str): Type de période (ex. "Annuel", "Mensuel").
    """
    try:
        theme_name = title.split(" - ")[0]
        store_period = title.split(" - ", 1)[1]
        streamlit_logger.info(
            f"Generating metrics plot for theme '{theme_name}' and period '{store_period}'."
        )

        metric_groups = get_metric_groups_and_labels(theme_name, period_type)
        sum_metrics = [
            "total_visitors",
            "total_transactions",
            "total_quantity",
            "total_revenue",
            "total_cost",
            "total_margin",
            "avg_revenue_last_4_weeks",
            "avg_visitors_last_4_weeks",
            "avg_sales_last_4_weeks",
        ]
        colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b"]

        def create_base_figure():
            return go.Figure(
                layout=go.Layout(
                    title=dict(text=title, x=0.5),
                    xaxis=dict(title=period_type, tickangle=45),
                    yaxis=dict(title="Valeurs", gridcolor="LightGrey"),
                    showlegend=True,
                    template="plotly_white",
                    hovermode="x unified",
                    height=600,
                )
            )

        # Cas spécial pour "Quantité et revenus"
        if theme_name == "Quantité et revenus":
            # Premier graphique (group1) - Revenus
            if "group1" in metric_groups:
                fig1 = create_base_figure()
                for i, metric in enumerate(metric_groups["group1"]["metrics"]):
                    if metric in data.columns and metric in metrics:
                        agg_func = "sum" if metric in sum_metrics else "mean"
                        plot_data = prepare_plot_data(data, metric, period_type, agg_func)
                        fig1.add_trace(
                            go.Scatter(
                                x=plot_data["x"],
                                y=plot_data["y"],
                                mode="lines+markers",
                                name=metric,
                                line=dict(color=colors[i]),
                            )
                        )
                fig1.update_layout(
                    title=f"{metric_groups['group1']['title']} - {store_period}",
                    yaxis_title=metric_groups["group1"]["label"],
                )
                st.plotly_chart(fig1, use_container_width=True)
                streamlit_logger.info(f"Group 1 metrics plotted for theme '{theme_name}'.")

            # Deuxième graphique (group2)
            if "group2" in metric_groups:
                if period_type == "Quotidien":
                    # Version avec double axe pour données quotidiennes
                    fig2 = create_base_figure()

                    # total_quantity sur axe gauche
                    if "total_quantity" in metrics and "total_quantity" in data.columns:
                        plot_data = prepare_plot_data(
                            data, "total_quantity", period_type, "sum"
                        )
                        fig2.add_trace(
                            go.Scatter(
                                x=plot_data["x"],
                                y=plot_data["y"],
                                mode="lines+markers",
                                name="total_quantity",
                                line=dict(color=colors[0]),
                            )
                        )

                    # variation sur axe droit
                    if "revenue_variation_vs_avg_4w_percent" in metrics:
                        plot_data = prepare_plot_data(
                            data, "revenue_variation_vs_avg_4w_percent", period_type, "mean"
                        )
                        fig2.add_trace(
                            go.Scatter(
                                x=plot_data["x"],
                                y=plot_data["y"],
                                mode="lines+markers",
                                name="revenue_variation_vs_avg_4w_percent",
                                line=dict(color=colors[1], dash="dash"),
                                yaxis="y2",
                            )
                        )

                    fig2.update_layout(
                        title=f"{metric_groups['group2']['title']} - {store_period}",
                        yaxis=dict(title="Quantité", gridcolor="LightGrey"),
                        yaxis2=dict(
                            title="Variation (%)",
                            overlaying="y",
                            side="right",
                            gridcolor="LightGrey",
                        ),
                    )
                else:
                    # Version simple pour les autres périodes
                    fig2 = create_base_figure()
                    plot_data = prepare_plot_data(
                        data, "total_quantity", period_type, "sum"
                    )
                    fig2.add_trace(
                        go.Scatter(
                            x=plot_data["x"],
                            y=plot_data["y"],
                            mode="lines+markers",
                            name="total_quantity",
                            line=dict(color=colors[0]),
                        )
                    )
                    fig2.update_layout(
                        title=f"{metric_groups['group2']['title']} - {store_period}",
                        yaxis_title="Quantité",
                    )

                st.plotly_chart(fig2, use_container_width=True)
                streamlit_logger.info(f"Group 2 metrics plotted for theme '{theme_name}'.")
            return

        # Cas général
        if not metric_groups:
            fig = create_base_figure()
            for i, metric in enumerate(metrics):
                if metric in data.columns:
                    agg_func = "sum" if metric in sum_metrics else "mean"
                    plot_data = prepare_plot_data(data, metric, period_type, agg_func)
                    fig.add_trace(
                        go.Scatter(
                            x=plot_data["x"],
                            y=plot_data["y"],
                            mode="lines+markers",
                            name=metric,
                            line=dict(color=colors[i]),
                        )
                    )
            st.plotly_chart(fig, use_container_width=True)
            streamlit_logger.info(f"General metrics plotted for theme '{theme_name}'.")
        else:
            # Graphique pour groupe 1
            if "group1" in metric_groups:
                fig1 = create_base_figure()
                for i, metric in enumerate(metric_groups["group1"]["metrics"]):
                    if metric in data.columns and metric in metrics:
                        agg_func = "sum" if metric in sum_metrics else "mean"
                        plot_data = prepare_plot_data(data, metric, period_type, agg_func)
                        fig1.add_trace(
                            go.Scatter(
                                x=plot_data["x"],
                                y=plot_data["y"],
                                mode="lines+markers",
                                name=metric,
                                line=dict(color=colors[i]),
                            )
                        )
                fig1.update_layout(
                    title=f"{metric_groups['group1']['title']} - {store_period}",
                    yaxis_title=metric_groups["group1"]["label"],
                )
                st.plotly_chart(fig1, use_container_width=True)
                streamlit_logger.info(f"Group 1 metrics plotted for theme '{theme_name}'.")

            # Graphique pour groupe 2
            if "group2" in metric_groups and theme_name != "Quantité et revenus":
                fig2 = create_base_figure()
                for i, metric in enumerate(metric_groups["group2"]["metrics"]):
                    if metric in data.columns and metric in metrics:
                        agg_func = "sum" if metric in sum_metrics else "mean"
                        plot_data = prepare_plot_data(data, metric, period_type, agg_func)
                        fig2.add_trace(
                            go.Scatter(
                                x=plot_data["x"],
                                y=plot_data["y"],
                                mode="lines+markers",
                                name=metric,
                                line=dict(color=colors[i]),
                            )
                        )
                fig2.update_layout(
                    title=f"{metric_groups['group2']['title']} - {store_period}",
                    yaxis_title=metric_groups["group2"]["label"],
                )
                st.plotly_chart(fig2, use_container_width=True)
                streamlit_logger.info(f"Group 2 metrics plotted for theme '{theme_name}'.")
    except Exception as e:
        streamlit_logger.error(f"Error generating metrics plot for theme '{theme_name}': {e}")
        st.error(f"Erreur lors de la génération du graphique pour le thème '{theme_name}'.")


def prepare_plot_data(data, metric, period_type, agg_func):
    """
    Prépare les données à tracer en fonction des agrégations et du type de période.

    Args:
        data (pd.DataFrame): Données à transformer.
        metric (str): Nom de la métrique à préparer.
        period_type (str): Type de période (ex. "Quotidien", "Mensuel").
        agg_func (str): Fonction d'agrégation (ex. "sum", "mean").

    Returns:
        dict: Dictionnaire contenant les données pour les axes x et y.
    """
    try:
        streamlit_logger.info(
            f"Preparing plot data for metric '{metric}' and period '{period_type}'."
        )

        custom_metrics = ["conversion_rate", "avg_transaction_value"]

        if period_type == "Quotidien":
            final_data = data.groupby("date")[metric].agg(agg_func).reset_index()
            streamlit_logger.info(
                f"Plot data prepared successfully for metric '{metric}' (Daily)."
            )
            return {"x": final_data["date"], "y": final_data[metric]}

        elif period_type == "Mensuel":
            data_grouped = (
                data.groupby(["year", "month", "store_id"])
                .agg(
                    {
                        "total_transactions": "sum",
                        "total_visitors": "sum",
                        "total_revenue": "sum",
                    }
                )
                .reset_index()
            )
            final_data = (
                data_grouped.groupby(["year", "month"])
                .agg(
                    {
                        "total_transactions": "sum",
                        "total_visitors": "sum",
                        "total_revenue": "sum",
                    }
                )
                .reset_index()
            )

            if metric in custom_metrics:
                if metric == "conversion_rate":
                    final_data[metric] = (
                        100
                        * final_data["total_transactions"]
                        / final_data["total_visitors"]
                    )
                elif metric == "avg_transaction_value":
                    final_data[metric] = (
                        final_data["total_revenue"] / final_data["total_transactions"]
                    )
            else:
                final_data[metric] = (
                    data.groupby(["year", "month"])[metric]
                    .agg(agg_func)
                    .reset_index(drop=True)
                )

            final_data["x"] = pd.to_datetime(
                final_data["year"].astype(str)
                + "-"
                + final_data["month"].astype(str).str.zfill(2)
            )
            streamlit_logger.info(
                f"Plot data prepared successfully for metric '{metric}' (Monthly)."
            )
            return {"x": final_data["x"], "y": final_data[metric]}

        elif period_type == "Annuel":
            data_grouped = (
                data.groupby(["year", "store_id"])
                .agg(
                    {
                        "total_transactions": "sum",
                        "total_visitors": "sum",
                        "total_revenue": "sum",
                    }
                )
                .reset_index()
            )
            final_data = (
                data_grouped.groupby("year")
                .agg(
                    {
                        "total_transactions": "sum",
                        "total_visitors": "sum",
                        "total_revenue": "sum",
                    }
                )
                .reset_index()
            )

            if metric in custom_metrics:
                if metric == "conversion_rate":
                    final_data[metric] = (
                        100
                        * final_data["total_transactions"]
                        / final_data["total_visitors"]
                    )
                elif metric == "avg_transaction_value":
                    final_data[metric] = (
                        final_data["total_revenue"] / final_data["total_transactions"]
                    )
            else:
                final_data[metric] = (
                    data.groupby("year")[metric].agg(agg_func).reset_index(drop=True)
                )

            streamlit_logger.info(
                f"Plot data prepared successfully for metric '{metric}' (Annual)."
            )
            return {"x": final_data["year"], "y": final_data[metric]}

        elif period_type == "Trimestriel":
            data_grouped = (
                data.groupby(["quarter", "store_id", "quarter_for_sort"])
                .agg(
                    {
                        "total_transactions": "sum",
                        "total_visitors": "sum",
                        "total_revenue": "sum",
                    }
                )
                .reset_index()
            )
            final_data = (
                data_grouped.groupby(["quarter", "quarter_for_sort"])
                .agg(
                    {
                        "total_transactions": "sum",
                        "total_visitors": "sum",
                        "total_revenue": "sum",
                    }
                )
                .reset_index()
            )

            if metric in custom_metrics:
                if metric == "conversion_rate":
                    final_data[metric] = (
                        100
                        * final_data["total_transactions"]
                        / final_data["total_visitors"]
                    )
                elif metric == "avg_transaction_value":
                    final_data[metric] = (
                        final_data["total_revenue"] / final_data["total_transactions"]
                    )
            else:
                final_data[metric] = (
                    data.groupby(["quarter", "quarter_for_sort"])[metric]
                    .agg(agg_func)
                    .reset_index(drop=True)
                )

            final_data = final_data.sort_values("quarter_for_sort")
            streamlit_logger.info(
                f"Plot data prepared successfully for metric '{metric}' (Quarterly)."
            )
            return {"x": final_data["quarter"], "y": final_data[metric]}
    except Exception as e:
        streamlit_logger.error(f"Error preparing plot data for metric '{metric}': {e}")
        raise


def get_themes(period_type):
    """
    Récupère les thèmes et leurs métriques associées en fonction du type de période.

    Args:
        period_type (str): Type de période (ex. "Quotidien", "Mensuel").

    Returns:
        dict: Thèmes et métriques correspondantes.
    """
    try:
        streamlit_logger.info(f"Fetching themes for period type '{period_type}'.")
        if period_type == "Quotidien":
            themes = {
                "Performance des visiteurs": [
                    "total_visitors",
                    "avg_visitors_last_4_weeks",
                    "visitors_variation_vs_avg_4w_percent",
                ],
                "Performance des transactions": [
                    "total_transactions",
                    "avg_sales_last_4_weeks",
                    "transactions_variation_vs_avg_4w_percent",
                ],
                "Quantité et revenus": [
                    "total_quantity",
                    "total_revenue",
                    "avg_revenue_last_4_weeks",
                    "revenue_variation_vs_avg_4w_percent",
                ],
                "Marges et coûts": ["total_cost", "total_margin"],
                "Efficacité des ventes": ["conversion_rate", "avg_transaction_value"],
                "Indicateurs spécifiques par visiteur": [
                    "revenue_per_visitor",
                    "margin_per_visitor",
                ],
            }
        else:
            themes = {
                "Performance des visiteurs": ["total_visitors"],
                "Performance des transactions": ["total_transactions"],
                "Quantité et revenus": ["total_quantity", "total_revenue"],
                "Marges et coûts": ["total_cost", "total_margin"],
                "Efficacité des ventes": ["conversion_rate", "avg_transaction_value"],
                "Indicateurs spécifiques par visiteur": [
                    "revenue_per_visitor",
                    "margin_per_visitor",
                ],
            }
        streamlit_logger.info(
            f"Themes fetched successfully for period type '{period_type}'."
        )
        return themes
    except Exception as e:
        streamlit_logger.error(f"Error fetching themes for period type '{period_type}': {e}")
        return {}


def display_kpi(title, value, variation=None):
    """
    Affiche un indicateur clé de performance (KPI) avec style.

    Args:
        title (str): Titre du KPI.
        value (str): Valeur du KPI.
        variation (float, optional): Variation en pourcentage par rapport à une période précédente. Par défaut, None.
    """
    try:
        streamlit_logger.info(f"Displaying KPI: {title} with value {value}.")
        if variation is not None:
            color = "green" if variation > 0 else "red"
            arrow = "▲" if variation > 0 else "▼"
            variation_text = (
                f"<p style='margin:0;font-size:16px;color:{color};'>{arrow} {abs(variation):.2f}% / mois précédent.</p>"
            )
        else:
            variation_text = ""
        st.markdown(
            f"""
            <div style='
                background-color:transparent;
                padding:20px;
                border-radius:10px;
                text-align:center;
                width:250px;
                height:150px;
                display:flex;
                flex-direction:column;
                justify-content:center;
                align-items:center;
                box-shadow:0 2px 4px rgba(0,0,0,0.1);
            '>
                <p style='margin:0;font-size:16px;font-weight:bold;color:grey;'>{title}</p>
                <p style='margin:0;font-size:24px;font-weight:bold;color:grey;'>{value}</p>
                {variation_text}
            </div>
            """,
            unsafe_allow_html=True,
        )
        streamlit_logger.info(f"KPI displayed successfully: {title}.")
    except Exception as e:
        streamlit_logger.error(f"Error displaying KPI '{title}': {e}")


def calculate_kpis(data):
    """
    Calcule les indicateurs clés de performance (KPI) pour un ensemble de données.

    Args:
        data (pd.DataFrame): Données filtrées pour calculer les KPI.

    Returns:
        dict: Dictionnaire contenant les valeurs des KPI.
    """
    try:
        streamlit_logger.info("Calculating KPIs.")
        total_visitors = data["total_visitors"].sum()
        total_transactions = data["total_transactions"].sum()
        total_revenue = data["total_revenue"].sum()
        total_cost = data["total_cost"].sum()
        total_margin = total_revenue - total_cost
        conversion_rate = (
            (total_transactions / total_visitors * 100) if total_visitors > 0 else 0
        )
        avg_transaction_value = (
            (total_revenue / total_transactions) if total_transactions > 0 else 0
        )

        kpis = {
            "Total Visiteurs": total_visitors,
            "Total Ventes": total_transactions,
            "Chiffre dAffaires (€)": total_revenue,
            "Coût Total (€)": total_cost,
            "Marge Totale (€)": total_margin,
            "Taux de Conversion (%)": conversion_rate,
            "Panier Moyen (€)": avg_transaction_value,
        }
        streamlit_logger.info(f"KPIs calculated successfully: {kpis}.")
        return kpis
    except Exception as e:
        streamlit_logger.error(f"Error calculating KPIs: {e}")
        return {}


def calculate_variations(current_data, previous_data):
    """
    Calcule les variations en pourcentage pour les KPI entre deux périodes.

    Args:
        current_data (pd.DataFrame): Données actuelles.
        previous_data (pd.DataFrame): Données de la période précédente.

    Returns:
        dict: Dictionnaire contenant les variations en pourcentage pour chaque KPI.
    """
    try:
        streamlit_logger.info("Calculating KPI variations.")

        variations = {}

        # Vérifiez que les données ne sont pas vides pour éviter les erreurs
        if current_data.empty or previous_data.empty:
            streamlit_logger.warning("One or both datasets are empty. No variations calculated.")
            return {
                kpi: None
                for kpi in [
                    "total_visitors",
                    "total_transactions",
                    "total_revenue",
                    "total_cost",
                    "total_margin",
                    "conversion_rate",
                    "avg_transaction_value",
                ]
            }

        # Calcul des métriques spécifiques (agrégation)
        current_visitors = current_data["total_visitors"].sum()
        previous_visitors = previous_data["total_visitors"].sum()
        current_transactions = current_data["total_transactions"].sum()
        previous_transactions = previous_data["total_transactions"].sum()
        current_revenue = current_data["total_revenue"].sum()
        previous_revenue = previous_data["total_revenue"].sum()

        # Calcul du taux de conversion pour chaque période
        current_conversion_rate = (
            (current_transactions / current_visitors * 100) if current_visitors > 0 else 0
        )
        previous_conversion_rate = (
            (previous_transactions / previous_visitors * 100)
            if previous_visitors > 0
            else 0
        )

        # Calcul du panier moyen pour chaque période
        current_avg_transaction_value = (
            (current_revenue / current_transactions) if current_transactions > 0 else 0
        )
        previous_avg_transaction_value = (
            (previous_revenue / previous_transactions) if previous_transactions > 0 else 0
        )

        # Ajoutez les variations des métriques spécifiques
        for kpi, current_value, previous_value in [
            ("total_visitors", current_visitors, previous_visitors),
            ("total_transactions", current_transactions, previous_transactions),
            ("total_revenue", current_revenue, previous_revenue),
            ("conversion_rate", current_conversion_rate, previous_conversion_rate),
            (
                "avg_transaction_value",
                current_avg_transaction_value,
                previous_avg_transaction_value,
            ),
        ]:
            if previous_value != 0:
                variations[kpi] = ((current_value - previous_value) / previous_value) * 100
            else:
                variations[kpi] = None  # Pas de variation si la valeur précédente est 0

        # Calcul des autres variations (si nécessaires)
        variations["total_cost"] = (
            (
                (current_data["total_cost"].sum() - previous_data["total_cost"].sum())
                / previous_data["total_cost"].sum()
                * 100
            )
            if previous_data["total_cost"].sum() > 0
            else None
        )

        variations["total_margin"] = (
            (
                (current_data["total_margin"].sum() - previous_data["total_margin"].sum())
                / previous_data["total_margin"].sum()
                * 100
            )
            if previous_data["total_margin"].sum() > 0
            else None
        )

        streamlit_logger.info(f"KPI variations calculated successfully: {variations}.")
        return variations
    except Exception as e:
        streamlit_logger.error(f"Error calculating KPI variations: {e}")
        return {}


def main():
    """
    Fonction principale pour afficher l'interface utilisateur de visualisation
    des métriques de performance des magasins avec Streamlit.
    """
    try:
        streamlit_logger.info("Application Streamlit lancée.")
        st.title("Visualisation des métriques des magasins")

        tabs = st.tabs(["KPI Généraux", "Graphiques Détaillés"])

        with tabs[1]:
            try:
                st.markdown(
                    "### Sélectionnez un magasin, un type de graphique et une période pour afficher les métriques"
                )

                stores_df = load_data_from_s3(STORES_KEY)
                metrics_df = load_data_from_s3(METRICS_KEY)

                if stores_df.empty or metrics_df.empty:
                    streamlit_logger.warning("Les données sont manquantes ou non disponibles.")
                    st.warning("Données manquantes ou non disponibles.")
                    return

                store_names = stores_df["name"].unique() if "name" in stores_df.columns else []
                store_names = ["Tous les magasins"] + list(
                    store_names
                )  # Ajouter l'option "Tous les magasins"
                selected_store = st.selectbox(
                    "Choisissez un magasin :", store_names, key="store_selectbox"
                )

                if selected_store:
                    streamlit_logger.info(f"Magasin sélectionné : {selected_store}.")
                    if selected_store == "Tous les magasins":
                        store_metrics = metrics_df.copy()  # Tous les magasins
                    else:
                        selected_store_id = stores_df.loc[
                            stores_df["name"] == selected_store, "id"
                        ].values[0]
                        store_metrics = metrics_df[metrics_df["store_id"] == selected_store_id]

                    if store_metrics.empty:
                        streamlit_logger.warning(f"Aucune métrique disponible pour le magasin : {selected_store}.")
                        st.warning("Aucune métrique disponible pour ce magasin.")
                        return

                    store_metrics["date"] = pd.to_datetime(store_metrics["date"])
                    store_metrics["year"] = store_metrics["date"].dt.year
                    store_metrics["month"] = store_metrics["date"].dt.month

                    graph_types = ["Annuel", "Trimestriel", "Mensuel", "Quotidien"]
                    selected_graph_type = st.selectbox(
                        "Choisissez un type de graphique :",
                        graph_types,
                        key="graph_type_selectbox",
                    )
                    streamlit_logger.info(f"Type de graphique sélectionné : {selected_graph_type}.")

                    if selected_graph_type == "Annuel":
                        filtered_metrics = store_metrics.copy()
                    elif selected_graph_type == "Trimestriel":
                        filtered_metrics = store_metrics.copy()
                        # Créer une colonne de tri et une colonne d'affichage
                        filtered_metrics["quarter_for_sort"] = pd.to_datetime(
                            filtered_metrics["date"]
                        ).dt.to_period("Q")
                        filtered_metrics["quarter"] = (
                            filtered_metrics["quarter_for_sort"]
                            .astype(str)
                            .str.replace("Q", "-Q")
                        )

                        # Séparer les colonnes à sommer et à moyenner
                        sum_metrics = [
                            "total_visitors",
                            "total_transactions",
                            "total_quantity",
                            "total_revenue",
                            "total_cost",
                            "total_margin",
                            "avg_revenue_last_4_weeks",
                            "avg_visitors_last_4_weeks",
                            "avg_sales_last_4_weeks",
                        ]
                        mean_metrics = filtered_metrics.select_dtypes(
                            include=["number"]
                        ).columns.difference(sum_metrics)

                        # Appliquer les agrégations appropriées
                        agg_dict = {metric: "sum" for metric in sum_metrics}
                        agg_dict.update({metric: "mean" for metric in mean_metrics})

                        filtered_metrics = (
                            filtered_metrics.groupby(
                                ["quarter", "store_id", "quarter_for_sort"]
                            )
                            .agg(agg_dict)
                            .reset_index()
                        )
                        filtered_metrics = filtered_metrics.sort_values("quarter_for_sort")
                    elif selected_graph_type == "Mensuel":
                        years = store_metrics["year"].unique()
                        selected_year = st.selectbox(
                            "Choisissez une année :", sorted(years), key="year_selectbox"
                        )
                        if selected_year:
                            filtered_metrics = store_metrics[
                                store_metrics["year"] == selected_year
                            ]
                        else:
                            filtered_metrics = pd.DataFrame()
                    else:  # Quotidien
                        years = store_metrics["year"].unique()
                        selected_year = st.selectbox(
                            "Choisissez une année :", sorted(years), key="year_selectbox2"
                        )
                        if selected_year:
                            months = store_metrics[store_metrics["year"] == selected_year][
                                "month"
                            ].unique()
                            month_names = {
                                1: "Janvier",
                                2: "Février",
                                3: "Mars",
                                4: "Avril",
                                5: "Mai",
                                6: "Juin",
                                7: "Juillet",
                                8: "Août",
                                9: "Septembre",
                                10: "Octobre",
                                11: "Novembre",
                                12: "Décembre",
                            }
                            selected_month = st.selectbox(
                                "Choisissez un mois :",
                                [month_names[m] for m in sorted(months)],
                                key="month_selectbox",
                            )
                            if selected_month:
                                selected_month_num = [
                                    k for k, v in month_names.items() if v == selected_month
                                ][0]
                                filtered_metrics = store_metrics[
                                    (store_metrics["year"] == selected_year)
                                    & (store_metrics["month"] == selected_month_num)
                                ]
                            else:
                                filtered_metrics = pd.DataFrame()
                        else:
                            filtered_metrics = pd.DataFrame()

                    if filtered_metrics.empty:
                        streamlit_logger.warning(
                            f"Aucune donnée disponible pour la période : {selected_graph_type}."
                        )
                        st.warning("Aucune donnée disponible pour cette période.")
                        return

                    streamlit_logger.info(
                        f"Filtrage des données terminé pour le type de graphique : {selected_graph_type}."
                    )
                    st.write(
                        f"**Métriques pour le magasin : {selected_store} - {selected_graph_type}**"
                    )

                    themes = get_themes(selected_graph_type)

                    selected_theme = st.selectbox("Choisissez un thème :", list(themes.keys()))
                    if selected_theme:
                        selected_metrics = themes[selected_theme]

                        plot_theme_metrics(
                            filtered_metrics,
                            selected_metrics,
                            f"{selected_theme} - {selected_store} ({selected_graph_type})",
                            selected_graph_type,
                        )

                    csv_data = filtered_metrics.to_csv(index=False).encode("utf-8")
                    file_prefix = (
                        "all_stores"
                        if selected_store == "Tous les magasins"
                        else f"store_{selected_store_id}"
                    )
                    file_suffix = ""

                    if selected_graph_type == "Quotidien":
                        file_suffix = f"_{selected_year}_{selected_month_num}"
                    elif selected_graph_type == "Mensuel":
                        file_suffix = f"_{selected_year}"

                    clicked = st.download_button(
                        label="Télécharger les données en CSV",
                        data=csv_data,
                        file_name=f"metrics_{file_prefix}_{selected_graph_type}{file_suffix}.csv",
                        mime="text/csv",
                    )
                    streamlit_logger.info("Téléchargement des données en CSV proposé à l'utilisateur.")

                    if clicked:
                        streamlit_logger.info("Un utilisateur a télécharger les données en CSV.")

            except Exception as e:
                streamlit_logger.error(f"Erreur lors de l'affichage des graphiques : {e}")
                st.error("Une erreur est survenue lors de l'affichage des graphiques.")

        with tabs[0]:
            try:
                streamlit_logger.info("Affichage des KPI généraux lancé.")
                st.markdown("### KPI Généraux")

                # Obtenir la date actuelle
                today = datetime.today()
                current_day = today.day

                # Convertir la colonne `date` en datetime si ce n'est pas déjà fait
                metrics_df["date"] = pd.to_datetime(metrics_df["date"], errors="coerce")
                metrics_df = metrics_df.dropna(subset=["date"])

                # Ajouter les sélecteurs pour l'année et le mois
                available_years = metrics_df["date"].dt.year.unique()
                selected_year = st.selectbox(
                    "Choisissez une année :",
                    sorted(available_years, reverse=True),
                    key="year_selectbox3",
                )

                # Filtrer par année
                filtered_data = metrics_df[metrics_df["date"].dt.year == selected_year]

                available_months = filtered_data["date"].dt.month.unique()
                month_names = {
                    1: "Janvier",
                    2: "Février",
                    3: "Mars",
                    4: "Avril",
                    5: "Mai",
                    6: "Juin",
                    7: "Juillet",
                    8: "Août",
                    9: "Septembre",
                    10: "Octobre",
                    11: "Novembre",
                    12: "Décembre",
                }
                selected_month = st.selectbox(
                    "Choisissez un mois :",
                    [month_names[m] for m in sorted(available_months)],
                    key="month_selectbox2",
                )

                # Filtrer par mois
                selected_month_num = [k for k, v in month_names.items() if v == selected_month][
                    0
                ]
                filtered_data = filtered_data[
                    filtered_data["date"].dt.month == selected_month_num
                ]

                # Associer les noms et identifiants des magasins
                store_options = {
                    "Tous les magasins": None
                }  # Ajouter l'option pour tous les magasins
                store_options.update(
                    {row["name"]: row["id"] for _, row in stores_df.iterrows()}
                )

                selected_store_name = st.selectbox(
                    "Choisissez un magasin:", list(store_options.keys())
                )

                # Récupérer l'identifiant du magasin sélectionné
                selected_store_id = store_options[selected_store_name]

                # Filtrer les données en fonction du magasin sélectionné
                if selected_store_id is not None:
                    filtered_data = filtered_data[
                        filtered_data["store_id"] == selected_store_id
                    ]

                # Si le mois sélectionné est le mois en cours
                if selected_year == today.year and selected_month_num == today.month:
                    filtered_data = filtered_data[filtered_data["date"].dt.day <= current_day]

                # Calcul des KPI avec les données filtrées
                kpis = calculate_kpis(filtered_data)

                # Filtrer les données du mois précédent pour le magasin sélectionné
                if selected_store_id is not None:
                    previous_month_data = metrics_df[
                        (metrics_df["date"].dt.year == selected_year)
                        & (metrics_df["date"].dt.month == selected_month_num - 1)
                        & (metrics_df["store_id"] == selected_store_id)
                    ]
                else:  # Si "Tous les magasins" est sélectionné
                    previous_month_data = metrics_df[
                        (metrics_df["date"].dt.year == selected_year)
                        & (metrics_df["date"].dt.month == selected_month_num - 1)
                    ]

                # Si le mois est janvier, aller chercher décembre de l'année précédente
                if selected_month_num == 1:
                    if selected_store_id is not None:
                        previous_month_data = metrics_df[
                            (metrics_df["date"].dt.year == selected_year - 1)
                            & (metrics_df["date"].dt.month == 12)
                            & (metrics_df["store_id"] == selected_store_id)
                        ]
                    else:
                        previous_month_data = metrics_df[
                            (metrics_df["date"].dt.year == selected_year - 1)
                            & (metrics_df["date"].dt.month == 12)
                        ]

                if selected_year == today.year and selected_month_num == today.month:
                    # Si le mois est en cours, filtrer jusqu'au jour actuel pour le mois précédent
                    if selected_store_id is not None:
                        previous_month_data = metrics_df[
                            (metrics_df["date"].dt.year == selected_year)
                            & (metrics_df["date"].dt.month == selected_month_num - 1)
                            & (metrics_df["date"].dt.day <= current_day)
                            & (metrics_df["store_id"] == selected_store_id)
                            ]
                    else:
                        previous_month_data = metrics_df[
                            (metrics_df["date"].dt.year == selected_year)
                            & (metrics_df["date"].dt.month == selected_month_num - 1)
                            & (metrics_df["date"].dt.day <= current_day)
                            ]
                    # Si le mois est janvier, aller chercher décembre de l'année précédente
                    if selected_month_num == 1:
                        if selected_store_id is not None:
                            previous_month_data = metrics_df[
                                (metrics_df["date"].dt.year == selected_year - 1)
                                & (metrics_df["date"].dt.month == 12)
                                & (metrics_df["date"].dt.day <= current_day)
                                & (metrics_df["store_id"] == selected_store_id)
                                ]
                        else:
                            previous_month_data = metrics_df[
                                (metrics_df["date"].dt.year == selected_year - 1)
                                & (metrics_df["date"].dt.month == 12)
                                & (metrics_df["date"].dt.day <= current_day)
                            ]

                # Calcul des variations
                variations = calculate_variations(filtered_data, previous_month_data)

                # Affichage des KPI avec variations
                col1, col2, col3 = st.columns(
                    [1, 1, 1]
                )  # Largeurs égales pour toutes les colonnes
                with col1:
                    display_kpi(
                        "Total Visiteurs",
                        f"{kpis['Total Visiteurs']:,.0f}",
                        variations.get("total_visitors"),
                    )
                with col2:
                    display_kpi(
                        "Total Ventes",
                        f"{kpis['Total Ventes']:,.0f}",
                        variations.get("total_transactions"),
                    )
                with col3:
                    display_kpi(
                        "Chiffre d'Affaires (Millions €)",
                        f"{kpis['Chiffre dAffaires (€)'] / 1_000_000:,.3f} M€",
                        variations.get("total_revenue"),
                    )

                col4, col5, col6 = st.columns(
                    [1, 1, 1]
                )  # Largeurs égales pour la deuxième rangée
                with col4:
                    display_kpi(
                        "Coût Total (Millions €)",
                        f"{kpis['Coût Total (€)'] / 1_000_000:,.3f} M€",
                        variations.get("total_cost"),
                    )
                with col5:
                    display_kpi(
                        "Marge Totale (Millions €)",
                        f"{kpis['Marge Totale (€)'] / 1_000_000:,.3f} M€",
                        variations.get("total_margin"),
                    )
                with col6:
                    display_kpi(
                        "Taux de Conversion (%)",
                        f"{kpis['Taux de Conversion (%)']:,.2f} %",
                        variations.get("conversion_rate"),
                    )

                col7, col8, col9 = st.columns(
                    [1, 1, 1]
                )  # Largeurs égales pour la deuxième rangée
                with col7:
                    display_kpi("", "")
                with col8:
                    display_kpi(
                        "Panier Moyen (€)",
                        f"{kpis['Panier Moyen (€)']:,.2f} €",
                        variations.get("avg_transaction_value"),
                    )
                with col9:
                    display_kpi("", "")

                streamlit_logger.info("KPI généraux affichés avec succès.")
            except Exception as e:
                streamlit_logger.error(f"Erreur lors de l'affichage des KPI généraux : {e}")
                st.error("Une erreur est survenue lors de l'affichage des KPI généraux.")
    except Exception as e:
        streamlit_logger.critical(f"Erreur critique dans l'application Streamlit : {e}")
        st.error("Une erreur critique est survenue.")


if __name__ == "__main__":
    main()
