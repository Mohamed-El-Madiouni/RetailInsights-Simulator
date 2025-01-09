# Retail Data Simulation

Ce projet consiste à créer et analyser des données synthétiques de vente et de visites pour un environnement de retail. Il permet de simuler des visites de clients, des ventes de produits, et de suivre les performances des magasins. 

Le projet inclut des étapes automatisées d'extraction, transformation, et visualisation des données, et met en avant des technologies modernes pour le traitement de données à grande échelle.

## Table des matières
- [Contexte et objectif](#contexte-et-objectif)
- [Architecture du projet](#architecture-du-projet)
- [Pipeline ETL](#pipeline-etl)
- [Technologies utilisées](#technologies-utilisées)
- [Résultats et visualisation](#résultats-et-visualisation)
- [Points forts](#points-forts)
- [Axes d'amélioration](#axes-damélioration)
- [Installation et exécution](#installation-et-exécution)

## Contexte et objectif

L'objectif principal du projet est de simuler un environnement retail pour :
- Analyser les performances des magasins.
- Générer des indicateurs clés de performance (KPI) tels que le total des ventes, la marge brute, ou le nombre moyen de visiteurs.

Ce projet a été conçu dans une optique professionnelle pour démontrer des compétences en ingénierie des données, notamment l'automatisation, le traitement de données dans le cloud, et la visualisation des résultats.

## Architecture du projet

L'architecture du projet suit une structure modulaire et bien organisée :

```
RetailInsights-Simulator/
|----data_api/                    # Contient les données générées localement
|----src/
|--------airflow/                 # DAGs pour orchestrer le pipeline
|--------API/                     # Scripts de génération des données
|--------data_processing/         # Extraction et transformation des données
|--------streamlit_app/           # Visualisation des KPI via Streamlit
|--------tests/                   # Tests unitaires pour valider le pipeline
|----README.md                    # Documentation
|----requirements.txt             # Dépendances Python
```

## Pipeline ETL

Le pipeline ETL est entièrement automatisé avec Apache Airflow. Il inclut :

1. **Génération des données** : Simulation de données synthétiques pour les clients, produits, ventes, et magasins, stockés sous forme de fichiers au format JSON.
2. **Extraction des données** : Lecture des données de l'API basée sur les données JSON générées, puis extraction de ces données dans des fichiers au format parquet sur S3.
3. **Transformation et agrégation** : Calcul des métriques comme les ventes moyennes ou les marges par magasin.
4. **Stockage cloud** : Les données sont converties au format Parquet et sauvegardées dans Amazon S3 pour un accès rapide et optimisé.
5. **Visualisation** : Les données agrégées sont affichées via une application interactive Streamlit.

## Technologies utilisées

- **Apache Airflow** : Orchestration et automatisation des workflows.
- **Amazon S3** : Stockage des données au format Parquet.
- **Amazon EC2** : Hébergement des workflows Airflow avec une gestion optimisée des coûts.
- **Streamlit et Plotly** : Visualisation interactive des résultats.
- **Python** : Langage principal pour la génération et transformation des données.
- **Pandas et PyArrow** : Manipulation et stockage efficace des données.

## Résultats et visualisation

L'application Streamlit affiche :
- Des graphiques interactifs montrant les tendances des ventes par magasin.
- Des filtres par période (année, mois) pour explorer les données.
- Les KPI clés tels que :
  - Total des ventes.
  - Performance des magasins.
  - Nombre moyen de visiteurs.

L'application consomme directement les données stockées sur S3, ce qui garantit une mise à jour en temps réel après l'exécution du pipeline.

## Points forts

- **Automatisation** :
  - Chaque étape du pipeline (de la génération des données à la visualisation) est entièrement automatisée.
  - L'utilisation de règles EventBridge pour démarrer et arrêter les instances EC2 réduit les coûts de plus de 90 %.
- **Stockage cloud optimisé** :
  - Les données sont stockées en Parquet, ce qui réduit la taille des fichiers et améliore les performances.
- **Transformations avancées** :
  - Gestion des valeurs nulles et aberrantes.
  - Calcul de métriques agrégées comme la marge brute.

## Axes d'amélioration

1. **Gestion des erreurs** :
   - Ajouter des mécanismes de retries ou des notifications en cas d'échec des tâches dans Airflow.
   - Mettre en place un système de validation des données avant le stockage sur S3.

2. **Évolutivité** :
   - Pour gérer une augmentation du volume de données, migrer vers un cluster Airflow ou un service managé comme MWAA (Managed Workflows for Apache Airflow).
   - Intégrer des bases de données analytiques comme Amazon Redshift pour des requêtes à grande échelle.

## Installation et exécution

1. Clonez ce dépôt :
```
git clone <url-du-depot>
```

2. Installez les dépendances :
```
pip install -r requirements.txt
```

3. Configurez vos clés AWS dans un fichier `.env` :
```
AWS_ACCESS_KEY_ID=<votre-access-key>
AWS_SECRET_ACCESS_KEY=<votre-secret-key>
AWS_REGION=<votre-region>
```

4. Lancez le serveur Airflow :
```
airflow webserver
```

5. Exécutez le pipeline via l'interface Airflow.
