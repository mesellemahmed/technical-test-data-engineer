# Réponses du test

## _Utilisation de la solution (étape 1 à 3)_

hadoop-2.8.1
spark-3.5.2-bin-hadoop3
jdk11
python 3.11.2

## Questions (étapes 4 à 7)

### Étape 4

Pour stocker les donnees récupérées des trois sources de données (API d'un système similaire à Spotify), j'ai choisi une architecture médaillon avec trois couches (Bronze, Silver et Gold). Un schéma de base de données structuré et performant est essentiel pour faciliter les opérations de traitement des données et permettre des analyses avancées en IA pour la recommandation musicale.
## 1. Schéma de la base de données

### 1.1. Bronze Layer (Raw Data)

Les données brutes de l'API seront d'abord stockées dans cette couche. Les fichiers seront stockés au format **Parquet**, ce qui permet une grande efficacité de stockage et une lecture rapide, notamment pour les systèmes Big Data.

- **Tables de données API** : Ces tables contiendront toutes les informations récupérées directement depuis l'API. Les données peuvent inclure les métadonnées des chansons, les utilisateurs, les interactions (écoutes, likes, partages).
  - `tracks_raw`: Informations sur les morceaux récupérés de l'API (ID, titre, artiste, album, genre, durée, etc.).
  - `users_raw`: Informations sur les utilisateurs (ID utilisateur, nom, région, préférences).
  - `listen_history_raw`: Données sur les interactions utilisateur (ID utilisateur, ID morceau, timestamp).

### 1.2. Silver Layer (Transformed Data)

La Silver Layer contient des données nettoyées et pré-transformées, prêtes pour l'analyse et les calculs algorithmiques. Les informations de l’API sont intégrées, dédupliquées, autres dimensions ont été dérivé et les relations entre les différentes entités sont établies.

- **Tables** :
  - `users`: Table nettoyée contenant les informations utilisateurs valides.
  - `genre`: Table nettoyée contenant les informations des genres valides.
  - `album`: Table nettoyée contenant les informations des albums valides.
  - `artist`: Table nettoyée contenant les informations des artists valides.
  - `listen_history`: Table des morceaux nettoyés avec une correspondance entre les différentes sources.

### 1.3. Gold Layer (Optimized for AI)

Cette couche contient des tables optimisées pour les tâches d’analyse avancée, notamment les recommandations musicales par IA. Les données sont prêtes à être utilisées dans les modèles de machine learning.
Cette couche contient toutes les dimensions déclarés précédemment et la table de fait (Listen_history), avec l'ajout d'une dimension Date riche en terme d'axe d'analyse prete a exploiter. Un modele Snowflake simple a été implémenté.

## 2. Recommandation de système de base de données

Pour ce type de système, il est recommandé d'utiliser un **data lake** basé sur **Apache Spark** avec des fichiers **Parquet**, qui peuvent être organisés dans des dossiers pour chaque couche (bronze, silver, gold). Spark offre une intégration native avec des formats de fichiers colonnes comme Parquet, ce qui permet une gestion optimisée des grands volumes de données, nécessaires dans un système Big Data pour des cas d’utilisation tels que la recommandation musicale pour notre cas.

### 2.1. Avantages d’un data lake avec Spark :

1. **Scalabilité** : Spark est capable de gérer de grandes quantités de données réparties sur plusieurs clusters, ce qui est crucial pour un système de recommandation musicale basé sur des volumes de données massifs.
2. **Intégration facile avec des modèles de machine learning** : Spark MLlib permet d’entraîner des modèles IA directement sur des données volumineuses, facilitant l'itération rapide pour améliorer les algorithmes de recommandation.
3. **Gestion native des formats de fichiers optimisés** : Parquet offre une efficacité de compression et un accès rapide aux données, en particulier lorsque des transformations de type batch sont nécessaires pour enrichir les données entre les couches bronze, silver et gold.
4. **Simplicité des pipelines de données** : Spark permet d’orchestrer facilement des pipelines de traitement de données, depuis l'ingestion brute (bronze) jusqu'à la transformation et l’optimisation des données (silver et gold), tout en assurant des performances élevées et une faible latence.

### En conclusion
Cette architecture en médaillon combinée à un data lake basé sur Apache Spark et des fichiers Parquet permet de gérer efficacement un flux de données varié et volumineux. Elle permet également d’effectuer des analyses IA avancées, comme les systèmes de recommandation musicale, tout en garantissant une flexibilité pour évoluer avec les besoins croissants en termes de traitement de données.

### Étape 5

Le suivi et le monitoring de la santé du pipeline sont cruciaux pour garantir que les données sont bien ingérées, transformées et optimisées dans les différentes couches, sans échec ni interruption. En général, un suivi continu et en temps réel est recommandé, selon les méthodes et outils suivants :
## 1. Utilisation d'une plateforme de monitoring
L'utilisation d'un outil de planification, d'orchestration, de monitoring et de visualisation du workflow comme Apache Airflow fournit des rapports détaillés sur l'exécution du pipeline, avec plusieurs métriques (réussite, échec, durée, etc.).
## 2. système de logs
Enrichir le pipeline avec des logs à chaque couche et à chaque étape, en particulier pour les exceptions et les erreurs. Ces logs sont ensuite stockés sur une plateforme de logging afin d’être analysés ultérieurement. En général, Elasticsearch est utilisé pour centraliser et interroger ces données.
## 3. système de notification
Configurer des alertes et des notifications par e-mail ou via Teams pour partager l'état d'exécution du pipeline avec les métriques pertinentes. Cela permet de tenir les équipes informées en temps réel des succès, des échecs, et des anomalies potentielles dans le pipeline.
## 4. Tableau de bord de suivi
Prometheus et Grafana sont les outils les plus utilisés pour extraire les métriques des applications et les visualiser dans des tableaux de bord dédiés. Prometheus collecte les données en temps réel, tandis que Grafana permet de créer des visualisations interactives et personnalisées pour un suivi efficace des performances et de l'état du système.

## Métriques clés:
En general, dans des pipelines d'ingestion, on parle des metriques suivantes: latence d'ingestion des données, taux de réussite/échec, délai des transformations, volumes de données, consommation de ressources (RAM, CPU).

### Étape 6
Selon les besoins et les choix des experts en intelligence artificielle, une couche sera ajoutée au pipeline, constituant une couche d'application dédiée à cet objectif. Cette couche consomme les données de la couche Gold, entraîne le modèle avec les nouvelles données et génère des recommandations qui seront ensuite stockées dans une base de données de recommandations. Cela facilitera l'utilisation de ces recommandations par l'application frontend.

### Étape 7
Le réentraînement du modèle s'effectue automatiquement dans la couche d'application. Dès que de nouvelles données sont disponibles dans la couche Gold, le modèle est entraîné, puis les recommandations sont calculées.
