##Vélomag prédiction OpenData Montpellier

Ce projet vise à :

Ingest automatiquement les données des compteurs vélo de Montpellier (open data)

Nettoyer et enrichir les données (jour, heure, weekend, etc.)

Récupérer les données météo via une API externe

Stocker les données dans une base PostgreSQL (locale ou Supabase)

Entraîner un modèle de prédiction de trafic vélo

Déployer une API FastAPI fournissant :

l’exploration des données

l’accès aux données brutes / clean

la prédiction du trafic pour un compteur

le monitoring (erreurs, anomalies)

Héberger un front Streamlit pour la visualisation

🏗️ Architecture du projet
VELOMAG_MONTPELLIER/
│
├── backend/
│   ├── api/                # Routes FastAPI
│   ├── data/               # Scripts d’ingestion (open data, météo)
│   ├── database/           # Classe Database + tables SQLAlchemy
│   ├── modeling/           # Entraînement du modèle ML
│   ├── app/                # Interface Streamlit (dashboard & prédiction)
│   ├── Dockerfile
│   └── requirements.txt
│
├── .env                    # Secrets locaux (non tracké)
├── README.md
└── docker-compose.yml

💾 Base de données

Le projet utilise PostgreSQL (local via Docker ou hébergé via Supabase).

Tables principales :

Table	Description
velo_raw	Données brutes des compteurs vélo
velo_clean	Données nettoyées et enrichies
meteo_raw	Données météo brutes
meteo_clean	Données météo nettoyées et agrégées
model_data	Prédictions stockées pour monitoring

Créées automatiquement grâce à :

from database.schemas import Database
db = Database(DATABASE_URL)
db.create_tables()

🔄 Pipeline d’ingestion

Récupération Open Data Montpellier
→ Stockage dans velo_raw

Nettoyage & enrichissement

conversion datetime

ajout weekday / heure

détection weekend

filtres (valeurs négatives, outliers)
→ stockage dans velo_clean

Ingestion météo
→ API Open-Meteo
→ stockage dans meteo_raw & meteo_clean

Push en base avec :

db.push_data(df, "velo_clean")

🤖 Modélisation

XGBoost

Entrées typiques :

heure

weekday

is_weekend

intensité moyenne historique

météo (temp max, temp min, radiation)

Sortie :

prédiction du trafic pour J+1 ou pour une datetime donnée.

Modèle sauvegardé dans backend/modeling/.

🧪 API FastAPI

Démarrage de l’API :

cd backend
uvicorn api.main:app --reload


Routes typiques :

Route	Description
GET /get_counters	Liste des compteurs
GET /get_historic Données historique
GET /get_prediction
GET /map/data

📊 Dashboard Streamlit

Interface simple permettant :

visualisation des tendances

sélection d’un compteur

affichage des prédictions

diagnostic des anomalies

Lancement :

streamlit run backend/app/dashboard.py

🐳 Docker & Déploiement
Lancer la stack complète :
docker-compose up --build

Services prévus :

backend (FastAPI + ingestion + modèle)

postgres Supabase

streamlit