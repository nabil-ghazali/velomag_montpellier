import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import matplotlib.pyplot as plt
import mlflow
import mlflow.xgboost
import os
import shutil
import warnings
import logging  # <--- Le module indispensable

# 1. CONFIGURATION DU LOGGER
# On crée un dossier pour stocker les logs
os.makedirs("logs", exist_ok=True)

# Configuration globale
logging.basicConfig(
    level=logging.INFO, # On capture tout ce qui est INFO et plus grave
    format='%(asctime)s - %(levelname)s - %(message)s', # Format : 2024-12-05 10:00:00 - INFO - Message
    handlers=[
        logging.FileHandler("logs/training.log"), # Sauvegarde dans un fichier
        logging.StreamHandler()                   # Affiche aussi dans la console
    ]
)

logger = logging.getLogger(__name__)

# On ignore les warnings
warnings.filterwarnings("ignore")

def train_xgboost_model(data_path="data/processed/train_data_xgboost.csv", test_days=14):
    
    # --- MLFLOW LOCAL ---
    mlflow.set_experiment("Velo_Montpellier_Local")
    
    logger.info("🚀 Démarrage du pipeline d'entraînement (Mode Local)...")

    # 2. CHARGEMENT
    try:
        if not os.path.exists(data_path):
            logger.error(f"❌ Fichier introuvable : {data_path}")
            return

        df = pd.read_csv(data_path, sep=';')
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df.sort_values(by='datetime')
        logger.info(f"✅ Données chargées avec succès : {len(df)} lignes.")
        
    except Exception as e:
        # exc_info=True permet d'afficher toute l'erreur technique dans le log
        logger.error(f"❌ Erreur critique lors du chargement des données : {e}", exc_info=True)
        return

    # Préparation
    features_to_drop = ['datetime', 'counter_id', 'intensity']
    features = [col for col in df.columns if col not in features_to_drop]
    target = 'intensity'

    logger.info(f"📋 Features utilisées ({len(features)}) : {features}")

    # Split Temporel
    cutoff_date = df['datetime'].max() - pd.Timedelta(days=test_days)
    mask_train = df['datetime'] < cutoff_date
    mask_test = df['datetime'] >= cutoff_date
    
    X_train = df.loc[mask_train, features]
    y_train = df.loc[mask_train, target]
    X_test = df.loc[mask_test, features]
    y_test = df.loc[mask_test, target]

    logger.info(f"✂️  Split Train/Test effectué à la date : {cutoff_date}")
    logger.info(f"   Train set : {X_train.shape} | Test set : {X_test.shape}")

    # --- DÉBUT DU RUN MLFLOW ---
    with mlflow.start_run() as run:
        
        # A. Paramètres
        params = {
            "n_estimators": 1000,
            "learning_rate": 0.05,
            "max_depth": 6,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "early_stopping_rounds": 50,
            "n_jobs": -1,
            "random_state": 42
        }
        mlflow.log_params(params)
        logger.info("⚙️  Hyperparamètres loggués dans MLflow.")
        
        # B. Entraînement
        logger.info("🏋️  Entraînement du modèle XGBoost en cours...")
        model = xgb.XGBRegressor(**params)
        model.fit(
            X_train, y_train,
            eval_set=[(X_train, y_train), (X_test, y_test)],
            verbose=False
        )

        # C. Scores
        preds = model.predict(X_test)
        preds = np.clip(preds, 0, None)
        
        mae = mean_absolute_error(y_test, preds)
        rmse = np.sqrt(mean_squared_error(y_test, preds))
        r2 = r2_score(y_test, preds)
        
        logger.info(f"🏆 RÉSULTATS FINAUX : R²={r2:.4f} | MAE={mae:.2f} | RMSE={rmse:.2f}")
        
        if r2 < 0.70:
            logger.warning("⚠️  Attention : Le score R² est inférieur à 0.70. Le modèle pourrait être peu performant.")

        mlflow.log_metrics({"mae": mae, "rmse": rmse, "r2": r2})

        # D. Sauvegarde Modèle
        logger.info("💾 Sauvegarde du modèle dans MLflow...")
        mlflow.xgboost.log_model(model, artifact_path="model")

        # E. Graphiques
        logger.info("📊 Génération des graphiques de validation...")
        temp_dir = "temp_mlflow_plots"
        os.makedirs(temp_dir, exist_ok=True)
        
        try:
            # Graphe 1
            plt.figure(figsize=(12, 8))
            xgb.plot_importance(model, max_num_features=20, height=0.5)
            plt.tight_layout()
            plt.savefig(f"{temp_dir}/feature_importance.png")
            plt.close()
            
            # Graphe 2
            if not mask_test.any():
                logger.warning("⚠️  Pas de données de test pour générer les graphiques !")
            else:
                sample_id = df.loc[mask_test, 'counter_id'].unique()[0]
                subset = df[(df['counter_id'] == sample_id) & (mask_test)]
                
                # Petit trick pour récupérer les preds alignées
                subset_preds = model.predict(subset[features])
                
                plt.figure(figsize=(15, 6))
                plt.plot(subset['datetime'], subset['intensity'], label='Réel', color='black', alpha=0.6)
                plt.plot(subset['datetime'], subset_preds, label='Prédit', color='#0072B2')
                plt.legend()
                plt.savefig(f"{temp_dir}/exemple_prediction.png")
                plt.close()
            
            mlflow.log_artifacts(temp_dir)
            logger.info("✅ Graphiques envoyés vers MLflow.")

        except Exception as e:
            logger.error(f"❌ Erreur lors de la génération des graphiques : {e}", exc_info=True)
        finally:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
        
        logger.info(f"🏁 Fin du Run MLflow : {run.info.run_id}")
        logger.info("💡 Pour voir les résultats : tape 'mlflow ui' dans ton terminal.")

if __name__ == "__main__":
    train_xgboost_model()