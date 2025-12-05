import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import matplotlib.pyplot as plt
import seaborn as sns # Pour des graphiques plus jolis
import joblib
import os

def train_xgboost_model(data_path="data/processed/train_data_xgboost.csv", test_days=14):
    """
    Entraîne un modèle XGBoost global et génère des graphiques de validation.
    """
    print(" Chargement des données...")
    
    # 1. Chargement
    try:
        df = pd.read_csv(data_path, sep=';')
    except FileNotFoundError:
        print(f" Erreur : Fichier introuvable {data_path}")
        return

    # Préparation
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df.sort_values(by='datetime')
    
    # On définit les colonnes
    features_to_drop = ['datetime', 'counter_id', 'intensity']
    features = [col for col in df.columns if col not in features_to_drop]
    target = 'intensity'
    
    # 2. SPLIT TEMPOREL
    cutoff_date = df['datetime'].max() - pd.Timedelta(days=test_days)
    print(f"  Split Train/Test à la date : {cutoff_date}")
    
    # On garde les masques pour récupérer les dates plus tard
    train_mask = df['datetime'] < cutoff_date
    test_mask = df['datetime'] >= cutoff_date
    
    X_train = df.loc[train_mask, features]
    y_train = df.loc[train_mask, target]
    
    X_test = df.loc[test_mask, features]
    y_test = df.loc[test_mask, target]

    # 3. ENTRAÎNEMENT
    print("  Entraînement XGBoost...")
    model = xgb.XGBRegressor(
        n_estimators=1000,
        learning_rate=0.05,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        n_jobs=-1,
        random_state=42,
        early_stopping_rounds=50
    )

    model.fit(
        X_train, y_train,
        eval_set=[(X_train, y_train), (X_test, y_test)],
        verbose=False # On cache le log pour clarté
    )

    # 4. PRÉDICTIONS
    print(" Prédictions sur le jeu de test...")
    predictions = model.predict(X_test)
    predictions = np.clip(predictions, 0, None) # Pas de négatifs
    
    # 5. SCORES
    mae = mean_absolute_error(y_test, predictions)
    rmse = np.sqrt(mean_squared_error(y_test, predictions))
    r2 = r2_score(y_test, predictions)
    
    print(f"\n RÉSULTATS ({test_days} jours) :")
    print(f"   - R²   : {r2:.4f}")
    print(f"   - MAE  : {mae:.2f}")
    print(f"   - RMSE : {rmse:.2f}")

    # 6. SAUVEGARDE
    os.makedirs("model/saved", exist_ok=True)
    joblib.dump(model, "model/saved/xgboost_velo.pkl")
    

    # =========================================================
    # 7. VISUALISATION AVANCÉE (POUR TOUS LES COMPTEURS)
    # =========================================================
    print("\n Génération des graphiques pour CHAQUE compteur...")
    os.makedirs("output/plots_xgboost", exist_ok=True)
    
    # On reconstruit un DataFrame propre pour l'affichage
    df_viz = df.loc[test_mask, ['datetime', 'counter_id', 'intensity']].copy()
    df_viz['predicted'] = predictions
    
    # On récupère TOUS les IDs uniques
    unique_counters = df_viz['counter_id'].unique()
    
    sns.set_theme(style="whitegrid")
    
    # On boucle sur tout le monde
    for i, counter in enumerate(unique_counters):
        # Petit print pour suivre l'avancement (utile si tu en as 50)
        print(f"   [{i+1}/{len(unique_counters)}] Génération graphe : {counter}...")
        
        subset = df_viz[df_viz['counter_id'] == counter]
        
        # Calcul du R² spécifique à ce compteur (pour voir les "mauvais élèves")
        if len(subset) > 0:
            r2_local = r2_score(subset['intensity'], subset['predicted'])
        else:
            r2_local = 0
        
        plt.figure(figsize=(15, 6))
        
        # Réalité
        plt.plot(subset['datetime'], subset['intensity'], 
                 label='Réel', color='black', alpha=0.6, linewidth=1.5)
        
        # Prédiction
        plt.plot(subset['datetime'], subset['predicted'], 
                 label='Prédit (XGBoost)', color='#0072B2', linewidth=2.5)
        
        # Zone d'erreur
        plt.fill_between(subset['datetime'], subset['intensity'], subset['predicted'], 
                         color='gray', alpha=0.1)
        
        plt.title(f"Compteur : {counter}\nR² local : {r2_local:.2f}", fontsize=14)
        plt.xlabel("Date")
        plt.ylabel("Nombre de vélos")
        plt.legend()
        
        # Astuce : On nettoie le nom du fichier (remplace les ':' par '_') pour Windows/Linux
        safe_name = counter.replace(':', '_').replace('/', '_')
        filename = f"output/plots_xgboost/plot_{safe_name}.png"
        
        plt.savefig(filename)
        plt.close() # Important : ferme la figure pour libérer la mémoire RAM

    print(f" Terminé ! Tous les graphiques sont dans 'model/xgboost/saved/plots_xgboost/'")

if __name__ == "__main__":
    train_xgboost_model()