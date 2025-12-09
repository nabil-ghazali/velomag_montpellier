import pandas as pd
import xgboost as xgb
import joblib
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
from modeling.features import FeatureEngineering

def train_model():
    print(" Démarrage de l'entraînement (Split Temporel)...")

    # --- 1. Chargement & Pipeline ---
    fe = FeatureEngineering()
    df = fe.create_dataset()

    # --- ÉTAPE CRUCIALE POUR LE TEMPOREL ---
    # On trie impérativement par date pour que le split coupe le "passé" du "futur"
    print(" Tri des données par ordre chronologique...")
    df = df.sort_values(by=['ds', 'counter_id'])

    # --- 2. Sélection des Features ---
    features_cols = [
        'counter_id_encoded', 'hour_sin', 'hour_cos', 
        'month_sin', 'month_cos', 'dow_sin', 'dow_cos',
        'is_weekend', 'is_holiday',
        'temperature_2m', 'wind_speed_10m', 'precipitation',
        
        # Les Lags (Mémoire)
        'lag_24h', 'lag_48h', 
        'lag_168h',       # J-7
        'lag_336h',       # J-14 (Nouveau)
        'lag_504h',       # J-21 (Nouveau)
        'mean_last_4_days'
    ]
    
    target_col = 'count'

    # Vérification colonnes
    missing = [c for c in features_cols if c not in df.columns]
    if missing:
        print(f"❌ Erreur : Colonnes manquantes : {missing}")
        return

    X = df[features_cols]
    y = df[target_col]

    # --- 3. Séparation Train / Test (TEMPORELLE) ---
    # shuffle=False : On prend les 80% premiers jours pour Train, 
    # et les 20% derniers jours pour Test.
    print("  Séparation Temporelle (Train sur le passé / Test sur le futur récent)...")
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, 
        test_size=0.2, 
        shuffle=False, # <--- C'EST ICI QUE TOUT CHANGE
        random_state=42
    )

    # Petit affichage pour vérifier les dates de coupure
    last_train_date = df.loc[X_train.index[-1], 'ds']
    first_test_date = df.loc[X_test.index[0], 'ds']
    print(f"   -> Fin de l'entraînement : {last_train_date}")
    print(f"   -> Début du test : {first_test_date}")

    # --- 4. Entraînement ---
    print(" Entraînement du modèle XGBoost...")
    model = xgb.XGBRegressor(
        n_estimators=1000, 
        learning_rate=0.05,
        subsample=0.9,
        colsample_bytree=0.8, 
        max_depth=8, 
        random_state=42, 
        n_jobs=-1
    )
    model.fit(X_train, y_train)

    # --- 5. Évaluation ---
    print(" Prédiction sur les données récentes...")
    predictions = model.predict(X_test)
    predictions = [max(0, x) for x in predictions]

    mae = mean_absolute_error(y_test, predictions)
    r2 = r2_score(y_test, predictions)

    print(f"\n RÉSULTATS (Sur données jamais vues) :")
    print(f"   - MAE : {mae:.2f}")
    print(f"   - R2 Score : {r2:.4f}")

    # --- 6. Sauvegarde Modèle ---
    joblib.dump(model, 'modeling/model_velo.pkl')
    print(" Modèle sauvegardé.")

    # --- 7. Sauvegarde BDD ---
    # N'oubliez pas de vider la table model_data avant si vous voulez éviter les doublons
    # python -m data.cli_data delete-tables-by-name --table-name model_data
    # python -m data.cli_data create-tables
    
    # print(" Envoi des prédictions en base...")
    # df_export = pd.DataFrame({
    #     'datetime': df.loc[X_test.index, 'ds'],
    #     'counter_id': df.loc[X_test.index, 'counter_id'],
    #     'predicted_values': predictions
    # })

    # try:
    #     fe.db.push_data(df_export, "model_data")
    #     print(f" {len(df_export)} prédictions enregistrées (Période : {first_test_date} -> Fin)")
    # except Exception as e:
    #     print(f"❌ Erreur BDD : {e}")

if __name__ == "__main__":
    train_model()