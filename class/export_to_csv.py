import pandas as pd

def exporter_donnees(df, nom_fichier="meteo_clean.csv"):
    print(f"💾 Sauvegarde en cours vers '{nom_fichier}'...")
    
    # --- Option 1 : La version Standard (US/International) ---
    # Parfait si tu réutilises ce fichier en Python plus tard
    # df.to_csv(nom_fichier) 
    
    # --- Option 2 : La version "Excel Français" (Recommandée) ---
    # Pour être sûr que les accents passent et que les colonnes soient bien séparées
    df.to_csv(
        nom_fichier,
        sep=';',             # On utilise le point-virgule (standard Excel FR)
        decimal=',',         # On utilise la virgule pour les décimales (ex: 20,5 au lieu de 20.5)
        encoding='utf-8-sig',# 'utf-8-sig' force Excel à bien lire les accents (é, à, è)
        index=True           # CRUCIAL : On garde l'index (tes dates !)
    )
    
    print("Export réussi !")

