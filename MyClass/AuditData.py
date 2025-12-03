import pandas as pd
import numpy as np

class AuditData ():    
    
    """
    Une 'classe statique' : regroupement de fonctions utiles.
    """
    @staticmethod
    def audit_dataframe(df):
        """
        Réalise un audit complet de la qualité des données :
        - Valeurs manquantes
        - Doublons
        - Détection d'outliers (méthode IQR)
        """
        print(f" --- AUDIT DU DATAFRAME ({df.shape[0]} lignes, {df.shape[1]} colonnes) ---")
        
        # 1. Vérification des types
        print("\n Types des données :")
        print(df.dtypes)

        # 2. Valeurs Manquantes
        print("\n Valeurs Manquantes (NaN) :")
        missing = df.isnull().sum()
        missing = missing[missing > 0] # On n'affiche que ceux qui ont des problèmes
        if missing.empty:
            print(" Aucune valeur manquante détectée.")
        else:
            print(missing)
            # Pourcentage de vide
            # On fait la moyenne des colonnes, PUIS la moyenne du résultat global
            print(f"   -> Globalement : {df.isnull().mean().mean():.2%} du tableau est vide.")

        # 3. Doublons
        duplicates = df.duplicated().sum()
        print(f"\n Lignes Dupliquées : {duplicates}")

        # 4. Détection des Outliers (Méthode IQR)
        # On ne travaille que sur les colonnes numériques
        # select_dtypes agit comme un tamis. On lui dit : "Garde uniquement les colonnes qui contiennent des nombres (np.number c'est-à-dire int ou float)."
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        print("\n Détection d'Outliers Potentiels (Méthode IQR) :")
        for col in numeric_cols:
            # Calcul des Quartiles
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            
            # Bornes (Tout ce qui est en dehors est considéré comme outlier)
            # Pourquoi 1.5 ? C'est une convention statistique (règle de Tukey). On prend la largeur du "ventre mou" (IQR) et on l'étend d'une fois et demie vers le haut et vers le bas.
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            # Compter les outliers
            # Compte toutes les lignes qui sont SOIT trop petites, SOIT trop grandes.
            outliers_count = ((df[col] < lower_bound) | (df[col] > upper_bound)).sum()
            print(f"Outlier de la colonne {df[col]}, min: {(df[col] < lower_bound)}, max: {(df[col] > upper_bound)}  ")
            if outliers_count > 0:
                print(f"    Colonne '{col}' : {outliers_count} outliers détectés")
                print(f"    (Bornes acceptables : {lower_bound:.2f} à {upper_bound:.2f})")
            else:
                print(f"    Colonne '{col}' : Clean")



