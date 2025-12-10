import streamlit as st
import pandas as pd
import requests
import folium
from streamlit_folium import st_folium
import plotly.express as px
import plotly.graph_objects as go
from datetime import date
import os


# --- 1. CONFIGURATION ---
st.set_page_config(page_title="VéloMag Montpellier", page_icon="🚲", layout="wide")
API_URL = os.getenv("API_URL", "http://localhost:8000")

st.title("🚲 Tableau de Bord - VéloMag")

# --- 2. FONCTIONS DE CHARGEMENT (CACHÉES) ---

@st.cache_data(ttl=3600)
def get_map_data():
    """Récupère les données globales pour la carte (Route /map-data)"""
    try:
        response = requests.get(f"{API_URL}/map-data")
        response.raise_for_status()
        df = pd.DataFrame(response.json())
        if not df.empty:
            df['datetime_obj'] = pd.to_datetime(df['date'])
            df['day_date'] = df['datetime_obj'].dt.date
            df['hour'] = df['datetime_obj'].dt.hour
            df = df.dropna(subset=['lat', 'lon'])
        return df
    except Exception as e:
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_counters_list():
    """Récupère la liste des compteurs disponibles"""
    try:
        response = requests.get(f"{API_URL}/counters")
        if response.status_code == 200:
            return response.json()['counters']
        return []
    except:
        return []

def get_detail_data(counter_id):
    """Récupère l'historique et la prédiction pour un compteur spécifique"""
    # Pas de cache ici pour permettre le changement rapide de compteur
    try:
        # Historique
        res_hist = requests.get(f"{API_URL}/history/{counter_id}")
        df_hist = pd.DataFrame(res_hist.json()) if res_hist.status_code == 200 else pd.DataFrame()
        
        # Prédiction
        res_pred = requests.get(f"{API_URL}/prediction/{counter_id}")
        df_pred = pd.DataFrame(res_pred.json()) if res_pred.status_code == 200 else pd.DataFrame()
        
        # Conversion dates
        if not df_hist.empty: df_hist['datetime'] = pd.to_datetime(df_hist['datetime'])
        if not df_pred.empty: df_pred['datetime'] = pd.to_datetime(df_pred['datetime'])
        
        return df_hist, df_pred
    except:
        return pd.DataFrame(), pd.DataFrame()

def get_color(daily_intensity):
    if daily_intensity < 500: return "green"
    elif daily_intensity < 1500: return "orange"
    else: return "red"

# --- 3. INTERFACE UTILISATEUR (ONGLETS) ---

tab1, tab2 = st.tabs([" Météo du Trafic (Carte)", " Analyse Détaillée (Courbes)"])

# ==========================================
# ONGLET 1 : LA CARTE (VUE GLOBALE)
# ==========================================
with tab1:
    st.header("Prévisions de trafic sur la ville")
    
    df_map = get_map_data()
    
    if not df_map.empty:
        # --- Filtres ---
        col_filter, _ = st.columns([1, 3])
        with col_filter:
            unique_dates = sorted(df_map['day_date'].unique())
            selected_date = st.selectbox(
                " Choisir la date à visualiser", 
                unique_dates, 
                format_func=lambda d: d.strftime('%A %d %B %Y')
            )
        
        # Filtrage
        df_day = df_map[df_map['day_date'] == selected_date]
        
        # --- KPI ---
        total_trafic = df_day['predicted_intensity'].sum()
        temp_moy = df_day['temperature_2m'].mean()
        
        kpi1, kpi2, kpi3 = st.columns(3)
        kpi1.metric("Date", selected_date.strftime('%d/%m/%Y'))
        kpi2.metric("Trafic Total Prévu", f"{int(total_trafic):,} vélos".replace(",", " "))
        kpi3.metric("Météo Moyenne", f"{temp_moy:.0f}°C")
        
        # --- Carte Folium ---
        row1_col1, row1_col2 = st.columns([2, 1])
        
        with row1_col1:
            st.subheader(" Carte des volumes")
            # Agrégation par compteur pour la journée
            df_agg = df_day.groupby('counter_id').agg({
                'predicted_intensity': 'sum', 'lat': 'first', 'lon': 'first'
            }).reset_index()
            
            m = folium.Map(location=[43.6107, 3.8767], zoom_start=13, tiles="CartoDB positron")
            
            for _, row in df_agg.iterrows():
                vol = int(row['predicted_intensity'])
                folium.CircleMarker(
                    location=[row['lat'], row['lon']],
                    radius=5 + (vol / 500),
                    color=get_color(vol),
                    fill=True, fill_color=get_color(vol), fill_opacity=0.7,
                    tooltip=f"{row['counter_id']}: {vol} vélos"
                ).add_to(m)
            
            st_folium(m, width=None, height=500)
            
        with row1_col2:
            st.subheader(" Profil Horaire Ville")
            df_chart = df_day.groupby('hour')['predicted_intensity'].sum().reset_index()
            fig_area = px.area(
                df_chart, x='hour', y='predicted_intensity',
                labels={'predicted_intensity': 'Vélos', 'hour': 'Heure'},
                color_discrete_sequence=['#0072B2']
            )
            fig_area.update_layout(height=450)
            st.plotly_chart(fig_area, use_container_width=True)

    else:
        st.warning(" Aucune donnée de carte disponible. Vérifiez le script 'predict.py'.")

# ==========================================
# ONGLET 2 : LE DÉTAIL (VUE COMPTEUR)
# ==========================================
with tab2:
    st.header("Analyse détaillée par compteur")
    
    counters_list = get_counters_list()
    
    if counters_list:
        # Sélecteur
        selected_counter = st.selectbox("📍 Sélectionnez un compteur spécifique :", counters_list)
        
        # Chargement des détails
        df_real, df_pred = get_detail_data(selected_counter)
        
        if not df_pred.empty:
            # Calculs pour le zoom (7 derniers jours réels + prédictions)
            last_real = df_real['datetime'].max() if not df_real.empty else df_pred['datetime'].min()
            start_view = last_real - pd.Timedelta(days=7)
            
            # Filtrage pour l'affichage
            df_real_zoom = df_real[df_real['datetime'] > start_view] if not df_real.empty else pd.DataFrame()
            
            # --- Graphique Comparatif ---
            st.subheader(f"Historique récent et Prévisions : {selected_counter}")
            
            fig = go.Figure()
            
            # Trace Réelle
            if not df_real_zoom.empty:
                fig.add_trace(go.Scatter(
                    x=df_real_zoom['datetime'], y=df_real_zoom['count'],
                    mode='lines', name='Historique Réel',
                    line=dict(color='blue', width=2)
                ))
            
            # Trace Prédiction
            fig.add_trace(go.Scatter(
                x=df_pred['datetime'], y=df_pred['count'],
                mode='lines', name='Prédiction (J+1)',
                line=dict(color='red', width=3, dash='dot')
            ))
            
            fig.update_layout(
                xaxis_title="Date", yaxis_title="Passages Vélo",
                hovermode="x unified",
                legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01)
            )
            
            st.plotly_chart(fig, use_container_width=True)
                        
            # Affichage des chiffres bruts de prédiction
            with st.expander("Voir les chiffres de la prédiction"):
                # On trie et on supprime les doublons éventuels sur la date
                df_clean_view = df_pred.drop_duplicates(subset=['datetime']).sort_values('datetime')
                
                # On formatte la date pour qu'elle soit jolie en nom de colonne
                df_clean_view['str_date'] = df_clean_view['datetime'].dt.strftime('%d/%m %Hh')
                
                # On transpose proprement
                st.dataframe(df_clean_view.set_index('str_date')[['count']].T)
                            
        else:
            st.info("Pas de prévisions disponibles pour ce compteur spécifique.")
            
    else:
        st.error("Impossible de récupérer la liste des compteurs.")